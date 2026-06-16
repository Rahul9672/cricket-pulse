from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DateType
)
import sys, os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'ingestion'))
from cricsheet_parser import load_match, extract_deliveries, get_all_match_files

# ─────────────────────────────────────────
# STEP 1 — SparkSession banao
# (Pandas mein yeh step nahi tha)
# ─────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CricketPulse-BatchAnalysis")   # Spark UI pe yeh naam dikhega
        .master("local[*]")                       # local = tera laptop, [*] = sab cores use karo
        .config("spark.sql.adaptive.enabled", "true")  # AQE — auto optimization
        .config("spark.sql.shuffle.partitions", "8")   # default 200 → 8 (chhota data)
        .getOrCreate()
    )

# ─────────────────────────────────────────
# STEP 2 — Data load karo
# Pandas: pd.DataFrame(list)
# Spark:  spark.createDataFrame(list, schema)
# ─────────────────────────────────────────

def load_season_data_spark(spark, data_dir: str, max_matches: int = 20):
    """
    Sab matches load karo → Spark DataFrame banao
    """
    # Schema define karo — Spark ko pata hona chahiye types
    schema = StructType([
        StructField("match_id",     StringType(),  True),
        StructField("date",         StringType(),  True),
        StructField("venue",        StringType(),  True),
        StructField("batting_team", StringType(),  True),
        StructField("batter",       StringType(),  True),
        StructField("bowler",       StringType(),  True),
        StructField("over",         IntegerType(), True),
        StructField("ball",         IntegerType(), True),
        StructField("runs_off_bat", IntegerType(), True),
        StructField("total_runs",   IntegerType(), True),
        StructField("extras",       IntegerType(), True),
        StructField("wicket",       StringType(),  True),  # null = no wicket
    ])

    all_deliveries = []
    files = get_all_match_files(data_dir)[:max_matches]

    print(f"Loading {len(files)} matches into Spark...")

    for filepath in files:
        try:
            match = load_match(str(filepath))
            meta  = match.get('info', {})
            date  = meta.get('dates', ['unknown'])[0]
            venue = meta.get('venue', 'unknown')

            for delivery in extract_deliveries(match):
                delivery['date']  = date
                delivery['venue'] = venue
                # teams list → string convert
                delivery['teams'] = str(delivery.get('teams', []))
                all_deliveries.append(delivery)

        except Exception as e:
            print(f"  Skipping {filepath.name}: {e}")

    # Spark DataFrame banao
    df = spark.createDataFrame(all_deliveries, schema=schema)

    # date string → actual date type
    df = df.withColumn("date", F.to_date("date", "yyyy-MM-dd"))

    print(f" Loaded {df.count():,} deliveries")
    print(f"   Partitions: {df.rdd.getNumPartitions()}")
    return df

# ─────────────────────────────────────────
# STEP 3 — Data Quality
# Pandas: df[condition]
# Spark:  df.filter(condition)
# ─────────────────────────────────────────

def clean_data_spark(df):
    original = df.count()

    df = df.filter(F.col("runs_off_bat").between(0, 6))
    df = df.filter(F.col("batter") != "")
    df = df.filter(F.col("batter").isNotNull())
    df = df.filter(F.col("bowler").isNotNull())

    cleaned = df.count()
    print(f"\n Cleaned: {original - cleaned} bad rows removed")
    print(f"   Final: {cleaned:,} deliveries")
    return df

# ─────────────────────────────────────────
# STEP 4 — Analytics
# Pandas: .groupby().agg()
# Spark:  .groupBy().agg()
# ─────────────────────────────────────────

def top_batters_spark(df, top_n: int = 10):
    """
    PANDAS:                          SPARK:
    df.groupby('batter')             df.groupBy('batter')
      .agg(runs=('runs','sum'))        .agg(F.sum('runs_off_bat')
      .reset_index()                      .alias('runs'))
    """
    batting = (
        df.groupBy("batter")
        .agg(
            F.sum("runs_off_bat").alias("runs"),
            F.count("runs_off_bat").alias("balls_faced"),
            F.sum(
                F.when(F.col("runs_off_bat") == 4, 1).otherwise(0)
            ).alias("fours"),
            F.sum(
                F.when(F.col("runs_off_bat") == 6, 1).otherwise(0)
            ).alias("sixes"),
        )
    )

    # Strike rate = derived column
    # Pandas: batting['sr'] = batting['runs']/batting['balls']*100
    # Spark:  .withColumn('sr', F.col('runs')/F.col('balls')*100)
    batting = batting.withColumn(
        "strike_rate",
        F.round(F.col("runs") / F.col("balls_faced") * 100, 2)
    )

    return (
        batting
        .orderBy(F.col("runs").desc())   # Pandas: .sort_values('runs', ascending=False)
        .limit(top_n)                     # Pandas: .head(top_n)
    )

def top_bowlers_spark(df, top_n: int = 10):
    # Economy rate
    economy = (
        df.groupBy("bowler")
        .agg(
            F.sum("total_runs").alias("runs_given"),
            F.count("total_runs").alias("balls_bowled"),
        )
        .withColumn(
            "economy",
            F.round(F.col("runs_given") / F.col("balls_bowled") * 6, 2)
        )
        .filter(F.col("balls_bowled") >= 60)  # min 10 overs
    )

    # Wickets — sirf non-null wicket rows
    wickets = (
        df.filter(F.col("wicket").isNotNull())
        .groupBy("bowler")
        .agg(F.count("wicket").alias("wickets"))
    )

    # Join karo
    # Pandas: economy_df.merge(wickets_df, on='bowler', how='left')
    # Spark:  economy.join(wickets, on='bowler', how='left')
    bowling = (
        economy
        .join(wickets, on="bowler", how="left")
        .fillna(0, subset=["wickets"])         # Pandas: .fillna(0)
        .orderBy(F.col("wickets").desc())
        .limit(top_n)
    )
    return bowling

def powerplay_analysis_spark(df):
    """
    Pandas: pd.cut(df['over'], bins=[...])
    Spark:  F.when().when().otherwise()
    """
    df = df.withColumn(
        "phase",
        F.when(F.col("over") <= 5, "Powerplay (1-6)")
         .when(F.col("over") <= 14, "Middle (7-15)")
         .otherwise("Death (16-20)")
    )

    return (
        df.groupBy("batting_team", "phase")
        .agg(
            F.sum("runs_off_bat").alias("runs"),
            F.count("runs_off_bat").alias("balls"),
            F.sum(
                F.when(F.col("wicket").isNotNull(), 1).otherwise(0)
            ).alias("wickets_lost"),
        )
        .withColumn(
            "run_rate",
            F.round(F.col("runs") / F.col("balls") * 6, 2)
        )
        .orderBy("batting_team", "phase")
    )

# ─────────────────────────────────────────
# STEP 5 — Run everything
# ─────────────────────────────────────────

def run_spark_analysis(data_dir: str = "data"):
    spark = create_spark_session()

    print("=" * 60)
    print(" CricketPulse — Spark Season Analysis")
    print("=" * 60)

    # Load + Clean
    df = load_season_data_spark(spark, data_dir)
    df = clean_data_spark(df)

    # Cache karo — baar baar same data read nahi hoga
    # Pandas mein yeh automatic tha
    # Spark mein explicit kehna padta hai
    df.cache()

    print("\n TOP 10 BATTERS:")
    print("-" * 60)
    top_batters_spark(df).show(truncate=False)

    print("\n TOP 10 BOWLERS:")
    print("-" * 60)
    top_bowlers_spark(df).show(truncate=False)

    print("\n POWERPLAY ANALYSIS:")
    print("-" * 60)
    powerplay_analysis_spark(df).show(truncate=False)

    # Save as Parquet — production standard format
    # Pandas: df.to_csv('file.csv')
    # Spark:  df.write.parquet('folder/')
    print("\n Saving to Parquet...")
    (
        top_batters_spark(df)
        .write
        .mode("overwrite")         # pehle se file hai toh overwrite karo
        .parquet("batch/output/top_batters_spark")
    )
    print(" Saved to batch/output/top_batters_spark/")

    # Spark UI — browser mein dekh sab kuch
    print("\n Spark UI: http://localhost:4040")
    print("   (script chal raha ho tab hi open karo)")

    spark.stop()

if __name__ == "__main__":
    run_spark_analysis("data")