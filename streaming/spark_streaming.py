from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType
)
import psycopg2
import json
import sys
import os

# ─────────────────────────────────────────
# STEP 1 — Spark Session with Kafka support
# ─────────────────────────────────────────

def create_spark_session():
    return (
        SparkSession.builder
        .appName("CricketPulse-Streaming")
        .master("local[3]")
        # local[3] = 3 cores
        # 1 core = receiver (Kafka se padho)
        # 2 cores = processing (compute karo)

        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5")
        # Kafka connector JAR — automatically download hoga
        # 2.12 = Scala version, 3.5.0 = Spark version

        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.checkpointLocation",
                "/tmp/cricket_checkpoint")
        # Checkpoint = progress track karo
        # Agar streaming job crash ho → yahan se resume karo
        # Exactly-once processing guarantee

        .getOrCreate()
    )

# ─────────────────────────────────────────
# STEP 2 — Kafka se read karo (source)
# ─────────────────────────────────────────

def read_kafka_stream(spark):
    """
    Kafka topic se continuously read karo
    Har message ek row ban jaata hai DataFrame mein
    """
    return (
        spark.readStream              # ← readStream (not read)
        .format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9092")
        .option("subscribe", "ipl-balls")
        # subscribe = kaunsa topic sunno

        .option("startingOffsets", "earliest")
        # earliest = topic shuruat se padho
        # latest   = sirf naye messages

        .option("failOnDataLoss", "false")
        # Agar kuch messages expire ho jaayein → crash mat karo
        .load()
    )
    # Result: DataFrame with columns:
    # key (binary), value (binary), topic, partition,
    # offset, timestamp, timestampType

# ─────────────────────────────────────────
# STEP 3 — Messages parse karo
# ─────────────────────────────────────────

def parse_messages(kafka_df):
    """
    Kafka value = binary bytes
    JSON parse karo → proper columns banao
    """

    # Ball event ka schema
    event_schema = StructType([
        StructField("match_id",     StringType(),  True),
        StructField("batting_team", StringType(),  True),
        StructField("batter",       StringType(),  True),
        StructField("bowler",       StringType(),  True),
        StructField("over",         IntegerType(), True),
        StructField("ball",         IntegerType(), True),
        StructField("runs_off_bat", IntegerType(), True),
        StructField("total_runs",   IntegerType(), True),
        StructField("wicket",       StringType(),  True),
    ])

    return (
        kafka_df
        # value column = bytes → string
        .withColumn("value_str",
            F.col("value").cast(StringType()))

        # JSON string → struct (nested columns)
        .withColumn("event",
            F.from_json(F.col("value_str"), event_schema))

        # Struct se individual columns nikalo
        .select(
            F.col("event.match_id").alias("match_id"),
            F.col("event.batting_team").alias("batting_team"),
            F.col("event.batter").alias("batter"),
            F.col("event.bowler").alias("bowler"),
            F.col("event.over").alias("over_num"),
            F.col("event.ball").alias("ball_num"),
            F.col("event.runs_off_bat").alias("runs_off_bat"),
            F.col("event.total_runs").alias("total_runs"),
            F.col("event.wicket").alias("wicket"),
            F.col("timestamp").alias("event_time"),
            # Kafka message timestamp = event ka time
        )

        # Invalid rows filter karo
        .filter(F.col("batter").isNotNull())
        .filter(F.col("runs_off_bat").between(0, 6))
    )

# ─────────────────────────────────────────
# STEP 4 — Real-time aggregations
# ─────────────────────────────────────────

def compute_live_stats(parsed_df):
    """
    Streaming aggregations — running totals
    groupBy + agg on streaming DataFrame
    """
    return (
        parsed_df
        .groupBy("batter")
        .agg(
            F.sum("runs_off_bat").alias("live_runs"),
            F.count("runs_off_bat").alias("live_balls"),
            F.sum(
                F.when(F.col("runs_off_bat") == 4, 1).otherwise(0)
            ).alias("live_fours"),
            F.sum(
                F.when(F.col("runs_off_bat") == 6, 1).otherwise(0)
            ).alias("live_sixes"),
        )
        .withColumn("live_sr",
            F.round(
                F.col("live_runs") / F.col("live_balls") * 100, 2
            )
        )
        .orderBy(F.col("live_runs").desc())
    )

# ─────────────────────────────────────────
# STEP 5 — PostgreSQL mein write karo (sink)
# ─────────────────────────────────────────

def write_to_postgres(batch_df, batch_id):
    """
    foreachBatch — har micro-batch ke baad yeh function call hogi
    batch_df = current batch ka DataFrame (regular, not streaming)
    batch_id = batch number (0, 1, 2...)
    """
    rows = batch_df.collect()
    # .collect() = Spark DataFrame → Python list
    # Streaming mein collect() se careful raho — large batches pe slow

    if not rows:
        print(f"Batch {batch_id}: No data")
        return

    conn = psycopg2.connect(
        host="localhost", port=5432,
        database="cricketpulse",
        user="admin", password="admin123"
    )
    cur = conn.cursor()

    # Live stats table create (agar nahi hai)
    cur.execute("""
        CREATE TABLE IF NOT EXISTS live_batter_stats (
            batter_name VARCHAR(100) PRIMARY KEY,
            live_runs   INTEGER DEFAULT 0,
            live_balls  INTEGER DEFAULT 0,
            live_fours  INTEGER DEFAULT 0,
            live_sixes  INTEGER DEFAULT 0,
            live_sr     FLOAT DEFAULT 0.0,
            batch_id    INTEGER,
            updated_at  TIMESTAMP DEFAULT NOW()
        )
    """)

    # Har batter ke live stats upsert karo
    for row in rows:
        cur.execute("""
            INSERT INTO live_batter_stats
                (batter_name, live_runs, live_balls,
                 live_fours, live_sixes, live_sr, batch_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (batter_name) DO UPDATE SET
                live_runs  = EXCLUDED.live_runs,
                live_balls = EXCLUDED.live_balls,
                live_fours = EXCLUDED.live_fours,
                live_sixes = EXCLUDED.live_sixes,
                live_sr    = EXCLUDED.live_sr,
                batch_id   = EXCLUDED.batch_id,
                updated_at = NOW()
        """, (
            row.batter, row.live_runs, row.live_balls,
            row.live_fours, row.live_sixes,
            float(row.live_sr) if row.live_sr else 0.0,
            batch_id
        ))

    conn.commit()
    cur.close()
    conn.close()

    print(f" Batch {batch_id}: {len(rows)} batters updated in PostgreSQL")

# ─────────────────────────────────────────
# STEP 6 — Pipeline start karo
# ─────────────────────────────────────────

def run_streaming_pipeline():
    spark = create_spark_session()

    print("="*60)
    print(" CricketPulse — Spark Structured Streaming")
    print("="*60)
    print("Listening to Kafka topic: ipl-balls")
    print("Writing to: PostgreSQL → live_batter_stats")
    print("Press Ctrl+C to stop\n")

    # Read
    kafka_df = read_kafka_stream(spark)

    # Parse
    parsed_df = parse_messages(kafka_df)

    # Aggregate
    stats_df = compute_live_stats(parsed_df)

    # Write — foreachBatch sink
    query = (
        stats_df
        .writeStream
        # ← writeStream (not write)

        .foreachBatch(write_to_postgres)
        # foreachBatch = har micro-batch pe function call karo

        .outputMode("complete")
        # complete = poori aggregation har baar write karo
        # groupBy ke saath complete ya update use karo

        .trigger(processingTime="10 seconds")
        # Har 10 seconds mein ek batch process karo

        .option("checkpointLocation", "/tmp/cricket_checkpoint")
        .start()
    )

    # Streaming query alive rakho
    try:
        query.awaitTermination()
        # Yahan ruka rehta hai jab tak Ctrl+C na dabao
    except KeyboardInterrupt:
        print("\n⏹ Streaming stopped by user")
        query.stop()
        spark.stop()

if __name__ == "__main__":
    run_streaming_pipeline()