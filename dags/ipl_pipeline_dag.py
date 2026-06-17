from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import sys
import os

# ─────────────────────────────────────────
# DEFAULT ARGS — har task pe apply hoga
# ─────────────────────────────────────────
default_args = {
    'owner': 'rahul',                    # kaun responsible hai
    'retries': 2,                         # fail hone pe 2 baar retry
    'retry_delay': timedelta(minutes=5),  # retry se pehle 5 min wait
    'email_on_failure': False,            # email alert (abhi off)
    'depends_on_past': False,             # pichla run fail ho toh bhi chalo
}

# ─────────────────────────────────────────
# TASK FUNCTIONS
# ─────────────────────────────────────────

def task_validate_data(**context):
    """
    Task 1 — Data check karo
    XCom se next task ko info pass karo
    """
    from pathlib import Path
    
    data_dir = '/opt/airflow/data'
    files = list(Path(data_dir).glob('*.json'))
    
    if not files:
        raise ValueError(f"No match files found in {data_dir}")
    
    print(f"✅ Found {len(files)} match files")
    print(f"   First match: {files[0].name}")
    
    # XCom — task ke beech data pass karna
    # context['ti'] = task instance
    context['ti'].xcom_push(
        key='match_count',
        value=len(files)
    )
    return len(files)

def task_run_spark_analysis(**context):
    """
    Task 2 — PySpark se IPL analysis chalao
    Pichle task se XCom se match_count lo
    """
    # XCom pull — pichle task ka value lo
    match_count = context['ti'].xcom_pull(
        task_ids='validate_data',
        key='match_count'
    )
    print(f"📊 Processing {match_count} matches with Spark...")
    
    # Spark imports
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
    
    sys.path.insert(0, '/opt/airflow/ingestion')
    from cricsheet_parser import load_match, extract_deliveries, get_all_match_files
    
    spark = (SparkSession.builder
             .appName("CricketPulse-Airflow")
             .master("local[2]")
             .config("spark.sql.shuffle.partitions", "8")
             .getOrCreate())
    
    # Load data
    files = get_all_match_files('/opt/airflow/data')[:20]
    all_deliveries = []
    
    for filepath in files:
        try:
            match = load_match(str(filepath))
            meta  = match.get('info', {})
            date  = meta.get('dates', ['unknown'])[0]
            venue = meta.get('venue', 'unknown')
            for d in extract_deliveries(match):
                d['date']  = date
                d['venue'] = venue
                all_deliveries.append(d)
        except Exception as e:
            print(f"Skip {filepath}: {e}")
    
    df = spark.createDataFrame(all_deliveries)
    df = df.filter(F.col('runs_off_bat').between(0, 6))
    df.cache()
    
    # Top batters
    batters = (
        df.groupBy('batter')
        .agg(
            F.sum('runs_off_bat').alias('runs'),
            F.count('runs_off_bat').alias('balls')
        )
        .withColumn('strike_rate',
            F.round(F.col('runs') / F.col('balls') * 100, 2))
        .orderBy(F.col('runs').desc())
        .limit(10)
    )
    
    # Output save karo
    output_path = '/opt/airflow/batch/output/airflow_run'
    batters.write.mode('overwrite').parquet(output_path)
    
    row_count = df.count()
    spark.stop()
    
    print(f"✅ Processed {row_count:,} deliveries")
    print(f"   Output: {output_path}")
    
    # XCom mein result pass karo
    context['ti'].xcom_push(key='row_count', value=row_count)
    return row_count

def task_generate_report(**context):
    """
    Task 3 — Summary report banao
    """
    row_count = context['ti'].xcom_pull(
        task_ids='run_spark_analysis',
        key='row_count'
    )
    
    run_date = context['ds']  # Airflow execution date — "2024-03-22"
    
    report = f"""
    ==========================================
    CricketPulse Daily Report — {run_date}
    ==========================================
    Deliveries processed : {row_count:,}
    Output location      : batch/output/airflow_run/
    Status               : SUCCESS
    ==========================================
    """
    print(report)
    
    # Report file save karo
    os.makedirs('/opt/airflow/batch/reports', exist_ok=True)
    with open(f'/opt/airflow/batch/reports/report_{run_date}.txt', 'w') as f:
        f.write(report)
    
    return "Report generated"

# ─────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────

with DAG(
    dag_id='ipl_daily_pipeline',          # unique naam
    default_args=default_args,
    description='Daily IPL data pipeline — validate → spark → report',
    schedule='@daily',                     # roz raat 12 baje
    start_date=datetime(2024, 1, 1),       # pehli baar kab run hoga
    catchup=False,                         # purane missed runs mat karo
    tags=['cricket', 'spark', 'de'],       # UI mein filter ke liye
) as dag:

    # ── Task 1 ──
    validate = PythonOperator(
        task_id='validate_data',
        python_callable=task_validate_data,
    )

    # ── Task 2 ──
    analyze = PythonOperator(
        task_id='run_spark_analysis',
        python_callable=task_run_spark_analysis,
    )

    # ── Task 3 ──
    report = PythonOperator(
        task_id='generate_report',
        python_callable=task_generate_report,
    )

    # ── Dependencies — order define karo ──
    # validate pehle → analyze → report
    validate >> analyze >> report
    #    ↑          ↑         ↑
    # Task 1     Task 2    Task 3
    # >> operator = "pehle yeh, phir woh"