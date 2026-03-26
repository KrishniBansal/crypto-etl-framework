from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import logging  
from sqlalchemy import text

# Add include folder to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from include.scripts.transformer import run_bitcoin_transformation
from include.scripts.live_api import fetch_live_bitcoin_data

# --- FAILURE ALERTS ---
def local_failure_alert(context):
    dag_id = context.get('task_instance').dag_id
    task_id = context.get('task_instance').task_id
    error = context.get('exception')
    execution_date = context.get('execution_date')
    log_message = f"--- FAILURE AT {execution_date} ---\nDAG: {dag_id}\nTask: {task_id}\nError: {error}\n\n"
    with open("/usr/local/airflow/include/error_alerts.txt", "a") as f:
        f.write(log_message)

def all_alerts_callback(context):
    local_failure_alert(context)

default_args = {
    'owner': 'data_engineer',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': all_alerts_callback,
}

with DAG(
    dag_id='enterprise_bitcoin_etl_v1',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='0 0 * * *', # Daily at midnight
    catchup=False
) as dag:

    def initialize_database():
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        
        # Path to your SQL setup file
        setup_path = "/usr/local/airflow/include/scripts/schema_setup.sql"
        
        with open(setup_path, 'r') as f:
            query = f.read()
            
        with engine.begin() as conn:
            conn.execute(text(query))
        print("Database Schema Verified/Initialized.")

    def peek_at_data():
        from sqlalchemy import create_engine
        import pandas as pd
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        
        # Write the query here
        query = "SELECT trade_date, avg_price FROM bitcoin_gold_metrics ORDER BY trade_date DESC LIMIT 5;"
        df = pd.read_sql(query, engine)
        
        print("-" * 50)
        print("AUDIT CHECK: LATEST 5 DAYS IN GOLD LAYER")
        print(df.to_string())
        print("-" * 50)

    def validate_data_quality():
        from sqlalchemy import create_engine
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        null_count = engine.connect().execute(text("SELECT count(*) FROM cleaned_bitcoin WHERE \"Close\" IS NULL")).fetchone()[0]
        if null_count > 0:
            raise ValueError(f"QUALITY ALERT: Found {null_count} null prices!")

    def create_silver_layer():
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        upsert_query = """
        INSERT INTO bitcoin_daily_summary (trade_date, daily_high, daily_low, avg_price, total_volume)
        SELECT 
            DATE("Timestamp") as trade_date,
            MAX("High"), MIN("Low"), AVG("Close"), SUM("Volume")
        FROM cleaned_bitcoin
        WHERE DATE("Timestamp") > (SELECT COALESCE(MAX(trade_date), '1900-01-01') FROM bitcoin_daily_summary)
        GROUP BY 1
        ON CONFLICT (trade_date) DO UPDATE SET
            daily_high = EXCLUDED.daily_high, daily_low = EXCLUDED.daily_low,
            avg_price = EXCLUDED.avg_price, total_volume = EXCLUDED.total_volume;
        """
        with engine.begin() as conn:
            conn.execute(text(upsert_query))

    def create_gold_analytics():
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        gold_query = """
        DROP TABLE IF EXISTS bitcoin_gold_metrics;
        CREATE TABLE bitcoin_gold_metrics AS
        SELECT 
            trade_date, avg_price,
            AVG(avg_price) OVER(ORDER BY trade_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) as rolling_7d_avg,
            (daily_high - daily_low) / NULLIF(avg_price, 0) * 100 as daily_volatility_pct
        FROM bitcoin_daily_summary;
        """
        with engine.begin() as conn:
            conn.execute(text(gold_query))

    def cleanup_bronze_layer():
        from sqlalchemy import create_engine, text
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        cleanup_query = "DELETE FROM cleaned_bitcoin WHERE \"Timestamp\" < NOW() - INTERVAL '30 days';"
        with engine.begin() as conn:
            conn.execute(text(cleanup_query))

    def load_live_data():
        from sqlalchemy import create_engine
        engine = create_engine("postgresql://postgres:postgres@postgres:5432/postgres")
        df = fetch_live_bitcoin_data()
        if not df.empty:
            df.to_sql('cleaned_bitcoin', engine, if_exists='append', index=False)

    # --- OPERATORS --
    init_db = PythonOperator(task_id='initialize_database_schema',python_callable=initialize_database)
    etl_task = PythonOperator(task_id='clean_and_load_bitcoin', python_callable=run_bitcoin_transformation)
    live_task = PythonOperator(task_id='ingest_live_price', python_callable=load_live_data)
    quality_gate = PythonOperator(task_id='validate_data_quality', python_callable=validate_data_quality)
    view_data_task = PythonOperator(task_id='peek_at_database_results', python_callable=peek_at_data)
    silver_layer = PythonOperator(task_id='create_daily_summary', python_callable=create_silver_layer)
    gold_task = PythonOperator(task_id='create_gold_metrics', python_callable=create_gold_analytics)
    cleanup_task = PythonOperator(task_id='cleanup_old_data', python_callable=cleanup_bronze_layer)

    # --- DEPENDENCY CHAIN ---
    init_db >> [etl_task, live_task] >> quality_gate >> view_data_task >> silver_layer >> gold_task >> cleanup_task