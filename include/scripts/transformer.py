import pandas as pd
import logging
from sqlalchemy import create_engine

# Using the standard Airflow logger
logger = logging.getLogger("airflow.task")

def run_bitcoin_transformation():
    # Path where Docker sees your files
    input_path = "/usr/local/airflow/include/data/bitstamp.csv"
    
    logger.info("Step 1: Extracting data from CSV...")
    df = pd.read_csv(input_path)
    
    logger.info("Step 2: Transforming data (Cleaning nulls and timestamps)...")
    # Convert Unix timestamp to readable date
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], unit='s')
    
    # Forward fill missing prices (common in crypto during low volume)
    df = df.ffill()
    
    # Drop any remaining rows that are entirely empty
    df.dropna(inplace=True)
    
    logger.info(f"Transformation complete. Row count: {len(df)}")
    
    # Step 3: Load to Postgres
    conn_string = "postgresql://postgres:postgres@postgres:5432/postgres"
    engine = create_engine(conn_string)
    
    # Prepare the tail data
    load_df = df.tail(100000)
    
    # NEW IDEMPOTENT LOGIC:
    # 1. Get the range of dates we are loading
    min_ts = load_df['Timestamp'].min()
    max_ts = load_df['Timestamp'].max()
    
    logger.info(f"Step 4: Syncing data from {min_ts} to {max_ts}...")
    
    from sqlalchemy import text
    with engine.begin() as conn:
        # 2. Delete existing records in this range to avoid duplicates
        conn.execute(text("DELETE FROM cleaned_bitcoin WHERE \"Timestamp\" BETWEEN :min AND :max"), 
             {"min": min_ts, "max": max_ts})
        
        # 3. Append the fresh data
        load_df.to_sql("cleaned_bitcoin", conn, if_exists='append', index=False)
        
    logger.info("Successfully synced data to database without duplicates.")