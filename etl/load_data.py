import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import json
import time

load_dotenv()

DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("HOST_PORT")
DB_NAME = os.getenv("POSTGRES_DB")
DB_TABLE = os.getenv("POSTGRES_TABLE")

def load_to_db(df: pd.DataFrame):
    """Bulk insert DataFrame into PostgreSQL using execute_values (fast)."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        logging.info(f"Appending {df.shape[0]} rows into table '{DB_TABLE}'")
        
        # Prepare data for insertion
        records = []
        for _, row in df.iterrows():
            records.append((
                row['name_common'],
                row['name_official'],
                row['cca3'],
                row['capital'],
                row['region'],
                row['subregion'],
                row['population'],
                row['area'],
                row['flag_png'],
                row['flag_svg'],
                json.dumps(row['languages']) if row['languages'] else None,
                json.dumps(row['currencies']) if row['currencies'] else None
            ))
        
        columns = (
            "name_common","name_official","cca3","capital","region","subregion",
            "population","area","flag_png","flag_svg","languages","currencies"
        )
        
        start_time = time.perf_counter()
        execute_values(
            cursor,
            f"INSERT INTO {DB_TABLE} ({', '.join(columns)}) VALUES %s",
            records
        )
        conn.commit()
        end_time = time.perf_counter()

        logging.info(f"Data successfully appended to '{DB_TABLE}'")
        logging.info(f"Time to write data: {end_time - start_time:.2f} seconds")

    except Exception as e:
        logging.error(f"Failed to load data into DB: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
