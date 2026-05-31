"""Load stage: bulk-load the cleaned CSV into PostgreSQL.

Runs as the ``run_loading`` Airflow task. Creates the target database and ``users``
table if needed, then performs an idempotent bulk upsert with ``execute_values`` and
``ON CONFLICT (user_id) DO NOTHING``. Configuration is read from environment variables
(``DB_HOST``, ``DB_PORT``, ``DEFAULT_DB``, ``TARGET_DB``, ``DB_USER``, ``DB_PASS``,
``LOCAL_CLEAN_PATH``).
"""
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_to_database() -> None:
    """Read the cleaned CSV and bulk-upsert it into the target PostgreSQL database."""
    #--- Load environment variables ---
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT")
    DEFAULT_DB = os.getenv("DEFAULT_DB") # Database to connect to for creating the target database (e.g., "postgres")
    TARGET_DB = os.getenv("TARGET_DB") # Database to load data into (e.g., "clean_data_db") 
    DB_USER = os.getenv("DB_USER")
    DB_PASS = os.getenv("DB_PASS")
    LOCAL_CLEAN_PATH = os.getenv("LOCAL_CLEAN_PATH")

    if not os.path.exists(LOCAL_CLEAN_PATH):
        logger.info(f"❌ File not found: {LOCAL_CLEAN_PATH}")
        return

    # --- Load cleaned CSV data into a DataFrame ---
    df = pd.read_csv(LOCAL_CLEAN_PATH)
    if df.empty:
        logger.info("⚠️ CSV file is empty. No data to insert.")
        return

    # --- Step 1: Connect to default DB to create the database if needed ---
    try:
        conn = psycopg2.connect(
            dbname=DEFAULT_DB,  
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        conn.autocommit = True
        cur = conn.cursor()

        # --- Step 2: Check if database exists ---
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (TARGET_DB,))
        if not cur.fetchone():
            logger.info(f"📦 Database '{TARGET_DB}' does not exist. Creating...")
            cur.execute(f"CREATE DATABASE {TARGET_DB};")
        else:
            logger.info(f"✅ Database '{TARGET_DB}' already exists.")

        cur.close()
        conn.close()

    except Exception as e:
        logger.error(f"❌ Error checking/creating database: {e}")
        return

    # --- Step 3: Connect to the target database and insert data ---
    try:
        # Connect to the target database    
        conn = psycopg2.connect(
            dbname=TARGET_DB,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        # Create cursor
        cur = conn.cursor()
        
        # Create table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS users (
            id SERIAL PRIMARY KEY,
            user_id TEXT UNIQUE NOT NULL,
            name TEXT,
            email TEXT,
            phone TEXT,
            zip_code INTEGER,
            age INTEGER,
            city TEXT
        );
        """
        cur.execute(create_table_query) # Create the users table if it doesn't exist
        conn.commit() # Commit the table creation

        # Insert data using execute_values for efficient bulk insert
        insert_query = f"""
        INSERT INTO users ({', '.join(df.columns)}) VALUES %s
        ON CONFLICT (user_id) DO NOTHING;
        """
        data = [tuple(row) for row in df.to_numpy()]
        execute_values(cur, insert_query, data)
        conn.commit()

        logger.info(f"✅ Inserted {len(df)} rows into '{TARGET_DB}.users'")

    except Exception as e:
        logger.error(f"❌ Error inserting data: {e}")
    finally:
        if 'cur' in locals(): cur.close()  # noqa: E701
        if 'conn' in locals(): conn.close()  # noqa: E701


if __name__ == "__main__":
    load_to_database()
