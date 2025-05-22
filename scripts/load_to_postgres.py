import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from utils.logger import setup_logging
from dotenv import load_dotenv
import os

# Setup logging
logger = setup_logging()

tagging = "load_data_to_postgres"

def load_data_to_postgres(**kwargs):
    logger.info(f"{tagging} - Starting data loading task...")

    ti = kwargs['ti']
    cleaned_file = ti.xcom_pull(task_ids='transform_task', key='cleaned_file_path')

    try:
        # Load environment variables
        load_dotenv()
        conn = psycopg2.connect(
            host=os.getenv("PG_HOST"),
            database=os.getenv("PG_DATABASE"),
            user=os.getenv("PG_USER"),
            password=os.getenv("PG_PASSWORD"),
            port=os.getenv("PG_PORT")
        )
        cur = conn.cursor()

        # Create table if not exists
        logger.debug(f"{tagging} - Ensuring table structure...")
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_by_category (
                id SERIAL PRIMARY KEY,
                category VARCHAR(255) NOT NULL,
                total_sales NUMERIC NOT NULL
            );
        """)
        conn.commit()
        logger.info(f"{tagging} - Table 'sales_by_category' is ready.")

        # Load cleaned data
        logger.debug(f"{tagging} - Reading cleaned CSV for loading...")
        df = pd.read_csv(cleaned_file)

        # Prepare data tuples
        data_tuples = list(df.itertuples(index=False, name=None))

        # Insert into PostgreSQL using batch insert (more efficient)
        insert_query = """
            INSERT INTO sales_by_category (category, total_sales)
            VALUES (%s, %s);
        """

        logger.info(f"{tagging} - Loading {len(data_tuples)} rows into PostgreSQL...")
        execute_batch(cur, insert_query, data_tuples, page_size=1000)
        conn.commit()

        cur.close()
        conn.close()
        logger.info(f"{tagging} - Successfully loaded data into PostgreSQL.")

    except Exception as e:
        logger.exception(f"{tagging} - Error during data loading: %s", str(e))
        raise