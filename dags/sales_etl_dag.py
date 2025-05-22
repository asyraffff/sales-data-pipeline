from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from scripts.extract_sales_data import download_latest_file
from scripts.transform_sales_data import clean_and_aggregate
from scripts.load_to_postgres import load_data_to_postgres
from scripts.data_quality_check import check_row_count
from utils.logger import logger

# dag - directed acyclic graph

# tasks : 1) fetch sales data from s3 (extract) 2) clean data (transform) 3) data quality checking 4) create and store data in table on postgres (load)
# operators : PythonOperator and PostgresOperator
# hooks - allows connection to postgres
# dependencies


# The pipeline will be scheduled to run daily
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 21),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'sales_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline for daily sales data',
    schedule='@daily',
    catchup=False,
)

# operators : PythonOperator and PostgresOperator
# hooks - allows connection to postgres

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=download_latest_file,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=clean_and_aggregate,
    dag=dag,
)

quality_check_task = PythonOperator(
    task_id='quality_check_task',
    python_callable=check_row_count,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_data_to_postgres,
    dag=dag,
)

# dependencies

extract_task >> transform_task >> quality_check_task >> load_task