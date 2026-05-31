from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Define the DAG using the @dag decorator
@dag(
    dag_id='s3-to-postgres-etl',
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    tags=['spark', 's3', 'postgres']
)

def etl_pipeline():
    # Placeholder function to trigger the ingestion task
    @task
    def run_ingestion():

        subprocess.run(['python3', '/opt/airflow/scripts/generate_dirty_data_S3.py'], check=True)
        return "Ingestion-Complete"

    # SparkSubmitOperator to run the cleaning step
    clean_data = SparkSubmitOperator(
        task_id='spark-clean-task',
        application='/opt/airflow/scripts/clean_dirty_data_S3.py',
        deploy_mode='client',
        executor_memory="4G",   
        driver_memory="4G",      
        verbose=True
    )

    # Placeholder function to trigger the loading task
    @task
    def run_loading():

        subprocess.run(['python3', '/opt/airflow/scripts/load_to_db_final.py'], check=True)
        return "Loading-Complete"

    # dbt builds the silver/analytics layer on top of the loaded users table.
    # dbt lives in an isolated venv (see Dockerfile.airflow) to avoid dependency
    # clashes with Airflow; project and profiles are mounted at /opt/airflow/dbt.
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=(
            '/home/airflow/dbt-venv/bin/dbt run '
            '--project-dir /opt/airflow/dbt '
            '--profiles-dir /opt/airflow/dbt'
        ),
    )

    ingest_step = run_ingestion()
    load_step = run_loading()

    ingest_step >> clean_data >> load_step >> run_dbt

# Εκτέλεση του DAG
etl_pipeline()