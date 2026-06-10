"""Airflow DAG (``s3-to-postgres-etl``) orchestrating the S3 → PySpark → PostgreSQL → dbt pipeline.

Task order: ``run_ingestion`` → ``spark-clean-task`` → ``run_loading`` → ``run_dbt`` → ``run_dbt_test``.
Manual trigger only (``schedule=None``, ``catchup=False``).

The raw S3 object key is date-partitioned per DAG run (``raw/dt=<ds>/dirty-data.csv``)
and injected into both the ingestion and the Spark task via the ``S3_FILE_KEY`` env
var, so each run writes to — and reads from — its own raw-zone partition and the
bucket accumulates an auditable history instead of being recreated per run.
"""
from airflow.decorators import dag, task
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import os
import subprocess

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

DBT_CMD_PREFIX = (
    '/home/airflow/dbt-venv/bin/dbt {verb} '
    '--project-dir /opt/airflow/dbt '
    '--profiles-dir /opt/airflow/dbt'
)


def _partitioned_key(ds: str) -> str:
    """Raw-zone key for one logical date, e.g. raw/dt=2026-06-10/dirty-data.csv."""
    return f"raw/dt={ds}/dirty-data.csv"


# Define the DAG using the @dag decorator
@dag(
    dag_id='s3-to-postgres-etl',
    default_args=default_args,
    start_date=datetime(2025, 10, 21),
    schedule=None,
    catchup=False,
    tags=['spark', 's3', 'postgres']
)

def etl_pipeline() -> None:
    """Define the ETL DAG: ingestion → Spark clean → load → dbt run → dbt test."""
    @task
    def run_ingestion(ds=None) -> str:
        """Run the Faker → S3 ingestion script against this run's raw partition."""
        env = {**os.environ, "S3_FILE_KEY": _partitioned_key(ds)}
        subprocess.run(['python3', '/opt/airflow/scripts/generate_dirty_data_S3.py'], check=True, env=env)
        return "Ingestion-Complete"

    # SparkSubmitOperator to run the cleaning step. The templated env var points
    # the script at the same date partition the ingestion task wrote to.
    clean_data = SparkSubmitOperator(
        task_id='spark-clean-task',
        application='/opt/airflow/scripts/clean_dirty_data_S3.py',
        deploy_mode='client',
        executor_memory="4G",
        driver_memory="4G",
        env_vars={"S3_FILE_KEY": "raw/dt={{ ds }}/dirty-data.csv"},
        verbose=True
    )

    @task
    def run_loading() -> str:
        """Run the PostgreSQL bulk-load script in a subprocess."""
        subprocess.run(['python3', '/opt/airflow/scripts/load_to_db_final.py'], check=True)
        return "Loading-Complete"

    # dbt builds the silver/analytics layer on top of the loaded users table.
    # dbt lives in an isolated venv (see Dockerfile.airflow) to avoid dependency
    # clashes with Airflow; project and profiles are mounted at /opt/airflow/dbt.
    run_dbt = BashOperator(
        task_id='run_dbt',
        bash_command=DBT_CMD_PREFIX.format(verb='run'),
    )

    # Execute the schema tests defined in dbt/models/**/_models.yml (not_null,
    # unique, accepted_values) so a bad load fails the DAG instead of silently
    # publishing broken marts.
    run_dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command=DBT_CMD_PREFIX.format(verb='test'),
    )

    ingest_step = run_ingestion()
    load_step = run_loading()

    ingest_step >> clean_data >> load_step >> run_dbt >> run_dbt_test

# Instantiate the DAG
etl_pipeline()
