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
import sys

# The ETL stage logic lives in /opt/airflow/scripts (mounted, not packaged). The
# heavy imports (boto3 / pandas / the script modules) are done lazily *inside* the
# task functions, not here — so DagBag parsing (and the CI dag-validate job) stays
# light and import-error-free even where those deps aren't installed.
SCRIPTS_DIR = "/opt/airflow/scripts"

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
        """Generate the Faker data and upload it to this run's raw partition.

        Uses the ``aws_default`` Airflow connection via ``S3Hook`` for credentials
        (connections as code) instead of an ambient boto3 client, and calls the
        ingestion functions directly — no subprocess, so failures and logs surface
        natively in the task.
        """
        sys.path.insert(0, SCRIPTS_DIR)
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        from generate_dirty_data_S3 import create_dirty_data, upload_to_s3

        local_path = os.environ["LOCAL_DIRTY_PATH"]
        bucket = os.environ["S3_BUCKET_NAME"]  # provisioned by infra/terraform, not created here
        key = _partitioned_key(ds)

        create_dirty_data(local_path)
        client = S3Hook(aws_conn_id="aws_default").get_conn()  # boto3 S3 client from the Airflow conn
        upload_to_s3(client, local_path, bucket, key)
        return "Ingestion-Complete"

    # SparkSubmitOperator to run the cleaning step. Submits to the spark_default
    # connection (the standalone cluster); the script honours SPARK_MASTER. The
    # templated env var points the script at the same date partition the ingestion
    # task wrote to (also used to derive the rejects/quality S3 keys).
    clean_data = SparkSubmitOperator(
        task_id='spark-clean-task',
        application='/opt/airflow/scripts/clean_dirty_data_S3.py',
        conn_id='spark_default',
        deploy_mode='client',
        executor_memory="4G",
        driver_memory="4G",
        env_vars={"S3_FILE_KEY": "raw/dt={{ ds }}/dirty-data.csv"},
        verbose=True
    )

    @task
    def run_loading() -> str:
        """Bulk-load the cleaned CSV into PostgreSQL using the Airflow connection.

        Sources DB credentials from the ``postgres_default`` Airflow connection (via
        ``BaseHook``) and injects a connection factory into ``load_to_database`` — so
        the loader stays standalone-runnable while the DAG keeps secrets in Airflow's
        connection store rather than reading them from raw env in the script.
        """
        sys.path.insert(0, SCRIPTS_DIR)
        import psycopg2
        from airflow.hooks.base import BaseHook
        from load_to_db_final import load_to_database

        conn = BaseHook.get_connection("postgres_default")

        def _connect(dbname):
            return psycopg2.connect(
                dbname=dbname, user=conn.login, password=conn.password,
                host=conn.host, port=conn.port,
            )

        load_to_database(connect=_connect)
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
