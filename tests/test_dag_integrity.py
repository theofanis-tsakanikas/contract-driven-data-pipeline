"""DAG integrity tests for the s3-to-postgres-etl pipeline.

These load the DAG with Airflow's DagBag and inspect it statically — no
scheduler, no executor, no DB run. They assert the file imports with zero
errors, the dag_id and task set are as documented, the linear dependency chain
holds, the DAG is manual-only (schedule=None, catchup=False), and there are no
cycles.

Airflow's pins are heavy and conflict with the PySpark test environment, so it
is installed only in the dedicated ``dag-validate`` CI job. When Airflow is not
importable (the regular ``test`` job, or a plain dev checkout) the whole module
is skipped rather than failing collection.
"""
import os
from pathlib import Path

import pytest

pytest.importorskip("airflow", reason="Airflow not installed; run in the dag-validate job")

from airflow.models import DagBag  # noqa: E402
from airflow.utils.dag_cycle_tester import check_cycle  # noqa: E402

DAG_ID = "s3-to-postgres-etl"
DAGS_DIR = Path(__file__).resolve().parents[1] / "dags"

EXPECTED_TASK_IDS = {"run_ingestion", "spark-clean-task", "run_loading", "run_dbt", "run_dbt_test"}
# task_id -> set of immediate downstream task_ids
EXPECTED_EDGES = {
    "run_ingestion": {"spark-clean-task"},
    "spark-clean-task": {"run_loading"},
    "run_loading": {"run_dbt"},
    "run_dbt": {"run_dbt_test"},
    "run_dbt_test": set(),
}


@pytest.fixture(scope="module")
def dagbag():
    # A tmp AIRFLOW_HOME keeps DagBag parsing from touching a real install.
    os.environ.setdefault("AIRFLOW_HOME", "/tmp/airflow-dag-tests")
    return DagBag(dag_folder=str(DAGS_DIR), include_examples=False)


def test_dagbag_imports_without_errors(dagbag):
    assert dagbag.import_errors == {}, f"DAG import errors: {dagbag.import_errors}"


def test_dag_id_is_registered(dagbag):
    assert DAG_ID in dagbag.dags


def test_expected_tasks_exist(dagbag):
    dag = dagbag.dags[DAG_ID]
    assert set(dag.task_ids) == EXPECTED_TASK_IDS


def test_dependency_chain(dagbag):
    dag = dagbag.dags[DAG_ID]
    actual = {tid: set(dag.get_task(tid).downstream_task_ids) for tid in dag.task_ids}
    assert actual == EXPECTED_EDGES


def test_schedule_is_manual_only(dagbag):
    assert dagbag.dags[DAG_ID].schedule_interval is None


def test_catchup_disabled(dagbag):
    assert dagbag.dags[DAG_ID].catchup is False


def test_no_cycles(dagbag):
    # Raises AirflowDagCycleException if a cycle is present.
    check_cycle(dagbag.dags[DAG_ID])
