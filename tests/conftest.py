import os
import sys
from pathlib import Path

import pytest

# clean_dirty_data_S3.py creates a boto3 S3 client at import time. boto3 needs a
# region to build the client (it does NOT need credentials just to construct it),
# so set a default before the module is imported anywhere in the test session.
os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")

# The ETL scripts live in scripts/ and are not packaged; put them on sys.path.
SCRIPTS_DIR = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS_DIR))


@pytest.fixture(scope="session")
def spark():
    """A lightweight local SparkSession shared across the test session."""
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[1]")
        .appName("clean-dataframe-tests")
        .config("spark.driver.memory", "1g")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield session
    session.stop()
