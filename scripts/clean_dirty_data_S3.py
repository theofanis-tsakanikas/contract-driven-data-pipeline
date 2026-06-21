"""Transform stage: download the raw CSV from S3 and clean it with PySpark.

Runs as the ``spark-clean-task`` Airflow task. ``clean_dataframe`` is the pure,
unit-tested transformation core (no I/O); the surrounding functions handle S3 download
and local single-file output. The raw object stays in S3 under its date-partitioned
key (``raw/dt=YYYY-MM-DD/...``) so every run leaves an auditable raw-zone history.
Configuration is read from environment variables (``S3_BUCKET_NAME``, ``S3_FILE_KEY``,
``LOCAL_DIRTY_PATH``, ``LOCAL_CLEAN_FOLDER``, ``LOCAL_CLEAN_PATH``).
"""
from pyspark.sql import SparkSession, DataFrame, Column
from pyspark.sql.functions import col, trim, length, md5, concat_ws, when, lit
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField, StringType
import boto3
import os
import glob
import shutil
import json
from dataclasses import dataclass, field
from functools import reduce
from operator import and_
from botocore.exceptions import ClientError
import logging

from data_contract import (
    CONTRACT,
    CLEAN_COLUMNS,
    INT_RANGE,
    NON_EMPTY,
    REGEX,
    SURROGATE_KEY,
    SURROGATE_SOURCES,
    FieldRule,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- AWS S3 client ---
s3_client = boto3.client("s3")

# --- CONFIGURATION / DATA CONTRACT ---
EXPECTED_SCHEMA = StructType([
    StructField("id", StringType(), True),
    StructField("name", StringType(), True),
    StructField("email", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("age", StringType(), True),
    StructField("city", StringType(), True)
])

def download_from_s3(bucket_name: str, s3_file_key: str, local_dirty_path: str) -> None:
    """Download dirty_data.csv from S3 bucket."""
    try:
        s3_client.download_file(bucket_name, s3_file_key, local_dirty_path)
        logger.info(f"✅ Downloaded '{s3_file_key}' from S3 bucket '{bucket_name}'")
    except ClientError as e:
        logger.error(f"❌ Error downloading file from S3: {e}")
        raise


def _zone_key(raw_key: str, zone: str, filename: str) -> str:
    """Map the raw-zone key to a sibling zone, preserving the date partition.

    ``raw/dt=2026-06-10/dirty-data.csv`` → ``rejects/dt=2026-06-10/rejected_data.csv``.
    Keeps rejects and DQ reports auditable in the lake next to the raw object they
    came from, rather than only on the local (gitignored) data mount.
    """
    partition = next((p for p in raw_key.split("/") if p.startswith("dt=")), None)
    prefix = f"{zone}/{partition}" if partition else zone
    return f"{prefix}/{filename}"


def _upload_artifact(bucket_name: str, local_path: str, s3_key: str) -> None:
    """Upload a run artifact (rejects / DQ report) back to S3. Non-fatal on failure.

    The cleaned data has already been produced and loaded; a failed governance-artifact
    upload should be logged loudly but must not fail the whole clean stage.
    """
    if not bucket_name or not os.path.exists(local_path):
        return
    try:
        s3_client.upload_file(local_path, bucket_name, s3_key)
        logger.info(f"☁️  Uploaded '{local_path}' to s3://{bucket_name}/{s3_key}")
    except ClientError as e:
        logger.warning(f"⚠️ Could not upload '{local_path}' to S3 (artifact, non-fatal): {e}")

def _prepared(df: DataFrame) -> DataFrame:
    """Normalise raw columns: trim the string fields and cast age to int.

    Both the accept path (``clean_dataframe``) and the reject path
    (``rejected_dataframe``) operate on this normalised frame, so they evaluate the
    contract against exactly the same values.
    """
    for field_name in ("name", "email", "phone", "zip_code", "city"):
        df = df.withColumn(field_name, trim(col(field_name)))
    return df.withColumn("age", col("age").cast(IntegerType()))


def _rule_ok(rule: FieldRule) -> Column:
    """The null-safe 'this field is valid' predicate for one contract rule.

    Null-safe by construction (a null value fails every rule), so the accept set and
    the rejected complement are provably exhaustive — every row lands in exactly one.
    """
    c = col(rule.field)
    if rule.kind == NON_EMPTY:
        return c.isNotNull() & (length(c) > 0)
    if rule.kind == REGEX:
        return c.isNotNull() & c.rlike(rule.pattern)
    if rule.kind == INT_RANGE:
        return c.isNotNull() & (c >= rule.minimum) & (c <= rule.maximum)
    raise ValueError(f"unknown rule kind: {rule.kind!r}")


def clean_dataframe(df: DataFrame) -> DataFrame:
    """Apply the data contract to a raw Spark DataFrame and return the cleaned one.

    Pure transformation (no I/O) so it can be unit-tested in isolation: normalises
    fields, keeps only rows satisfying every contract rule, drops the source id,
    derives the deterministic ``user_id`` MD5 pseudonym, and reorders columns. The
    validation rules come from ``data_contract.CONTRACT`` (single source of truth).
    """
    df = _prepared(df)

    # Keep rows that satisfy EVERY contract rule (conjunction of the per-field predicates).
    valid = reduce(and_, (_rule_ok(rule) for rule in CONTRACT))
    df = df.filter(valid)

    # Drop the source id if present.
    if "id" in df.columns:
        df = df.drop("id")

    # Deterministic pseudonymised surrogate key (no natural key is stored downstream).
    df = df.withColumn(SURROGATE_KEY, md5(concat_ws("||", *SURROGATE_SOURCES)))

    return df.select(*CLEAN_COLUMNS)


def rejected_dataframe(df: DataFrame) -> DataFrame:
    """Return the rows the contract rejects, tagged with a ``rejection_reason``.

    The exact complement of :func:`clean_dataframe`: each rejected row carries the
    first contract rule it violated (in contract order), giving full lineage for why
    a record never reached the warehouse — instead of vanishing silently.
    """
    df = _prepared(df)

    reason = None
    for rule in CONTRACT:
        fails = ~_rule_ok(rule)
        reason = when(fails, lit(rule.reason)) if reason is None else reason.when(fails, lit(rule.reason))

    return df.withColumn("rejection_reason", reason).filter(col("rejection_reason").isNotNull())


@dataclass(frozen=True)
class DQReport:
    """Data-quality summary for one cleaning run."""

    total: int
    accepted: int
    rejected: int
    by_reason: dict = field(default_factory=dict)

    @property
    def accept_rate(self) -> float:
        return 1.0 if self.total == 0 else self.accepted / self.total

    def to_dict(self) -> dict:
        return {
            "total": self.total,
            "accepted": self.accepted,
            "rejected": self.rejected,
            "accept_rate": round(self.accept_rate, 4),
            "rejected_by_reason": self.by_reason,
        }


def data_quality_report(df: DataFrame) -> DQReport:
    """Compute the data-quality summary (total / accepted / rejected-by-reason) for a batch."""
    df = df.cache()
    try:
        total = df.count()
        by_reason = {
            row["rejection_reason"]: row["count"]
            for row in rejected_dataframe(df).groupBy("rejection_reason").count().collect()
        }
        rejected = sum(by_reason.values())
        return DQReport(total=total, accepted=total - rejected, rejected=rejected, by_reason=by_reason)
    finally:
        df.unpersist()

def _write_single_csv(df: DataFrame, temp_folder: str, final_path: str) -> None:
    """Write a DataFrame as one header CSV at ``final_path`` (coalesce + move)."""
    df.coalesce(1).write.option("header", True).option("encoding", "UTF-8").mode("overwrite").csv(temp_folder)
    csv_files = glob.glob(os.path.join(temp_folder, "*.csv"))
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {temp_folder}")
    shutil.move(csv_files[0], final_path)
    shutil.rmtree(temp_folder)


def clean_data_with_spark(
    local_dirty_path: str,
    local_clean_folder: str,
    local_clean_path: str,
    local_rejects_path: str | None = None,
    dq_report_path: str | None = None,
    bucket_name: str | None = None,
    raw_s3_key: str | None = None,
) -> None:
    """Clean dirty CSV data using PySpark, quarantine rejects, and emit a DQ report.

    The cleaned output is written exactly as before. In addition, rows that fail the
    contract are quarantined to ``local_rejects_path`` with their ``rejection_reason``
    (lineage), and a data-quality summary is written to ``dq_report_path`` and logged.
    When ``bucket_name`` / ``raw_s3_key`` are given, the rejects and DQ report are also
    uploaded back to the lake under date-partitioned ``rejects/`` and ``quality/`` zones.
    """
    # Master is configurable: when SPARK_MASTER is set (e.g. the DAG/compose set it
    # to spark://spark-master:7077) the job runs on the standalone cluster; unset
    # (standalone script run, CI) it falls back to in-process local[*]. No Python
    # UDFs are used — the transform is pure Spark SQL — so no --py-files shipping
    # of data_contract.py to executors is required.
    builder = (
        SparkSession.builder
        .appName("Clean Dirty Data")
        .config("spark.driver.memory", "2g")
        .master(os.getenv("SPARK_MASTER", "local[*]"))
    )
    spark = builder.getOrCreate()

    if not os.path.exists(local_dirty_path):
        logger.error(f"❌ File not found: {local_dirty_path}")
        return

    # Load dirty CSV data into a Spark DataFrame with the expected schema
    raw = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(EXPECTED_SCHEMA) \
        .csv(local_dirty_path) \
        .cache()

    # Data-quality report (total / accepted / rejected-by-reason) for this run.
    report = data_quality_report(raw)
    logger.info("📊 Data quality: %s", json.dumps(report.to_dict()))
    if dq_report_path:
        with open(dq_report_path, "w", encoding="utf-8") as fh:
            json.dump(report.to_dict(), fh, indent=2)
        logger.info(f"✅ Data-quality report written to: {dq_report_path}")
        if raw_s3_key:
            _upload_artifact(bucket_name, dq_report_path, _zone_key(raw_s3_key, "quality", "dq_report.json"))

    # Accepted rows → the cleaned output (unchanged behaviour).
    _write_single_csv(clean_dataframe(raw), local_clean_folder, local_clean_path)
    logger.info(f"✅ Cleaned data saved locally to: {local_clean_path}")

    # Rejected rows → quarantine with the reason (lineage), instead of vanishing.
    if local_rejects_path and report.rejected > 0:
        rejects_folder = local_rejects_path + "_temp"
        _write_single_csv(rejected_dataframe(raw), rejects_folder, local_rejects_path)
        logger.info(f"🗂️  Quarantined {report.rejected} rejected row(s) to: {local_rejects_path}")
        if raw_s3_key:
            _upload_artifact(bucket_name, local_rejects_path, _zone_key(raw_s3_key, "rejects", "rejected_data.csv"))

    raw.unpersist()
    spark.stop()

def main() -> None:
    """Main ETL workflow: Download → Clean (+ quarantine rejects + DQ report). Raw S3 object retained."""
    # --- AWS S3 Configuration ---
    bucket_name = os.getenv("S3_BUCKET_NAME", "my-dirty-data-bucket")
    s3_file_key = os.getenv("S3_FILE_KEY", "dirty-data.csv")
    local_dirty_path = os.getenv("LOCAL_DIRTY_PATH", "/opt/airflow/data/dirty_data.csv")
    local_clean_folder = os.getenv("LOCAL_CLEAN_FOLDER", "/opt/airflow/data/clean_data_temp")
    local_clean_path = os.getenv("LOCAL_CLEAN_PATH", "/opt/airflow/data/clean_data.csv")
    local_rejects_path = os.getenv("LOCAL_REJECTS_PATH", "/opt/airflow/data/rejected_data.csv")
    dq_report_path = os.getenv("DQ_REPORT_PATH", "/opt/airflow/data/dq_report.json")

    # Step 1: Download dirty data from S3
    download_from_s3(bucket_name, s3_file_key, local_dirty_path)

    # Step 2: Clean the data locally with Spark; quarantine rejects + emit the DQ
    # report, and upload those governance artifacts back to the lake.
    clean_data_with_spark(
        local_dirty_path, local_clean_folder, local_clean_path, local_rejects_path, dq_report_path,
        bucket_name=bucket_name, raw_s3_key=s3_file_key,
    )

if __name__ == "__main__":
    main()
