"""Transform stage: download the raw CSV from S3 and clean it with PySpark.

Runs as the ``spark-clean-task`` Airflow task. ``clean_dataframe`` is the pure,
unit-tested transformation core (no I/O); the surrounding functions handle S3 download
and local single-file output. The raw object stays in S3 under its date-partitioned
key (``raw/dt=YYYY-MM-DD/...``) so every run leaves an auditable raw-zone history.
Configuration is read from environment variables (``S3_BUCKET_NAME``, ``S3_FILE_KEY``,
``LOCAL_DIRTY_PATH``, ``LOCAL_CLEAN_FOLDER``, ``LOCAL_CLEAN_PATH``).
"""
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, trim, length, md5, concat_ws
from pyspark.sql.types import IntegerType
from pyspark.sql.types import StructType, StructField, StringType
import boto3
import os
import glob 
import shutil
from botocore.exceptions import ClientError
import logging

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

def clean_dataframe(df: DataFrame) -> DataFrame:
    """Apply the data contract to a raw Spark DataFrame and return the cleaned one.

    Pure transformation (no I/O) so it can be unit-tested in isolation:
    trims strings, filters invalid rows, casts age, drops the source id,
    derives the deterministic user_id hash, and reorders columns.
    """
    # Clean string fields
    df = df.withColumn("name", trim(col("name"))) \
           .withColumn("email", trim(col("email"))) \
           .withColumn("phone", trim(col("phone"))) \
           .withColumn("zip_code", trim(col("zip_code"))) \
           .withColumn("city", trim(col("city")))

    # Filter invalid data
    df = df.filter(col("name").isNotNull() & (length(col("name")) > 0)) \
           .filter(col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$")) \
           .filter(col("phone").rlike(r"^69\d{8}$")) \
           .filter(col("zip_code").rlike(r"^\d{5}$"))

    # Convert age to integer and filter out invalid ages
    df = df.withColumn("age", col("age").cast(IntegerType())) \
           .filter((col("age") >= 18) & (col("age") <= 99)) \
           .filter(col("city").isNotNull() & (length(col("city")) > 0))

    # Drop id if exists
    if "id" in df.columns:
        df = df.drop("id")

    # Create user_id hash
    df = df.withColumn("user_id", md5(concat_ws("||", "name", "email", "phone"))) # Generate a unique user_id based on name, email, and phone

    # Reorder columns
    desired_order = ["user_id", "name", "email", "phone", "zip_code", "age", "city"]
    df = df.select(*desired_order)

    return df

def clean_data_with_spark(local_dirty_path: str, local_clean_folder: str, local_clean_path: str) -> None:
    """Clean dirty CSV data using PySpark."""

    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Clean Dirty Data") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


    if not os.path.exists(local_dirty_path):
        logger.error(f"❌ File not found: {local_dirty_path}")
        return

    # Load dirty CSV data into a Spark DataFrame with the expected schema
    df = spark.read \
        .option("header", "true") \
        .option("encoding", "UTF-8") \
        .schema(EXPECTED_SCHEMA) \
        .csv(local_dirty_path)

    # Apply the data contract (pure, testable transformation)
    df = clean_dataframe(df)

    # Save locally (same logic as before) as a single CSV file with UTF-8 encoding and header
    df.coalesce(1) \
      .write \
      .option("header", True) \
      .option("encoding", "UTF-8") \
      .mode("overwrite") \
      .csv(local_clean_folder)

    # Move the generated CSV file from the Spark output folder to the desired location
    csv_files = glob.glob(os.path.join(local_clean_folder, "*.csv")) # Find the generated CSV file in the Spark output folder
    if not csv_files:
        raise FileNotFoundError(f"No CSV file found in {local_clean_folder}")

    shutil.move(csv_files[0], local_clean_path) # Move the generated CSV file to the desired location
    shutil.rmtree(local_clean_folder) # Clean up the temporary Spark output folder
    logger.info(f"✅ Cleaned data saved locally to: {local_clean_path}")

    spark.stop()

def main() -> None:
    """Main ETL workflow: Download → Clean. The raw S3 object is retained."""
    # --- AWS S3 Configuration ---
    bucket_name = os.getenv("S3_BUCKET_NAME", "my-dirty-data-bucket")
    s3_file_key = os.getenv("S3_FILE_KEY", "dirty-data.csv")
    local_dirty_path = os.getenv("LOCAL_DIRTY_PATH", "/opt/airflow/data/dirty_data.csv")
    local_clean_folder = os.getenv("LOCAL_CLEAN_FOLDER", "/opt/airflow/data/clean_data_temp")
    local_clean_path = os.getenv("LOCAL_CLEAN_PATH", "/opt/airflow/data/clean_data.csv")

    # Step 1: Download dirty data from S3
    download_from_s3(bucket_name, s3_file_key, local_dirty_path)

    # Step 2: Clean the data locally with Spark and save the cleaned data locally
    clean_data_with_spark(local_dirty_path, local_clean_folder, local_clean_path)

if __name__ == "__main__":
    main()
