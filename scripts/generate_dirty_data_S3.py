import csv
import random
import faker
import boto3
import os
from botocore.exceptions import NoCredentialsError, ClientError
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize Faker for generating random data
fake = faker.Faker('en_US')  

# AWS S3 Credentials 
s3_client = boto3.client('s3')  # Uses environment variables for credentials

# Functions to generate random values for different fields
def random_phone():
    r = random.random()
    if r < 0.5:
        return "69" + "".join(str(random.randint(0, 9)) for _ in range(8))
    elif r < 0.75:
        return fake.phone_number()
    else:
        return ""

def random_zip():
    if random.random() < 0.7:
        return "".join(str(random.randint(0, 9)) for _ in range(5))
    else:
        choices = [
            "".join(random.choices("ABCDEFGHIJKLMNOPQRSTUVWXYZ", k=5)),
            "".join(str(random.randint(0, 9)) for _ in range(random.randint(1, 4))),
            "ABCDE",
            ""
        ]
        return random.choice(choices)

def random_email(name):
    if random.random() < 0.8:
        return name.lower().replace(" ", ".") + "@" + fake.free_email_domain()
    else:
        return "invalid-email"

def random_age():
    if random.random() < 0.8:
        return random.randint(18, 90)
    else:
        return random.choice([-5, 0, 150])

def random_name():
    if random.random() < 0.9:
        return fake.name()
    else:
        return ""

def random_city():
    if random.random() < 0.85:
        return fake.city()
    else:
        return ""

def create_dirty_data(filepath):
    """Create a dirty CSV file with random data."""
    with open(filepath, mode="w", newline="", encoding="utf-8-sig") as f:
        writer = csv.writer(f)
        # Writing header for the CSV file
        writer.writerow(["id", "name", "email", "phone", "zip_code", "age", "city"])

        # Generate random data for 100 records
        for i in range(1, 101):
            name = random_name()
            email = random_email(name) if name else ""
            phone = random_phone()
            zip_code = random_zip()
            age = random_age()
            city = random_city()

            # Write the generated data to the CSV file
            writer.writerow([i, name, email, phone, zip_code, age, city])

    logger.info(f"✅ Dirty data saved to {filepath}")

def upload_to_s3(local_filepath, bucket_name, s3_file_key):
    """Upload the dirty data CSV to S3."""
    try:
        # Upload the local CSV file to the specified S3 bucket
        s3_client.upload_file(local_filepath, bucket_name, s3_file_key)
        logger.info(f"✅ File '{local_filepath}' successfully uploaded to S3 bucket '{bucket_name}' at '{s3_file_key}'.")
    except FileNotFoundError:
        logger.info(f"⚠️ The file '{local_filepath}' was not found.")
    except NoCredentialsError:
        logger.info("⚠️ No valid AWS credentials were found. Please set them via environment variables or AWS CLI.")
    except ClientError as e:
        logger.info(f"⚠️ Error uploading file to S3: {e}")

def create_bucket(bucket_name, region):
    """Create an S3 bucket in any region dynamically."""
    try:
        # Check if the bucket already exists
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"⚠️ The bucket '{bucket_name}' already exists.")
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code in ('404', 'NoSuchBucket'):
            try:
                # Configure bucket creation parameters based on the region
                bucket_config = {}
                if region != "us-east-1":
                    bucket_config['CreateBucketConfiguration'] = {'LocationConstraint': region}
                
                # Create the bucket with the appropriate configuration
                s3_client.create_bucket(Bucket=bucket_name, **bucket_config)
                
                logger.info(f"✅ Bucket '{bucket_name}' created successfully in region '{region}'.")
            except ClientError as create_err:
                logger.error(f"❌ Error creating bucket: {create_err}")
        else:
            logger.error(f"❌ Error checking bucket existence: {e}")

def main():
    # File path for the local dirty data CSV
    local_csv_path = os.getenv("LOCAL_DIRTY_PATH")
    
    # S3 bucket and file key configuration
    bucket_name = os.getenv("S3_BUCKET_NAME")
    s3_file_key = os.getenv("S3_FILE_KEY")
    region = os.getenv("AWS_DEFAULT_REGION")

    # Create dirty data and save it locally
    create_dirty_data(local_csv_path)

    # Create the bucket if it doesn't exist
    create_bucket(bucket_name, region)

    # Upload the local CSV file to S3
    upload_to_s3(local_csv_path, bucket_name, s3_file_key)

if __name__ == "__main__":
    main()
