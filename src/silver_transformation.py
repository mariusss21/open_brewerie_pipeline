import json
import logging

import boto3
import pandas as pd
from airflow.sdk import Variable

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_s3_client():
    """Fetches S3 credentials from Airflow and returns a boto3 client."""
    try:
        s3_endpoint_url = Variable.get("minio_endpoint_url")
        s3_access_key = Variable.get("minio_access_key")
        s3_secret_key = Variable.get("minio_secret_key")
    except KeyError as e:
        logging.error(f"Could not find required Airflow Variable for S3 connection: {e}")
        raise

    return boto3.client(
        "s3",
        endpoint_url=s3_endpoint_url,
        aws_access_key_id=s3_access_key,
        aws_secret_access_key=s3_secret_key,
    )

def run_silver_transformation():
    """
    Main function to transform Bronze JSON data to a Silver Parquet file.
    This process is now idempotent, deleting old data before writing new data.
    """
    logging.info("--- Starting Silver Layer Transformation ---")
    s3_client = get_s3_client()

    # Get bucket and prefix configuration from Airflow Variables
    bronze_bucket = Variable.get("bronze_bucket", "bronze")
    bronze_prefix = Variable.get("bronze_prefix", "breweries")
    silver_bucket = "silver"
    silver_prefix = "breweries" # The target directory in the silver bucket

    # 1. Read all JSON files from the Bronze layer
    logging.info(f"Listing objects in s3://{bronze_bucket}/{bronze_prefix}/")
    try:
        response = s3_client.list_objects_v2(Bucket=bronze_bucket, Prefix=bronze_prefix)
        if 'Contents' not in response:
            logging.warning("No files found in the Bronze layer. Skipping transformation.")
            return
        json_files = [obj['Key'] for obj in response['Contents'] if obj['Key'].endswith('.json') and 'run_checkpoint' not in obj['Key']]
        if not json_files:
            logging.warning("No data files found to process. Exiting.")
            return
    except Exception as e:
        logging.error(f"Failed to list objects in Bronze bucket. Error: {e}")
        raise

    # 2. Load and concatenate data into a pandas DataFrame
    all_breweries = []
    for file_key in json_files:
        obj = s3_client.get_object(Bucket=bronze_bucket, Key=file_key)
        data = json.loads(obj['Body'].read().decode('utf-8'))
        all_breweries.extend(data)

    df = pd.DataFrame(all_breweries)
    logging.info(f"Successfully loaded {len(df)} records into DataFrame.")

    # 3. Data Cleaning and Schema Enforcement
    logging.info("Applying schema and cleaning data...")
    df.dropna(subset=['id'], inplace=True)
    df['latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
    df['longitude'] = pd.to_numeric(df['longitude'], errors='coerce')

    string_columns = ['name', 'brewery_type', 'address_1', 'city', 'state_province', 'postal_code', 'country', 'phone', 'website_url', 'state', 'street']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].fillna('Unknown')

    # drop street column if it is equal to address_1
    if df['address_1'].equals(df['street']):
        df.drop('street', axis=1, inplace=True)

    # remove leading and trailing spaces from country column
    df['country'] = df['country'].str.rstrip().str.strip()

    # 4. Clean up existing data in the Silver layer to ensure idempotency
    logging.info(f"Preparing to write to Silver layer. Cleaning up existing data in s3://{silver_bucket}/{silver_prefix}/")
    paginator = s3_client.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=silver_bucket, Prefix=silver_prefix)
    objects_to_delete = []
    for page in pages:
        if "Contents" in page:
            for obj in page["Contents"]:
                objects_to_delete.append({"Key": obj["Key"]})
    
    if objects_to_delete:
        logging.info(f"Found {len(objects_to_delete)} objects to delete.")
        s3_client.delete_objects(Bucket=silver_bucket, Delete={"Objects": objects_to_delete})
        logging.info("Successfully deleted old data from Silver layer.")
    else:
        logging.info("Silver layer target directory is already empty. No cleanup needed.")

    # 5. Write to Silver Layer as Partitioned Parquet
    partition_col = 'country'
    output_path = f"s3://{silver_bucket}/{silver_prefix}/"
    logging.info(f"Writing DataFrame to Parquet at {output_path}, partitioned by '{partition_col}'")

    try:
        df.to_parquet(
            output_path,
            engine='pyarrow',
            compression='snappy',
            partition_cols=[partition_col],
            storage_options={
                "key": Variable.get("minio_access_key"),
                "secret": Variable.get("minio_secret_key"),
                "client_kwargs": {"endpoint_url": Variable.get("minio_endpoint_url")}
            }
        )
        logging.info("Successfully wrote partitioned Parquet file to the Silver layer.")
    except Exception as e:
        logging.error(f"Failed to write Parquet file to Silver bucket. Error: {e}")
        raise

    logging.info("--- Silver Layer Transformation Finished ---")
