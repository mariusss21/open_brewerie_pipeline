import json
import logging
import time
from typing import Any, Dict
from datetime import datetime

import boto3
import requests
from airflow.sdk import Variable
from tenacity import retry, stop_after_attempt, wait_exponential

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=2, max=10),
    reraise=True,
)
def fetch_page(page_number: int, api_base_url: str, per_page: int) -> list:
    """Fetches a single page of brewery data."""
    logging.info(f"Fetching page {page_number}...")
    params = {"page": page_number, "per_page": per_page}
    try:
        response = requests.get(api_base_url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        logging.warning(f"Request failed for page {page_number}: {e}. Retrying...")
        raise


def get_run_checkpoint(s3_client, bucket: str, checkpoint_key: str) -> Dict[str, Any]:
    """Reads the checkpoint file from S3."""
    try:
        response = s3_client.get_object(Bucket=bucket, Key=checkpoint_key)
        return json.loads(response["Body"].read().decode("utf-8"))
    except s3_client.exceptions.NoSuchKey:
        logging.info("Checkpoint file not found. Starting a new run.")
        return {"last_page_processed": 0, "total_records_extracted": 0, "status": "starting", "page_details": {}}
    except (json.JSONDecodeError, IOError) as e:
        logging.warning(f"Could not parse checkpoint, starting new run. Error: {e}")
        return {"last_page_processed": 0, "total_records_extracted": 0, "status": "starting_after_error", "page_details": {}}


def save_run_checkpoint(s3_client, checkpoint: Dict[str, Any], bucket: str, checkpoint_key: str):
    """Saves the current state to the checkpoint file in S3."""
    logging.info(f"Saving checkpoint. Total records so far: {checkpoint['total_records_extracted']}.")
    s3_client.put_object(
        Bucket=bucket, Key=checkpoint_key, Body=json.dumps(checkpoint, indent=2)
    )


def save_to_bronze(s3_client, data: list, page_number: int, bucket: str, bronze_prefix: str):
    """Saves a page of data to the bronze layer."""
    output_key = f"{bronze_prefix}/page_{page_number}.json"
    logging.info(f"Saving {len(data)} records to s3://{bucket}/{output_key}")
    s3_client.put_object(
        Bucket=bucket, Key=output_key, Body=json.dumps(data, indent=2)
    )


def get_api_metadata(api_meta_url: str) -> dict:
    """Fetches the API metadata to get the total number of records."""
    logging.info("Fetching API metadata for validation...")
    try:
        response = requests.get(api_meta_url, timeout=30)
        response.raise_for_status()
        meta = response.json()
        logging.info(f"Successfully fetched metadata: {meta}")
        return meta
    except requests.exceptions.RequestException as e:
        logging.error(f"Could not fetch API metadata. Validation skipped. Error: {e}")
        raise
    

# --- Main Extraction and Validation Logic ---
def extract_breweries_data():
    """
    Main extraction function. It now includes a final validation step.
    """
    # 1. Get all configuration from Airflow Variables
    logging.info("Fetching configuration from Airflow Variables...")
    api_base_url = Variable.get("api_base_url")
    api_meta_url = "https://api.openbrewerydb.org/v1/breweries/meta" # Metadata URL
    per_page = int(Variable.get("per_page"))
    s3_endpoint_url = Variable.get("minio_endpoint_url")
    s3_access_key = Variable.get("minio_access_key")
    s3_secret_key = Variable.get("minio_secret_key")
    bronze_bucket = Variable.get("bronze_bucket")
    bronze_prefix = Variable.get("bronze_prefix")
    checkpoint_key = f"{bronze_prefix}/.run_checkpoint.json"

    # 2. Create the S3 client
    s3_client = boto3.client(
        "s3", endpoint_url=s3_endpoint_url, aws_access_key_id=s3_access_key, aws_secret_access_key=s3_secret_key
    )

    # 3. Run the extraction loop
    checkpoint = get_run_checkpoint(s3_client, bronze_bucket, checkpoint_key)
    page = checkpoint["last_page_processed"] + 1

    while True:
        try:
            data = fetch_page(page, api_base_url, per_page)
            if not data:
                logging.info("No more data found. Finalizing extraction.")
                checkpoint["status"] = "completed"
                save_run_checkpoint(s3_client, checkpoint, bronze_bucket, checkpoint_key)
                break
            
            save_to_bronze(s3_client, data, page, bronze_bucket, bronze_prefix)
            
            checkpoint["last_page_processed"] = page
            checkpoint["total_records_extracted"] += len(data)
            checkpoint["page_details"][str(page)] = len(data)
            checkpoint["status"] = "running"
            save_run_checkpoint(s3_client, checkpoint, bronze_bucket, checkpoint_key)
            
            page += 1
            time.sleep(0.2)

        except Exception as e:
            logging.error(f"A critical error occurred on page {page}: {e}")
            checkpoint["status"] = "failed"
            checkpoint["error_message"] = str(e)
            save_run_checkpoint(s3_client, checkpoint, bronze_bucket, checkpoint_key)
            raise

    # 4. Perform Validation
    logging.info("--- Starting Data Validation Step ---")
    metadata = get_api_metadata(api_meta_url)
    expected_total = int(metadata.get("total", 0))
    extracted_total = checkpoint["total_records_extracted"]

    logging.info(f"API metadata reports {expected_total} total records.")
    logging.info(f"Extraction process collected {extracted_total} total records.")

    if expected_total == extracted_total:
        logging.info("✅ Validation successful! Extracted record count matches the source total. Checkpoint will be reseted.")

        logging.info("Saving previous checkpoint to bronze layer...")
        backup_checkpoint_key = f"{bronze_prefix}/run_checkpoint_backup_{datetime.now().strftime('%Y%m%d%H%M%S')}.json"
        save_run_checkpoint(s3_client, checkpoint, bronze_bucket, backup_checkpoint_key)
        
        logging.info("Resetting checkpoint...")
        checkpoint["last_page_processed"] = 0
        checkpoint["total_records_extracted"] = 0
        checkpoint["status"] = "starting"
        checkpoint["page_details"] = {}
        save_run_checkpoint(s3_client, checkpoint, bronze_bucket, checkpoint_key)
    else:
        error_message = (
            f"❌ Validation Failed! Mismatch in record counts. "
            f"Expected: {expected_total}, Extracted: {extracted_total}."
        )
        logging.error(error_message)
        raise ValueError(error_message)

