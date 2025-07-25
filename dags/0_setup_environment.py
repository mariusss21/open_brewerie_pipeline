import logging
from datetime import datetime

import boto3
from airflow.decorators import dag, task
from airflow.models import Variable

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

@dag(
    dag_id="0_setup_environment",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    doc_md="""
    ### Environment Setup DAG

    This DAG prepares the S3 environment for the breweries data pipeline.
    It creates the bronze, silver, and gold buckets required by the medallion architecture.
    This DAG is designed to be run manually once before the main extraction pipeline.
    """,
    tags=["breweries", "setup"],
)
def setup_environment_dag():
    """
    DAG to create the necessary S3 buckets for the data pipeline.
    """

    @task
    def create_bucket(bucket_name: str):
        """Creates an S3 bucket if it does not already exist."""
        
        s3_endpoint_url = Variable.get("minio_endpoint_url")
        s3_access_key = Variable.get("minio_access_key")
        s3_secret_key = Variable.get("minio_secret_key")

        s3_client = boto3.client(
            "s3",
            endpoint_url=s3_endpoint_url,
            aws_access_key_id=s3_access_key,
            aws_secret_access_key=s3_secret_key,
        )

        try:
            logging.info(f"Checking if bucket '{bucket_name}' exists...")
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully.")
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            logging.info(f"Bucket '{bucket_name}' already exists. No action taken.")
        except Exception as e:
            logging.error(f"Failed to create bucket '{bucket_name}'. Error: {e}")
            raise

    # Create tasks for each bucket. They will run in parallel.
    create_bucket.expand(bucket_name=["bronze", "silver", "gold"])

# Instantiate the DAG
setup_environment_dag()