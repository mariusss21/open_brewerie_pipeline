import logging
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from pendulum import duration

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

@dag(
    dag_id="1_extract_breweries_data",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",  # This will run the DAG once a day after midnight
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=10),
    },
    catchup=False,
    doc_md="""
    ### Daily Breweries Data Extraction

    This DAG runs daily to extract new or updated data from the breweries API.
    It uses a checkpointing mechanism to ensure it can resume from where it left off.
    """,
    tags=["breweries", "extraction"],
)
def daily_extraction_dag():
    """
    This DAG orchestrates the daily extraction of breweries data.
    """
    import sys
    sys.path.append('/opt/airflow/src')
    from extract_data import extract_breweries_data

    @task
    def run_extraction_task():
        """
        A single task that calls the main extraction logic from the source script.
        """
        logging.info("Starting the breweries data extraction process...")
        extract_breweries_data()
        logging.info("Extraction process finished successfully.")

    # Task to trigger the Silver layer DAG
    trigger_silver_transformation = TriggerDagRunOperator(
        task_id="trigger_silver_transformation",
        trigger_dag_id="2_silver_transformation",
        wait_for_completion=False,
    )

    # Define the task dependency
    run_extraction_task() >> trigger_silver_transformation

# Instantiate the DAG
daily_extraction_dag()
