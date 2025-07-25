import logging
import sys
from datetime import datetime
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.decorators import dag, task
from pendulum import duration

@dag(
    dag_id="2_silver_transformation",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # This DAG is triggered by the extraction DAG, not on a schedule
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=10),
    },
    catchup=False,
    doc_md="""
    ### Silver Layer Transformation

    This DAG transforms the raw JSON data from the Bronze layer into a cleaned,
    structured, and partitioned Parquet file in the Silver layer.
    """,
    tags=["breweries", "transformation", "silver"],
)
def silver_transformation_dag():
    """
    Orchestrates the Bronze-to-Silver data transformation.
    """
    sys.path.append('/opt/airflow/src')
    from silver_transformation import run_silver_transformation

    @task
    def run_transformation_task():
        """
        A single task that calls the main silver transformation logic.
        """
        logging.info("Starting the Silver layer transformation process...")
        run_silver_transformation()
        logging.info("Silver layer transformation finished successfully.")

    # Task to trigger the Silver layer DAG
    trigger_gold_aggregation = TriggerDagRunOperator(
        task_id="trigger_gold_aggregation",
        trigger_dag_id="3_gold_aggregation",
        wait_for_completion=False,
    )

    run_transformation_task() >> trigger_gold_aggregation

# Instantiate the DAG
silver_transformation_dag()
