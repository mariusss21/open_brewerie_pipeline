import logging
import sys
from datetime import datetime

from airflow.decorators import dag, task
from pendulum import duration

@dag(
    dag_id="3_gold_aggregation",
    start_date=datetime(2023, 1, 1),
    schedule=None,  # This DAG is triggered by the Silver DAG
    default_args={
        "retries": 3,
        "retry_delay": duration(seconds=10),
    },
    catchup=False,
    doc_md="""
    ### Gold Layer Aggregation (Parallel)

    This DAG reads from the Silver layer and runs two aggregation tasks in parallel
    to create the final analytics tables in the Gold layer.
    """,
    tags=["breweries", "aggregation", "gold"],
)
def gold_aggregation_dag():
    """
    Orchestrates the Silver-to-Gold data aggregation with parallel tasks.
    """

    sys.path.append('/opt/airflow/src')
    from gold_aggregation import   (aggregate_breweries_by_location, 
                                    find_breweries_with_duplicate_names,
                                    create_map_visualization_data)

    @task
    def aggregate_by_location_task():
        """
        Task to create the aggregation of breweries by type and location.
        """
        logging.info("Starting task: aggregate_by_location_task")
        aggregate_breweries_by_location()
        logging.info("Finished task: aggregate_by_location_task")

    @task
    def find_duplicates_task():
        """
        Task to find breweries that have duplicate names.
        """
        logging.info("Starting task: find_duplicates_task")
        find_breweries_with_duplicate_names()
        logging.info("Finished task: find_duplicates_task")

    @task
    def create_map_data_task():
        """
        Task to create the map visualization data.
        """
        logging.info("Starting task: create_map_data_task")
        create_map_visualization_data()
        logging.info("Finished task: create_map_data_task")

    # The two tasks are defined but not chained, so Airflow will run them in parallel.
    aggregate_by_location_task()
    find_duplicates_task()
    create_map_data_task()

# Instantiate the DAG
gold_aggregation_dag()
