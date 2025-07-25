# Open Brewery DB API Data Pipeline 🍻

This project involves creating a data pipeline that consumes data from the Open Brewery DB API, processes it through a Medallion architecture, and visualizes the results.

## Table of Contents
- [Architecture](#architecture)
- [Data Pipeline](#data-pipeline)
- [Getting Started](#getting-started)
- [Monitoring and Alerting](#monitoring-and-alerting)
- [Directory Structure](#directory-structure)

## Architecture

The solution is containerized using Docker and Docker Compose, orchestrating the following services:

- **Orchestration**: **Apache Airflow** is used to schedule, execute, and monitor the data pipeline DAGs.
- **Data Lake**: **MinIO** serves as an S3-compatible object storage for our data lake, which is structured according to the Medallion architecture.
- **Dashboarding**: A **Streamlit** application provides an interactive dashboard to visualize the final aggregated data.

### Medallion Architecture

The data is processed through three distinct layers in our MinIO data lake:

1.  **Bronze Layer**: Stores the raw, unaltered data fetched directly from the Open Brewery DB API in JSON format.
2.  **Silver Layer**: Contains cleaned, transformed, and enriched data stored in a columnar format (Parquet). The data is partitioned by `country`.
3.  **Gold Layer**: Holds aggregated data, ready for analytics and visualization. This layer provides business-level insights, such as the number of breweries per type and location.

## Data Pipeline

The data pipeline is managed by a series of Airflow DAGs, each responsible for a specific stage of the ETL process:

1.  `0_setup_environment.py`: A utility DAG that creates the necessary buckets (`bronze`, `silver`, `gold`) in MinIO.
2.  `1_extract_breweries_data_daily.py`: This DAG runs daily to fetch brewery data from the API and stores the raw JSON response in the Bronze layer.
3.  `2_silver_transformations.py`: Triggered after the extraction, this DAG processes the raw data from the Bronze layer. It performs cleaning, schema enforcement, and type casting before saving the data as partitioned Parquet files in the Silver layer.
4.  `3_gold_aggregation_dag.py`: The final DAG in the pipeline, which reads the transformed data from the Silver layer and creates aggregated tables in the Gold layer. For example, it calculates the total number of breweries by type and location.

## Getting Started

Follow these steps to set up and run the project locally.

### Prerequisites

- [Git](https://git-scm.com/)
- [Docker](https://www.docker.com/products/docker-desktop/)
- [Docker Compose](https://docs.docker.com/compose/install/)

### Installation & Setup

1.  **Clone the repository:**
    ```bash
    git clone https://github.com/mariusss21/open_brewerie_pipeline.git
    cd open_brewerie_pipeline
    ```

2.  **Create an `.env` file:**
    Airflow requires a specific user ID to run. Create a `.env` file in the root of the project with the following content:
    ```
    AIRFLOW_UID=50000
    ```

    Or run the following command:

    ```bash
    echo -e "AIRFLOW_UID=$(id -u)" > .env
    ```

3.  **Initialize the database:**
    ```bash
    docker compose up airflow-init
    ```

4. **Running the containers:**
    ```bash
    docker compose up -d
    ```

5.  **Access the services:**
    - **Airflow UI**: [http://localhost:8080](http://localhost:8080) (user: `airflow`, pass: `airflow`)
    - **MinIO Console**: [http://localhost:8081](http://localhost:8082) (user: `minioadmin`, pass: `minioadmin`)
    - **Streamlit Dashboard**: [http://localhost:8501](http://localhost:8501)

### Running the Pipeline

1.  Open the Airflow UI in your browser.
2.  Un-pause the DAG `0_setup_environment`
3.  Trigger the `0_setup_environment` DAG manually to create the MinIO buckets.
4.  Un-pause the DAGs in the following order:
    - `1_extract_breweries_data_daily`
    - `2_silver_transformations`
    - `3_gold_aggregation_dag`
5.  Trigger the `1_extract_breweries_data_daily` DAG manually to start the data pipeline.

## Monitoring and Alerting

A robust monitoring and alerting strategy is crucial for production pipelines. Here’s a proposed approach:

-   **Pipeline Failures**: Configure Airflow's built-in alerting system to send email notifications on task failures. For more advanced alerting, integrate with tools like Slack.
-   **Data Quality**: Implement data quality checks within the DAGs using a library like **Great Expectations**. These checks can validate the data at each stage of the pipeline (e.g., checking for nulls, verifying distributions) and can be configured to halt the pipeline and send an alert if data quality issues are detected.
-   **Infrastructure Monitoring**: Use tools like **Prometheus** and **Grafana** to monitor the health and performance of the Docker containers. This would involve setting up cAdvisor to export container metrics to Prometheus and creating a Grafana dashboard to visualize them.

## Directory Structure

```
.
├── config/              # Airflow configuration files
├── dags/                # Airflow DAGs
│   ├── 0_setup_environment.py
│   ├── 1_extract_breweries_data_daily.py
│   ├── 2_silver_transformations.py
│   └── 3_gold_aggregation_dag.py
├── logs/                # Airflow logs
├── plugins/             # Airflow plugins
├── src/                 # Source code for data transformations
│   ├── extract_data.py
│   ├── silver_transformations.py
│   └── gold_aggregation.py
├── streamlit/           # Streamlit application files
│   ├── app.py
│   └── requirements.txt
├── .env                 # Environment variables
├── docker-compose.yaml  # Docker Compose configuration
├── variables.json       # Airflow variables
└── readme.md            # This file