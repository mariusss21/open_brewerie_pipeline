import logging

import pandas as pd
from airflow.sdk import Variable

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def _get_silver_data_df() -> pd.DataFrame:
    """
    Reads the partitioned Parquet data from the Silver layer and returns a DataFrame.
    This helper function is used by both aggregation tasks.
    """
    logging.info("--- Reading data from Silver Layer ---")
    
    # Get S3 configuration
    try:
        s3_endpoint_url = Variable.get("minio_endpoint_url")
        s3_access_key = Variable.get("minio_access_key")
        s3_secret_key = Variable.get("minio_secret_key")
        silver_bucket = "silver"
    except KeyError as e:
        logging.error(f"Could not find required Airflow Variable: {e}")
        raise

    storage_options = {
        "key": s3_access_key,
        "secret": s3_secret_key,
        "client_kwargs": {"endpoint_url": s3_endpoint_url}
    }

    silver_path = f"s3://{silver_bucket}/breweries/"
    logging.info(f"Reading partitioned Parquet data from {silver_path}")
    
    try:
        df = pd.read_parquet(silver_path, storage_options=storage_options)
        logging.info(f"Successfully read {len(df)} records from the Silver layer.")
        return df
    except Exception as e:
        logging.error(f"Failed to read data from Silver layer. Error: {e}")
        raise

def aggregate_breweries_by_location():
    """
    Creates an aggregated view of breweries per type and location and saves it to the Gold layer.
    """
    df = _get_silver_data_df()
    gold_bucket = "gold"
    storage_options = {
        "key": Variable.get("minio_access_key"),
        "secret": Variable.get("minio_secret_key"),
        "client_kwargs": {"endpoint_url": Variable.get("minio_endpoint_url")}
    }

    logging.info("Creating aggregation: Breweries per type and location.")
    breweries_by_location = df[['country', 'brewery_type', 'id']].groupby(['country', 'brewery_type']).count().reset_index()
    breweries_by_location = breweries_by_location.rename(columns={'id': 'count'})

    # Reinforce datatypes
    breweries_by_location['country'] = breweries_by_location['country'].astype(str)
    breweries_by_location['brewery_type'] = breweries_by_location['brewery_type'].astype(str)
    breweries_by_location['count'] = breweries_by_location['count'].astype(int)
    
    gold_path_location = f"s3://{gold_bucket}/breweries_by_type_and_location.parquet"
    logging.info(f"Writing location aggregation to {gold_path_location}")
    breweries_by_location.to_parquet(
        gold_path_location,
        index=False,
        storage_options=storage_options
    )
    logging.info("Location aggregation finished successfully.")


def find_breweries_with_duplicate_names():
    """
    Finds all breweries that share a name with at least one other brewery and saves the list to the Gold layer.
    """
    df = _get_silver_data_df()
    gold_bucket = "gold"
    storage_options = {
        "key": Variable.get("minio_access_key"),
        "secret": Variable.get("minio_secret_key"),
        "client_kwargs": {"endpoint_url": Variable.get("minio_endpoint_url")}
    }

    logging.info("Creating aggregation: Breweries with duplicate names.")

    list_brew_repeated = df[['name', 'id']].groupby('name').count().copy()
    list_brew_repeated = list_brew_repeated[list_brew_repeated['id'] > 1]
    list_brew_repeated = list_brew_repeated.index.tolist()

    breweries_with_duplicate_names = df[df['name'].isin(list_brew_repeated)].sort_values('name')

    gold_path_duplicates = f"s3://{gold_bucket}/breweries_with_duplicate_names.parquet"
    logging.info(f"Writing duplicate names aggregation to {gold_path_duplicates}")
    breweries_with_duplicate_names.to_parquet(
        gold_path_duplicates,
        index=False,
        storage_options=storage_options
    )
    logging.info("Duplicate names aggregation finished successfully.")

def create_map_visualization_data():
    """
    Creates a slimmed-down dataset from the Silver layer for map visualizations
    and saves it to the Gold layer.
    """
    logging.info("--- Starting Map Visualization Data Creation ---")
    df = _get_silver_data_df()
    gold_bucket = "gold"
    storage_options = {
        "key": Variable.get("minio_access_key"),
        "secret": Variable.get("minio_secret_key"),
        "client_kwargs": {"endpoint_url": Variable.get("minio_endpoint_url")}
    }

    if df.empty:
        logging.info("Input DataFrame is empty. Skipping map data creation.")
        return

    map_cols = ['id', 'name', 'latitude', 'longitude']
    if not all(col in df.columns for col in map_cols):
        raise ValueError("Silver data is missing one of the required columns for map visualization.")
        
    map_df = df[map_cols].copy()
    map_df.dropna(subset=['latitude', 'longitude'], inplace=True)

    output_path = f"s3://{gold_bucket}/breweries_locations_for_map.parquet"
    logging.info(f"Writing map data with {len(map_df)} records to {output_path}")
    
    map_df.to_parquet(output_path, index=False, storage_options=storage_options)
    logging.info("--- Map Visualization Data Creation Finished ---")