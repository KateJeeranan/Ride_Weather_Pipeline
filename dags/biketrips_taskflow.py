from airflow.models import DAG
from airflow.decorators import dag, task
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.utils.dates import days_ago
import pandas as pd
import requests
from datetime import timedelta
import logging
from airflow.models import Variable
import glob
import os

# Constants
MYSQL_CONNECTION = "mysql_default"  # MySQL connection ID defined in Airflow
WEATHER_API_KEY = os.environ.get("WEATHER_API_KEY")
GCS_BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = os.environ.get("BQ_DATASET")
BQ_TABLE = os.environ.get("BQ_TABLE")

WEATHER_DATA_URL = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/newyork/2018-05-01/2018-06-01?unitGroup=metric&include=days&key={WEATHER_API_KEY}&contentType=json"
mysql_output_path = "/home/airflow/gcs/data/trips_merged.parquet"  # Output path for merged trip data
weather_data_output_path = "/home/airflow/gcs/data/weather_data.parquet"  # Output path for weather data
final_output_path = "/home/airflow/gcs/data/final_output.parquet"  # Output path for final merged data

if not WEATHER_API_KEY or not GCS_BUCKET_NAME:
    raise ValueError("Environment variables WEATHER_API_KEY and GCS_BUCKET_NAME must be set")

# Default arguments for the DAG
default_args = {
    "owner": "kate",
    "retries": 5,  # Retry up to 5 times
    "retry_delay": timedelta(minutes=10),  # Wait 10 minutes between retries
}

@task()
def get_data_from_mysql(output_path):
    """
    Fetch data from MySQL, merge tables (trips, users, and stations), and save the result as a Parquet file.
    """
    try:
        mysqlserver = MySqlHook(MYSQL_CONNECTION)
        engine = mysqlserver.get_sqlalchemy_engine()

        # Load stations and users
        stations = pd.read_sql(sql="SELECT * FROM bike_trips.stations", con=engine)
        users = pd.read_sql(sql="SELECT * FROM bike_trips.users", con=engine)

        chunksize = 50000
        for i, trips_chunk in enumerate(pd.read_sql(sql="SELECT * FROM bike_trips.trips", con=engine, chunksize=chunksize)):
            merged_trips = trips_chunk.merge(users, on="user_id", how="left") \
                .merge(stations, left_on="start_station_id", right_on="station_id", how="left") \
                .rename(columns={"station_name": "start_station_name"}) \
                .drop(columns=["station_id"]) \
                .merge(stations, left_on="end_station_id", right_on="station_id", how="left") \
                .rename(columns={"station_name": "end_station_name"}) \
                .drop(columns=["station_id"])
            
            # Convert date columns
            merged_trips['start_date'] = pd.to_datetime(merged_trips['start_date'])
            merged_trips['stop_date'] = pd.to_datetime(merged_trips['stop_date'])

            # Save chunked Parquet file
            chunk_output_path = f"{output_path}_chunk_{i}.parquet"
            merged_trips.to_parquet(chunk_output_path, index=False, compression="snappy")

            del trips_chunk
            del merged_trips

        logging.info(f"Saved chunks to {output_path}")
    except Exception as e:
        logging.error(f"Error in get_data_from_mysql: {e}")
        raise


@task()
def get_weather_data(output_path):
    """
    Fetch weather data from the Visual Crossing API and save it as a Parquet file.
    """
    try:
        # Fetch weather data from the API
        r = requests.get(WEATHER_DATA_URL)
        result_weather = r.json()
        
        # Convert JSON response to DataFrame
        df = pd.DataFrame(result_weather['days'])
        df = df.drop(columns=['stations', 'source', 'icon'])  # Drop unnecessary columns

        # Convert datetime column to datetime format
        df['datetime'] = pd.to_datetime(df['datetime'])

        # Save weather data to Parquet file
        df.to_parquet(output_path, index=False)
        print(f"Output to {output_path}")
    except Exception as e:
        logging.error(f"Error in get_weather_data: {e}")
        raise


@task()
def merge_data(trips_path, weather_data_path, output_path):
    """
    Merge trip data and weather data based on the start_date and datetime columns.
    """
    try:
        # Read all Parquet chunk files dynamically
        trips_files = glob.glob(f"{trips_path}_chunk_*.parquet")
        if not trips_files:
            raise FileNotFoundError(f"No Parquet files found at {trips_path}_chunk_*.parquet")

        trips = pd.concat([pd.read_parquet(file) for file in trips_files], ignore_index=True)
        weather = pd.read_parquet(weather_data_path)

        # Merge with weather data
        final_df = trips.merge(weather, how="left", left_on="start_date", right_on="datetime")
        final_df = final_df.drop(["datetime", "datetimeEpoch"], axis=1)  

        # Save final merged data
        final_df.to_parquet(output_path, index=False, compression="snappy")
        logging.info(f"Final output saved to {output_path}")

    except Exception as e:
        logging.error(f"Error in merge_data: {e}")
        raise



@dag(default_args=default_args, schedule_interval="@once", start_date=days_ago(1), tags=["bike_trips"])
def bike_weather():
    """
    DAG to fetch, merge, and process bike trip and weather data.
    """
    # Task 1: Fetch and merge data from MySQL
    t1 = get_data_from_mysql(output_path=mysql_output_path)

    # Task 2: Fetch weather data from the API
    t2 = get_weather_data(output_path=weather_data_output_path)

    # Task 3: Merge trip data and weather data
    t3 = merge_data(
        trips_path=mysql_output_path,
        weather_data_path=weather_data_output_path,
        output_path=final_output_path
    )

    # Task 4: Load the final merged data into BigQuery
    t4 = GCSToBigQueryOperator(
        task_id='gcs_to_bigquery',
        bucket=GCS_BUCKET_NAME,
        source_objects=['data/final_output.parquet'],
        source_format="PARQUET",
        destination_project_dataset_table=f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}",
        autodetect=True,
        write_disposition='WRITE_TRUNCATE',
    )

    # Define task dependencies
    [t1, t2] >> t3 >> t4

# Instantiate the DAG
bike_weather()