#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket
from prefect_gcp.cloud_storage import GcpCredentials

@task(log_prints=True)
def extract_from_gcs(color: str, year: int, month: int) -> Path:
    """Download trips data from GCS"""
    gcs_path = f'data/{color}/{color}_tripdata_{year}-{month:02}.parquet'
    gcs_block = GcsBucket.load('zoomcamp-gcs')
    gcs_block.get_directory(from_path=gcs_path, local_path=f'./')
    return Path(f'./{gcs_path}')

@task(log_prints=True)
def transform_data(path: Path) -> pd.DataFrame:
    """Cleaning data"""
    df = pd.read_parquet(path)
    print(f"Pre: missing passenger count: {df['passenger_count'].isna().sum()}")
    df['passenger_count'].fillna(0, inplace=True)
    print(f"Post: missing passenger count: {df['passenger_count'].isna().sum()}")
    return df

@task(log_prints=True)
def write_to_bq(df: pd.DataFrame) -> None:
    """Write DataFrame to Big Query"""
    gcp_credentials_block = GcpCredentials.load("zoomcamp-gcp-credentials")
    df.to_gbq(
        destination_table='trips_data_all.yellow_taxi_data',
        project_id='splendid-skill-375614',
        credentials=gcp_credentials_block.get_credentials_from_service_account(),
        chunksize=500_000,
        if_exists='append'
    )

@flow(name="Data from GCS to GBQ")
def etl_gcs_to_bq() -> None:
    """Main ETL flow for loading data form GCS bucket to Big Query"""
    color = 'yellow'
    year = 2021
    month = 1

    path = extract_from_gcs(color, year, month)
    df = transform_data(path)
    write_to_bq(df)

if __name__ == '__main__':
    etl_gcs_to_bq()