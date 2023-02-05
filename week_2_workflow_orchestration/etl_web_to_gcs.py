#!/usr/bin/env python
# coding: utf-8

from pathlib import Path
import pandas as pd
from prefect import flow, task
from prefect_gcp.cloud_storage import GcsBucket

@task(log_prints=True, retries=3)
def fetch_data(url: str) -> pd.DataFrame:
    """Read taxi data from web into pandas dataframe"""

    df = pd.read_csv(url)

    return df

@task(log_prints=True)
def process_data(df: pd.DataFrame, color: str) -> pd.DataFrame:
    """Fix dtype issues"""
    if color == 'yellow':
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    elif color == 'green':
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    print(f'columns: {df.dtypes}')
    print(f'shape: {df.shape}')
    return df

@task(log_prints=True)
def write_local(df: pd.DataFrame, color: str, dataset_file: str) -> Path:
    """Write dataframe locally as parquet file"""
    path = Path(f'data/{color}/{dataset_file}.parquet')
    df.to_parquet(path, compression='gzip')
    return path

@task(log_prints=True)
def write_to_gcs(path: Path) -> None:
    """Upload data to Google Cloud Storage"""
    gcs_block = GcsBucket.load("zoomcamp-gcs")
    gcs_block.upload_from_path(
        from_path=path,
        to_path=path
    )
    return


@flow(name="Web to GCS")
def etl_web_to_gcs() -> None:
    """The main ETL func"""
    color = 'green'
    year = 2019
    month = 4
    dataset_file = f'{color}_tripdata_{year}-{month:02}'
    dataset_url = f'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/{color}/{dataset_file}.csv.gz'

    df = fetch_data(dataset_url)
    df_clean = process_data(df, color)
    path = write_local(df_clean, color, dataset_file)
    write_to_gcs(path)

if __name__ == '__main__':
    etl_web_to_gcs()