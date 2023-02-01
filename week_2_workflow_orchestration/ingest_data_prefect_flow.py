#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
from prefect import flow, task
from prefect.tasks import task_input_hash
from datetime import timedelta
from prefect_sqlalchemy import SqlAlchemyConnector

def convert_columns_to_timestamp(df, table='yellow_taxi_data'):
    if table == 'yellow_taxi_data':
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    elif table == 'green_taxi_data':
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    return

@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def download_data(url: str) -> pd.DataFrame:
    if url.endswith('.csv.gz'):
        csv_name = 'yellow_tripdata_2021-01.csv.gz'
    else:
        csv_name = 'output.csv'

    print('Start downloading csv data...')
    os.system(f'wget {url} -O {csv_name}')

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    convert_columns_to_timestamp(df)
    print('CSV data is successfully received! Start connecting to the database...')
    return df

@task(log_prints=True)
def transform_data(df) -> pd.DataFrame:
    print(f'Pre: missing passengers count: {df["passenger_count"].isin([0]).sum()}')
    df = df[df['passenger_count'] != 0]
    print(f'Post: missing passengers count: {df["passenger_count"].isin([0]).sum()}')
    return df


@task(log_prints=True, retries=5)
def ingest_data(df, table_name) -> None:
    connection_block = SqlAlchemyConnector.load("postgres-connector")
    with connection_block.get_connection(begin=False) as engine:
        print('Successfully connected! Start reading CSV data and inserting chunks into database...')
        t_start = time()
        df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
        df.to_sql(name=table_name, con=engine, if_exists='append')
        t_end = time()
        print(f'Successfully inserted chunk! It just took {round(t_end - t_start, 4)} seconds')
        #while True:
        #    try:
        #        t_start = time()
        #        df = next(df_iter)
        #        convert_columns_to_timestamp(table_name, df)
        #        df.to_sql(name=table_name, con=engine, if_exists='append')
        #        t_end = time()
        #        print(f'Successfully inserted chunk! It just took {round(t_end - t_start, 4)} seconds')
        #    except Exception as e:
        #        print('Done! All data from CSV file has been inserted into Postgres database')
        #        break

@flow(name='Ingest Flow')
def main_flow(table_name: str) -> None:
    print('This pipeline ingests data into Postgres. Begin inserting...')
    csv_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz'

    raw_df = download_data(csv_url)
    df = transform_data(raw_df)
    ingest_data(df, table_name)

if __name__ == '__main__':
    main_flow(table_name='yellow_taxi_data')
