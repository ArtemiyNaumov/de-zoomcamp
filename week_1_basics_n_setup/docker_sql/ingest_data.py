#!/usr/bin/env python
# coding: utf-8

import argparse
import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

def convert_columns_to_timestamp(table, df):
    if table == 'yellow_taxi_data':
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    elif table == 'green_taxi_data':
        df.lpep_dropoff_datetime = pd.to_datetime(df.lpep_dropoff_datetime)
        df.lpep_pickup_datetime = pd.to_datetime(df.lpep_pickup_datetime)
    return

def main(params):
    user = params.user
    password = params.pwd
    host = params.host
    port = params.port
    db_name = params.db_name
    table_name = params.table_name
    csv_url_base = params.csv_url_base
    # csv_gz_name = 'yellow_tripdata_2021-01.csv.gz'
    csv_names = {'yellow_taxi_data': 'yellow_tripdata_2021-01.csv',
                    'green_taxi_data': 'green_tripdata_2019-01.csv',
                    'zones': 'taxi_zone_lookup.csv'}

    #print('Start downloading csv data...')
    #os.system(f'wget {csv_url_base+csv_names[table_name]}')

    #print('Done! Now unpacking .gz file...')
    #os.system(f'gzip {csv_gz_name}')

    print('CSV data is successfully received! Start connecting to the database...')
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')
    engine.connect()

    print('Successfully connected! Start reading CSV data and inserting chunks into database...')
    csv_name = csv_names[table_name]
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    convert_columns_to_timestamp(table_name, df)

    t_start = time()
    df.head(0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')
    t_end = time()
    print(f'Successfully inserted chunk! It just took {round(t_end - t_start, 4)} seconds')

    while True:
        try:
            t_start = time()
            df = next(df_iter)
            convert_columns_to_timestamp(table_name, df)
            df.to_sql(name=table_name, con=engine, if_exists='append')
            t_end = time()
            print(f'Successfully inserted chunk! It just took {round(t_end - t_start, 4)} seconds')
        except Exception as e:
            print('Done! All data from CSV file has been inserted into Postgres database')
            break

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest data to Postgres')
    parser.add_argument('--user', help='username for postgres')
    parser.add_argument('--pwd', help='password for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db_name', help='db name for postgres')
    parser.add_argument('--table_name', help='table name for postgres where the data will be stored')
    parser.add_argument('--csv_url_base', help='url of the csv file')

    args = parser.parse_args()

    main(args)
