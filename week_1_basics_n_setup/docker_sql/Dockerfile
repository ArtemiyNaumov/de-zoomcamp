FROM python:3.11

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2

WORKDIR /app
COPY ingest_data.py ingest_data.py
COPY ./data/yellow_tripdata_2021-01.csv yellow_tripdata_2021-01.csv
COPY ./data/green_tripdata_2019-01.csv green_tripdata_2019-01.csv
COPY ./data/taxi_zone_lookup.csv taxi_zone_lookup.csv


ENTRYPOINT [ "python", "ingest_data.py" ]