CSV_URL_BASE="http://{LOCAL_MACHINE_IP}:8000/data/"

# winpty python ingest_data.py \
docker run -it \
    --network=pg-network \
    taxi_ingest:v001 \
    --user=root \
    --pwd=root \
    --host=pgdatabase \
    --port=5432 \
    --db_name=ny_taxi \
    --table_name=green_taxi_data \
    --csv_url_base=${CSV_URL_BASE}