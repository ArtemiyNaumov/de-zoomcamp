winpty docker run -it \
    -e POSTGRES_USER=root \
    -e POSTGRES_PASSWORD=root \
    -e POSTGRES_DB=ny_taxi \
    -v //c/Users/60129676/Desktop/VSCodeProjects/de-zoomcamp/docker-test/ny_taxi_pg_data:/var/lib/postgresql/data \
    -p 5431:5432 \
    --network=pg-network \
    --name pg-database \
    postgres:13