/*
* SETUP:
* Create an external table using the fhv 2019 data.
* Create a table in BQ using the fhv 2019 data (do not partition or cluster this table).
*/

-- Creating an external table
CREATE OR REPLACE EXTERNAL TABLE `splendid-skill-375614.trips_data_all.external_fhv_tripdata`
OPTIONS (
  format = 'CSV',
  uris = ['gs://de_zoomcamp_data_lake_splendid-skill-375614/data/fhv/fhv_tripdata_2019-*.csv.gz']
);

SELECT * FROM `splendid-skill-375614.trips_data_all.external_fhv_tripdata` limit 10;

-- Create a non partitioned table from external table
CREATE OR REPLACE TABLE `splendid-skill-375614.trips_data_all.fhv_tripdata_non_partitioned` AS
SELECT * FROM `splendid-skill-375614.trips_data_all.external_fhv_tripdata`;

-- Question 1
SELECT COUNT(1) FROM `splendid-skill-375614.trips_data_all.fhv_tripdata_non_partitioned`;

-- Answer: 43244696

-- Question 2
SELECT COUNT(DISTINCT Affiliated_base_number) FROM `splendid-skill-375614.trips_data_all.external_fhv_tripdata`;
--3165

SELECT COUNT(DISTINCT Affiliated_base_number) FROM `splendid-skill-375614.trips_data_all.fhv_tripdata_non_partitioned`;
--3165

-- Answer: 0 MB for the External Table and 317.94MB for the BQ Table

-- Question 3
SELECT COUNT(1) FROM `splendid-skill-375614.trips_data_all.fhv_tripdata_non_partitioned` 
WHERE PUlocationID IS NULL AND DOlocationID IS NULL;

-- Answer: 717748

-- Question 4
-- Answer: Partition by pickup_datetime Cluster on affiliated_base_number

-- Question 5
-- Creating a partition and cluster table
CREATE OR REPLACE TABLE `splendid-skill-375614.trips_data_all.fhv_tripdata_partitioned_clustered` 
PARTITION BY DATE(pickup_datetime)
CLUSTER BY affiliated_base_number AS
SELECT * FROM `splendid-skill-375614.trips_data_all.fhv_tripdata_non_partitioned`;

SELECT COUNT(DISTINCT affiliated_base_number) FROM `splendid-skill-375614.trips_data_all.fhv_tripdata_partitioned_clustered`
WHERE pickup_datetime BETWEEN '2019-03-01' AND '2019-03-31';

-- Answer: 647.87 MB for non-partitioned table and 23.06 MB for the partitioned table

-- Question 6
-- Answer: GCP Bucket

-- Question 7
-- Answer: False