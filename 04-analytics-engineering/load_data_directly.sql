-- Load all yellow taxi data (2019-2020) into partitioned table
-- Run this in BigQuery console

-- Yellow taxi 2019
INSERT INTO `zoomcamp-data-engineer-484608.zoomcamp.yellow_taxi_partitioned`
SELECT 
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  CAST(store_and_fwd_flag AS STRING) as store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
FROM `zoomcamp-data-engineer-484608.zoomcamp.yellow_taxi_external`
WHERE tpep_pickup_datetime BETWEEN '2019-01-01' AND '2019-12-31';

-- Yellow taxi 2020
INSERT INTO `zoomcamp-data-engineer-484608.zoomcamp.yellow_taxi_partitioned`
SELECT 
  VendorID,
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  RatecodeID,
  CAST(store_and_fwd_flag AS STRING) as store_and_fwd_flag,
  PULocationID,
  DOLocationID,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge
FROM `zoomcamp-data-engineer-484608.zoomcamp.yellow_taxi_external`
WHERE tpep_pickup_datetime BETWEEN '2020-01-01' AND '2020-12-31';

-- Green taxi 2019
INSERT INTO `zoomcamp-data-engineer-484608.zoomcamp.green_taxi_partitioned`
SELECT 
  VendorID,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  CAST(store_and_fwd_flag AS STRING) as store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  CAST(ehail_fee AS FLOAT64) as ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM `zoomcamp-data-engineer-484608.zoomcamp.green_taxi_external`
WHERE lpep_pickup_datetime BETWEEN '2019-01-01' AND '2019-12-31';

-- Green taxi 2020
INSERT INTO `zoomcamp-data-engineer-484608.zoomcamp.green_taxi_partitioned`
SELECT 
  VendorID,
  lpep_pickup_datetime,
  lpep_dropoff_datetime,
  CAST(store_and_fwd_flag AS STRING) as store_and_fwd_flag,
  RatecodeID,
  PULocationID,
  DOLocationID,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  CAST(ehail_fee AS FLOAT64) as ehail_fee,
  improvement_surcharge,
  total_amount,
  payment_type,
  trip_type,
  congestion_surcharge
FROM `zoomcamp-data-engineer-484608.zoomcamp.green_taxi_external`
WHERE lpep_pickup_datetime BETWEEN '2020-01-01' AND '2020-12-31';
