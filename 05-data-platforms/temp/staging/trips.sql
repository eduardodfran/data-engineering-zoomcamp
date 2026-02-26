/* @bruin

# Staging asset for NY taxi trips.
# - Cleans types, filters by run window, and deduplicates.
name: staging.trips
# Platform: DuckDB SQL
type: duckdb.sql

depends:
  - ingestion.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: Event pickup timestamp (used for incremental updates)
    primary_key: true
    nullable: false
    checks:
      - name: not_null
  - name: pu_location_id
    type: integer
    description: Pickup location id
  - name: do_location_id
    type: integer
    description: Dropoff location id
  - name: total_amount
    type: double
    description: Total fare amount charged

custom_checks:
  - name: non_negative_total_amount
    description: Total amount should be non-negative for valid trips
    query: |
      SELECT COUNT(*) FROM staging.trips WHERE total_amount < 0
    value: 0

@bruin */

-- TODO: Write the staging SELECT query.
--
-- Purpose of staging:
-- - Clean and normalize schema from ingestion
-- - Deduplicate records (important if ingestion uses append strategy)
-- - Enrich with lookup tables (JOINs)
-- - Filter invalid rows (null PKs, negative values, etc.)
--
-- Why filter by {{ start_datetime }} / {{ end_datetime }}?
-- When using `time_interval` strategy, Bruin:
--   1. DELETES rows where `incremental_key` falls within the run's time window
--   2. INSERTS the result of your query
-- Therefore, your query MUST filter to the same time window so only that subset is inserted.
-- If you don't filter, you'll insert ALL data but only delete the window's data = duplicates.

WITH raw AS (
  SELECT *
  FROM ingestion.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
), clean AS (
  SELECT
    CAST(vendor_id AS INTEGER) AS vendor_id,
    CAST(pickup_datetime AS TIMESTAMP) AS pickup_datetime,
    CAST(dropoff_datetime AS TIMESTAMP) AS dropoff_datetime,
    CAST(passenger_count AS INTEGER) AS passenger_count,
    CAST(trip_distance AS DOUBLE) AS trip_distance,
    CAST(ratecode_id AS INTEGER) AS ratecode_id,
    store_and_fwd_flag,
    CAST(pu_location_id AS INTEGER) AS pu_location_id,
    CAST(do_location_id AS INTEGER) AS do_location_id,
    CAST(payment_type AS INTEGER) AS payment_type,
    CAST(fare_amount AS DOUBLE) AS fare_amount,
    CAST(extra AS DOUBLE) AS extra,
    CAST(mta_tax AS DOUBLE) AS mta_tax,
    CAST(tip_amount AS DOUBLE) AS tip_amount,
    CAST(tolls_amount AS DOUBLE) AS tolls_amount,
    CAST(improvement_surcharge AS DOUBLE) AS improvement_surcharge,
    CAST(total_amount AS DOUBLE) AS total_amount,
    CAST(congestion_surcharge AS DOUBLE) AS congestion_surcharge,
    extracted_at,
    _source_url,
    taxi_type
  FROM raw
  WHERE pickup_datetime IS NOT NULL
), dedup AS (
  SELECT *, ROW_NUMBER() OVER (
    PARTITION BY vendor_id, pickup_datetime, pu_location_id, do_location_id, total_amount
    ORDER BY extracted_at DESC
  ) AS rn
  FROM clean
)
SELECT
  vendor_id,
  pickup_datetime,
  dropoff_datetime,
  passenger_count,
  trip_distance,
  ratecode_id,
  store_and_fwd_flag,
  pu_location_id,
  do_location_id,
  payment_type,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  improvement_surcharge,
  total_amount,
  congestion_surcharge,
  extracted_at,
  _source_url,
  taxi_type
FROM dedup
WHERE rn = 1
