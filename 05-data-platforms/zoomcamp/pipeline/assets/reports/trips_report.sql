/* @bruin

# Trips report asset: daily aggregation with taxi type and payment method.
name: reports.trips_report
type: duckdb.sql
depends:
  - staging.trips
materialization:
  type: table
columns:
  - name: trip_date
    type: date
    description: Date of the trip (based on pickup_datetime)
    primary_key: true
  - name: taxi_type
    type: varchar
    description: Type of taxi (yellow, green)
    primary_key: true
  - name: payment_type_name
    type: varchar
    description: Payment method name
    primary_key: true
  - name: trip_count
    type: bigint
    description: Total number of trips
    checks:
      - name: positive
  - name: total_passengers
    type: bigint
    description: Total number of passengers
    checks:
      - name: non_negative
  - name: total_distance
    type: double
    description: Total trip distance in miles
    checks:
      - name: non_negative
  - name: total_fare
    type: double
    description: Total fare amount
    checks:
      - name: non_negative
  - name: total_tips
    type: double
    description: Total tip amount
    checks:
      - name: non_negative
  - name: total_revenue
    type: double
    description: Total revenue (fare + tips)
    checks:
      - name: non_negative
  - name: avg_fare
    type: double
    description: Average fare per trip
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: double
    description: Average trip distance
    checks:
      - name: non_negative
  - name: avg_passengers
    type: double
    description: Average passengers per trip
    checks:
      - name: non_negative

@bruin */

-- Purpose of reports:
-- - Aggregate staging data for dashboards and analytics
-- Required Bruin concepts:
-- - Filter using `{{ start_datetime }}` / `{{ end_datetime }}` for incremental runs
-- - GROUP BY your dimension + date columns

SELECT
  trip_date,
  taxi_type,
  payment_type_name_fixed AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(passenger_count) AS total_passengers,
  SUM(trip_distance) AS total_distance,
  SUM(fare_amount) AS total_fare,
  SUM(tip_amount) AS total_tips,
  SUM(fare_amount + tip_amount) AS total_revenue,
  AVG(fare_amount) AS avg_fare,
  AVG(trip_distance) AS avg_trip_distance,
  AVG(passenger_count) AS avg_passengers
FROM (
  SELECT
    CAST(DATE(pickup_datetime) AS DATE) AS trip_date,
    taxi_type,
    COALESCE(payment_type, 'unknown') AS payment_type_name_fixed,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount
  FROM staging.trips
  WHERE pickup_datetime >= '{{ start_datetime }}'
    AND pickup_datetime < '{{ end_datetime }}'
)
GROUP BY trip_date, taxi_type, payment_type_name_fixed
