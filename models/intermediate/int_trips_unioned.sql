{{ config(materialized='view') }}

with yellow_tripdata as (
    select
        vendor_id,
        rate_code_id,
        pu_location_id,
        do_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        cast(null as int64) as trip_type, -- yellow does not have this
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        cast(null as numeric) as ehail_fee, -- align with green
        improvement_surcharge,
        total_amount,
        payment_type,
        'yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
green_tripdata as (
    select
        vendor_id,
        rate_code_id,
        pu_location_id,
        do_location_id,
        pickup_datetime,
        dropoff_datetime,
        store_and_fwd_flag,
        passenger_count,
        trip_distance,
        trip_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        ehail_fee,
        improvement_surcharge,
        total_amount,
        payment_type,
        'green' as service_type
    from {{ ref('stg_green_tripdata') }}
)

select * from yellow_tripdata
union all
select * from green_tripdata