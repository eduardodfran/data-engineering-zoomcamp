select 
    cast(vendorid as int) as vendor_id,
    cast(ratecodeid as int) as rate_code_id,
    cast(pulocationid as int) as pu_location_id,
    cast(dolocationid as int) as do_location_id,
    cast(lpep_pickup_datetime as timestamp) as pickup_datetime,
    cast(lpep_dropoff_datetime as timestamp) as dropoff_datetime,
    store_and_fwd_flag,
    cast(passenger_count as int) as passenger_count,
    cast(trip_distance as float64) as trip_distance,
    cast(trip_type as int) as trip_type,
    cast(fare_amount as numeric) as fare_amount,
    cast(extra as numeric) as extra,
    cast(mta_tax as numeric) as mta_tax,
    cast(tip_amount as numeric) as tip_amount,
    cast(tolls_amount as numeric) as tolls_amount,
    cast(ehail_fee as numeric) as ehail_fee,
    cast(improvement_surcharge as numeric) as improvement_surcharge,
    cast(total_amount as numeric) as total_amount,
    cast(payment_type as int) as payment_type
from {{ source('staging', 'green_taxi_partitioned') }}
where vendorid is not null