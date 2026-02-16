select
    dispatching_base_num,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,
    cast(pulocationid as int) as pu_location_id,
    cast(dolocationid as int) as do_location_id,
    cast(sr_flag as int) as sr_flag,
    affiliated_base_number
from {{ source('staging', 'fhv_tripdata') }}