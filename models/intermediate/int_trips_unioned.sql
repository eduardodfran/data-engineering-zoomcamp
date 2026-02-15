{{ config(materialized='view') }}

with yellow_tripdata as (
    select
        *,
        'yellow' as service_type
    from {{ ref('stg_yellow_tripdata') }}
),
green_tripdata as (
    select
        *,
        'green' as service_type
    from {{ ref('stg_green_tripdata') }}
)

select * from yellow_tripdata
union all
select * from green_tripdata