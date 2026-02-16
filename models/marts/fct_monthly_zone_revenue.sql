with trips as (
    select *
    from {{ ref('fct_trips') }}
),

monthly as (
    select
        date_trunc(cast(pickup_datetime as date), month) as revenue_month,
        service_type,
        pu_location_id,
        pickup_zone,
        sum(total_amount) as revenue_monthly_total_amount,
        count(*) as total_monthly_trips
    from trips
    group by 1, 2, 3, 4
)

select *
from monthly