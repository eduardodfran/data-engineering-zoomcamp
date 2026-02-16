with trips_unioned as (
	select *
	from {{ ref('int_trips_unioned') }}
	where pu_location_id is not null
	  and do_location_id is not null
),

dim_vendors as (
	select * from {{ ref('dim_vendors') }}
),

dim_locations as (
	select * from {{ ref('dim_locations') }}
)

select
	trips_unioned.vendor_id,
	dim_vendors.vendor_name,
	trips_unioned.service_type,
	trips_unioned.rate_code_id,
	trips_unioned.pu_location_id,
	pickup_zone.zone as pickup_zone,
	trips_unioned.do_location_id,
	dropoff_zone.zone as dropoff_zone,
	trips_unioned.pickup_datetime,
	trips_unioned.dropoff_datetime,
	trips_unioned.store_and_fwd_flag,
	trips_unioned.passenger_count,
	trips_unioned.trip_distance,
	trips_unioned.trip_type,
	trips_unioned.fare_amount,
	trips_unioned.extra,
	trips_unioned.mta_tax,
	trips_unioned.tip_amount,
	trips_unioned.tolls_amount,
	trips_unioned.ehail_fee,
	trips_unioned.improvement_surcharge,
	trips_unioned.total_amount,
	trips_unioned.payment_type,
	timestamp_diff(trips_unioned.dropoff_datetime, trips_unioned.pickup_datetime, second) as trip_duration
from trips_unioned
inner join dim_vendors
	on trips_unioned.vendor_id = dim_vendors.vendor_id
inner join dim_locations as pickup_zone
	on trips_unioned.pu_location_id = pickup_zone.location_id
inner join dim_locations as dropoff_zone
	on trips_unioned.do_location_id = dropoff_zone.location_id