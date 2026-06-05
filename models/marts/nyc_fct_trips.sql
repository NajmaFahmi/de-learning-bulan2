with trips as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
),

dates as (
    select * from {{ ref('nyc_dim_date') }}
)


select 
    t.trip_id,
    d.date_id,
    t.pickup_datetime,
    t.dropoff_datetime,
    t.passenger_count,
    t.trip_distance,
    t.fare_amount,
    t.tip_amount,
    t.total_amount,
    t.trip_duration_min,
    t.tip_percentage
from trips t 
left join dates d on DATE(t.pickup_datetime) = d.full_date

