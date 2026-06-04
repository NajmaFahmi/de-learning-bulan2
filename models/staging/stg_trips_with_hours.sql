{{ config(materialized='view') }}

with source as (
    select * from {{ source('public', 'nyc_taxi_trips') }}
)

select 
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    passenger_count,
    trip_distance,
    fare_amount,
    tip_amount,
    total_amount,
    trip_duration_min,
    {{ calculate_duration_hours('trip_duration_min') }} as trip_duration_hour,
    round(tip_amount / nullif(fare_amount, 0)*100, 2):: float as tip_percentage,
    loaded_at
from source