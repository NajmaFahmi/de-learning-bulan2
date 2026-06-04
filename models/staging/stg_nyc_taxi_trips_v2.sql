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
    round(tip_amount / nullif(fare_amount, 0)*100, 2):: float as tip_percentage,
    loaded_at
from source
{{ filter_valid_trips(min_distance=1, min_fare=5) }}