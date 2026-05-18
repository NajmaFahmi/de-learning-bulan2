--- yg ini materializationnya set to default (VIEW) sesuai folder

--- Define source data
with source as (
    select * from {{ source('public', 'nyc_taxi_trips') }}
),

--- Select data
renamed as (
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

        -- derived column
        round(tip_amount / nullif(fare_amount, 0) * 100, 2)::float as tip_percentage,

        loaded_at
    from source 
    where passenger_count > 0
        and trip_distance > 0
        and fare_amount > 0
)

select * from renamed