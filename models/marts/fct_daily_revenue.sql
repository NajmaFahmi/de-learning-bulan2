--- yg ini materializationnya set to default sesuai folder yaitu TABLE

--- Define source data
with trips as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
),

--- Select data
daily_revenue as (
    select 
        DATE(pickup_datetime) as pickup_date,
        COUNT(*) as total_trips,
        SUM(total_amount) as total_revenue,
        ROUND(AVG(trip_distance), 2) as avg_trip_distance,
        ROUND(AVG(tip_percentage), 2) as avg_tip_percentage
    from trips
    group by DATE(pickup_datetime)
    --tidak boleh taruh order by disini, karena tidak akan terbaca
)

select * from daily_revenue
order by pickup_date desc