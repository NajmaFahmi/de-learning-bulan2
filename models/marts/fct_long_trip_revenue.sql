--- Materialization set to default folder = TABLE

--- Select data
select 
    DATE(pickup_datetime) as pickup_date,
    COUNT(*) as total_long_trips,
    SUM(fare_amount) as long_trip_revenue,
    AVG(trip_distance) as avg_distance
from {{ ref('stg_long_trips') }}
group by DATE(pickup_datetime)
order by pickup_date desc