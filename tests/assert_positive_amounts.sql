select 
    trip_id,
    fare_amount,
    tip_amount,
    total_amount
from {{ ref('stg_nyc_taxi_trips') }}
where fare_amount < 0
    or tip_amount < 0
    or total_amount < 0