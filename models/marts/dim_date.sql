with trips as (
    select distinct
        DATE(pickup_datetime) as full_date
    from {{ ref('stg_nyc_taxi_trips') }}
)


select 
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_id,
    full_date,
    EXTRACT(dow from full_date)::int as day_of_week,
    EXTRACT(month from full_date)::int as month,
    EXTRACT(year from full_date)::int as year,
    CASE WHEN EXTRACT(dow from full_date) IN (0,6) THEN true ELSE false END as is_weekend
from trips 


