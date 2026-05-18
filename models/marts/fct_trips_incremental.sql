--- Change materialization table to INCREMENTAL 
{{ config(
    materialized='incremental',
    unique_key='trip_id'
)
}}

--- Define source data
with source as (
    select * from {{ ref('stg_nyc_taxi_trips') }}
)

--- Select data
select 
    trip_id,
    pickup_datetime,
    trip_distance,
    fare_amount,
    tip_percentage,
    loaded_at
from source 


--- Only add data if its a new row / data (loaded at nya terbaru)
{% if is_incremental() %}
    where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}