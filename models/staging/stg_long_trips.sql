--- ubah materialization menjadi EPHEMERAL
{{ config(materialized='ephemeral') }}


--- select data
select *
from {{ ref('stg_nyc_taxi_trips') }}
where trip_distance > 10