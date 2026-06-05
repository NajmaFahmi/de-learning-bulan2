{% snapshot nyc_trips_snapshot %}

{{
    config(
        target_schema='dbt_snapshots',
        unique_key='trip_id',
        strategy='timestamp',
        updated_at='loaded_at'
    )
}}

select * from {{ source('public', 'nyc_taxi_trips') }}

{% endsnapshot %}