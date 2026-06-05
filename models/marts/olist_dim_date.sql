--- Ambil Data Asli
--- Jadikan segmen date dengan transformasinya

with order_timestamp as (
    select distinct date(order_purchase_timestamp) as full_date
    from {{ ref('stg_olist_orders') }}
)

select 
    {{ dbt_utils.generate_surrogate_key(['full_date']) }} as date_id,
    full_date, 
    extract(month from full_date)::int as month,
    extract(year from full_date)::int as year,
    case when extract(dow from full_date) in (0,6) then true else false end as is_weekend
from order_timestamp