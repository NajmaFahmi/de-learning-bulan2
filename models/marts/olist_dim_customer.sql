--- Ambil Data Asli
--- Jadikan hanya segmen customer
select 
    customer_id,
    customer_city,
    customer_state
from {{ ref('stg_olist_orders') }}
group by customer_id, customer_city, customer_state
