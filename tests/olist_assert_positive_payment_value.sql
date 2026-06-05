select 
    order_id,
    payment_value
from {{ ref('olist_fct_orders') }}
where payment_value < 0