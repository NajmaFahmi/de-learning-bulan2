--- Ambil Data Asli
--- Gabung dengan dim tables melalui foreign key

with orders as (
    select * from {{ ref('stg_olist_orders') }}
),

customers as (
    select * from {{ ref('olist_dim_customer') }}
),

products as (
    select * from {{ ref('olist_dim_product') }}
),

dates as (
    select * from {{ ref('olist_dim_date') }}
)


select 
    o.order_id,
    o.customer_id,
    o.product_id,
    d.date_id,
    o.payment_type,
    o.payment_value,
    o.price,
    o.freight_value,
    o.total_order_value,
    o.review_score,
    o.is_late_delivery
from orders o
left join customers c on o.customer_id = c.customer_id
left join products p on o.product_id = p.product_id
left join dates d on DATE(o.order_purchase_timestamp) = d.full_date
