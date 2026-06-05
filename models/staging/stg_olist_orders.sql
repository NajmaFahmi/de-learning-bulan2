--- Define Source Data
with source as (
    select * from {{ source('public', 'olist_orders_table') }}
),


--- Select Data
filtered as (
    select *,
        (price + freight_value) as total_order_value
    from source 
    where order_status = 'delivered'
        and payment_value is not null
)

select * from filtered