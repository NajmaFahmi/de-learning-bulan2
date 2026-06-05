--- Ambil Data Asli
--- Jadikan hanya segmen product
select 
    product_id,
    product_category_name_english
from {{ ref('stg_olist_orders') }}
group by product_id, product_category_name_english