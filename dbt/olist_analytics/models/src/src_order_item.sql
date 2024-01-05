WITH src_order_item as (
    select * from {{ source('olist', 'src_order_item') }}
)


select * from src_order_item