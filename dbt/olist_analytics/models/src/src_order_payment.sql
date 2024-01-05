WITH src_order_payment as (
    select * from {{ source('olist', 'src_order_payment') }}
)


select * from src_order_payment