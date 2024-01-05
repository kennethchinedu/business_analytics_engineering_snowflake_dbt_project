WITH src_order_review as (
    select * from {{ source('olist', 'src_order_review') }}
)


select * from src_order_review