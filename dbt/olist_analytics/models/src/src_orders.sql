WITH src_orders as (
    select * from {{ source('olist', 'src_orders') }}
)


select * from src_orders