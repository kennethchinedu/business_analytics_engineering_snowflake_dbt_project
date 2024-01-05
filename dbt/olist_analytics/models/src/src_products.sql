WITH src_products as (
    select * from {{ source('olist', 'src_products') }}
)


select * from src_products