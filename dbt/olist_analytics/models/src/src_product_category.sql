WITH product_category as (
    select * from {{ source('olist', 'product_category') }}
)


select * from product_category