WITH src_customers as (
    select * from {{ source('olist', 'src_customers') }}
)


select * from src_customers