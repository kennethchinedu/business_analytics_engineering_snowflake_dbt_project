WITH src_sellers as (
    select * from {{ source('olist', 'src_sellers') }}
)


select * from src_sellers