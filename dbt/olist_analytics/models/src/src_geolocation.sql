WITH src_geolocation as (
    select * from {{ source('olist', 'src_geolocation') }}
)


select * from src_geolocation