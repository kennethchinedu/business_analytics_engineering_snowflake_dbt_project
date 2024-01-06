WITH stg_product_cat AS (
    SELECT * FROM {{ ref('src_product_category')}}
)

select * from stg_product_cat