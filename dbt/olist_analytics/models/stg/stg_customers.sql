WITH stg_customers AS (
    SELECT * FROM {{ ref('src_customers')}}
)

select * from stg_customers