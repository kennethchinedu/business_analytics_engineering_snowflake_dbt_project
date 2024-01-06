WITH
 stg_order_review AS (
    SELECT 
    * 
    FROM {{ ref('src_order_review')}}
 ) 

SELECT 
    * 
FROM
    stg_order_review