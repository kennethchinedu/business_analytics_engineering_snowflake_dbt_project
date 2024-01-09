WITH
 stg_order_payment AS (
    SELECT 
    * 
    FROM {{ ref('src_order_payment')}}
 ) 

SELECT 
    * 
FROM 
    stg_order_payment