WITH
 stg_order_item AS (
    SELECT 
    * 
    FROM {{ ref('src_order_item')}}
 ) 


 SELECT 
    * 
FROM 
    stg_order_item