WITH
 oi AS (
    SELECT 
    * 
    FROM {{ ref('src_order_item')}}
 ),

p AS (
    SELECT 
    * 
    FROM {{ ref('src_products')}}
 )


 SELECT
   
   oi.order_id,
   oi.product_id,
   oi.shipping_limit_date,
   oi.price,
   oi.freight_value,
   p.product_category_name
FROM 
   p
LEFT JOIN oi ON p.product_id = oi.product_id 

