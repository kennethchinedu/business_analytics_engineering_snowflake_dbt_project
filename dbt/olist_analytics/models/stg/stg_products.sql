WITH
 p AS (
    SELECT 
    * 
    FROM {{ ref('src_products')}}
 ) ,
 
pc AS (
    SELECT 
    * 
    FROM {{ ref('src_product_category')}}
 ) 


SELECT 
    p.product_id AS product_id,
    pc.product_category_name_english AS product_category,
    p.product_weight_g AS product_weight_g,
    p.product_length_cm AS product_length_cm,
    p.product_height_cm AS product_height_cm,
    p.product_width_cm AS product_width_cm
FROM p
JOIN pc ON pc.product_category_name = p.product_category_name

