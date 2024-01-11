WITH
 o AS (
    SELECT 
        * 
    FROM {{ ref('dim_orders')}}
 ), 

 d AS (
    SELECT 
        *
    FROM {{ ref('dim_delivery')}}
 )


SELECT 
    o.*,
    d.geolocation_zip_code_prefix,
    d.geolocation_lat,
    d.geolocation_lng,
    d.geolocation_city,
    d.geolocation_state, 
    d.customer_city

FROM 
    o 
LEFT JOIN d ON  d.geolocation_state = o.customer_state 
