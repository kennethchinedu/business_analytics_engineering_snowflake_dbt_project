WITH
 g AS (
    SELECT 
        * 
    FROM {{ ref('stg_geolocations')}}
 ),

 c AS (
    SELECT 
        * 
    FROM {{ ref ('stg_customers')}}

 ) 

SELECT 
    g.*,
    c.customer_id,
    c.customer_city


FROM 
    g
LEFT JOIN c ON  g.geolocation_state = c.customer_state