WITH
 stg_geolocations AS (
    SELECT 
    * 
    FROM {{ ref('src_geolocation')}}
 ) 

SELECT 
   * 
FROM
   stg_geolocations