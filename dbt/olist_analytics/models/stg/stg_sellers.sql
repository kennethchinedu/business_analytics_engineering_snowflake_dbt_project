WITH
 stg_sellers AS (
    SELECT 
    * 
    FROM {{ ref('src_sellers')}}
 ) 

 SELECT 
    * 
FROM    stg_sellers