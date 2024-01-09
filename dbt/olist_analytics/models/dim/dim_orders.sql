WITH
 oi AS (
    SELECT 
    * 
    FROM {{ ref('stg_order_items')}}
 ),
 o AS (
    SELECT 
    * 
    FROM {{ ref('stg_orders')}}
), 
c AS (
    SELECT 
    * 
    FROM {{ ref('stg_customers')}}
),
p AS (
    SELECT 
    *
    FROM {{ ref('stg_products') }}
)

SELECT
    oi.order_id AS order_id, 
    oi.order_item As order_item,
    oi.product_id AS product_id, 
    oi.seller_id AS seller_id, 
    oi.price AS order_price,
    oi.freight_value AS freight_value,
    oi.shipping_limit_date AS shipping_limit_date,
    o.order_purchase_date,
    o.order_purchase_year,
    o.order_purchase_month,
    o.order_purchase_day,
    o.order_purchase_time,
    DATEDIFF('day', o.order_purchase_date, o.order_estimated_delivery_date) AS estimated_delivery_days,
    DATEDIFF('day', o.order_purchase_date, o.order_delivered_customer_date) AS days_to_deliver_order,
    CASE 
        WHEN o.order_approved_time IS NOT NULL THEN 'Order Approved'
        ELSE 'Not Approved'
    END AS order_approved_status,
    c.customer_id,
    c.customer_zip_code_prefix,
    c.customer_state,
    p.product_category


FROM o
LEFT JOIN oi ON oi.order_id = o.order_id
LEFT JOIN c ON o.customer_id = c.customer_unique_id
LEFT JOIN p on oi.product_id = p.product_id 



    
