WITH
 ot AS (
    SELECT 
    * 
    FROM {{ ref('stg_order_payment')}}
 ),
 o AS (
    SELECT 
    * 
    FROM {{ ref('stg_orders')}}
), 
oi AS (
    SELECT 
    *
    FROM {{ ref('stg_order_items') }}
)


SELECT 
    ot.order_id,
    ot.payment_sequential,
    ot.payment_type,
    ot.payment_installments,
    ot.payment_value


