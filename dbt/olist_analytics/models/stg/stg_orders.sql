WITH
 stg_orders AS (
    SELECT 
    * 
    FROM {{ ref('src_orders')}}
 ) 



 SELECT 
    order_id, 
    customer_id, 
    order_status, 
    -- Extract order purchase timestamp data
    order_purchase_timestamp,
    CASE 
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(order_purchase_timestamp)) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_purchase_day,
    CASE EXTRACT(MONTH FROM order_purchase_timestamp)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
        ELSE 'Invalid Month'
    END AS  order_purchase_month,
    EXTRACT(DAY FROM order_purchase_timestamp) AS order_purchase_days,
    TO_CHAR(order_purchase_timestamp, 'YYYY') AS order_purchase_year,
    TO_CHAR(order_purchase_timestamp, 'HH24:MI') AS order_purchase_time, 

    -- Extract order approved timestamp data
    order_approved_at,
    CASE 
        WHEN UPPER(DAYNAME(order_approved_at)) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(order_approved_at)) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_approved_day,
    TO_CHAR(order_approved_at,'HH24:MI') AS order_approved_time,

    -- order estimated delivery data
    order_estimated_delivery_date,
    CASE 
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(order_estimated_delivery_date)) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_estimated_delivery_day,
    TO_CHAR(order_estimated_delivery_date, 'HH24:MI') AS order_estimated_delivery_time,

    --order delivered to customers data
    order_delivered_customer_date,
    CASE 
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(order_delivered_customer_date)) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_delivered_to_customer_day,
    CASE EXTRACT(MONTH FROM order_delivered_customer_date)
        WHEN 1 THEN 'January'
        WHEN 2 THEN 'February'
        WHEN 3 THEN 'March'
        WHEN 4 THEN 'April'
        WHEN 5 THEN 'May'
        WHEN 6 THEN 'June'
        WHEN 7 THEN 'July'
        WHEN 8 THEN 'August'
        WHEN 9 THEN 'September'
        WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'
        WHEN 12 THEN 'December'
        ELSE 'Invalid Month'
    END AS  order_delivered_to_customer_month,
    TO_CHAR(order_delivered_customer_date, 'HH24:MI') AS order_delivered_to_customer_time 
FROM 
    stg_orders

 

