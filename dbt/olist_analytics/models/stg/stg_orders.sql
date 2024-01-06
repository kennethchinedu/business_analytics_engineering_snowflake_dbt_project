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
    TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI') AS order_purchase_date,
    CASE 
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'))) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_purchase_day,
    CASE EXTRACT(MONTH FROM TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH:MI'))
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
    EXTRACT(DAY FROM TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI')) AS order_purchase_days,
    
    EXTRACT(YEAR FROM TO_DATE(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI')) AS order_purchase_year,
    TO_CHAR(TO_TIME(order_purchase_timestamp, 'MM/DD/YYYY HH24:MI'), 'HH24:MI') AS order_purchase_time, 

    -- Extract order approved timestamp data
    TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI') AS order_aproved_date,
    CASE 
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(TO_DATE(order_approved_at, 'MM/DD/YYYY HH24:MI'))) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_approved_day,
    TO_CHAR(TO_TIME(order_approved_at, 'MM/DD/YYYY HH24:MI'), 'HH24:MI') AS order_approved_time,

    -- order estimated delivery data
    TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI') AS order_estimated_delivery_date,
    CASE 
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(TO_DATE(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'))) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_estimated_delivery_day,
    TO_CHAR(TO_TIME(order_estimated_delivery_date, 'MM/DD/YYYY HH24:MI'), 'HH24:MI') AS order_estimated_delivery_time,

    --order delivered to customers data
    TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI') AS order_delivered_customer_date,
    CASE 
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'SUN' THEN 'Sunday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'MON' THEN 'Monday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'TUE' THEN 'Tuesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'WED' THEN 'Wednesday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'THUR' THEN 'Thursday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'FRI' THEN 'Friday'
        WHEN UPPER(DAYNAME(TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'))) = 'SAT' THEN 'Saturday'
        ELSE 'Unknown'
    END AS order_delivered_to_customer_day,
    CASE EXTRACT(MONTH FROM TO_DATE(order_delivered_customer_date, 'MM/DD/YYYY HH:MI'))
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
    TO_CHAR(TO_TIME(order_delivered_customer_date, 'MM/DD/YYYY HH24:MI'), 'HH24:MI') AS order_delivered_to_customer_time 
FROM 
    stg_orders


