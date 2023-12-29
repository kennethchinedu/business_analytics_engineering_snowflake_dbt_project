-- creating product streams
create stream product_stream on  table OLIST.RAW.RAW_PRODUCTS;
--


-- configuring streams and tasks on raw table 
-- these streams loads data from raw table into the source table for dbt consumption 
merge into OLIST.SOURCE.SRC_PRODUCTS p 
using OLIST.SOURCE.PRODUCT_STREAM s 
    on p.product_id = s.product_id 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set p.product_id = s.product_id,
        p.product_category_name = s.product_category_name,
        p.product_name_lenght = s.product_name_lenght,
        p.product_description_lenght = s.product_description_lenght,
        p.product_photos_qty = s.product_photos_qty,
        p.product_weight_g = s.product_weight_g,
        p.product_length_cm = s.product_length_cm,
        p.product_height_cm = s.product_height_cm,
        p.product_width_cm = s.product_width_cm,
        p.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(product_id,product_category_name,product_name_lenght,product_photos_qty,product_weight_g,product_length_cm,product_height_cm,product_width_cm,last_updated_date)
values(s.product_id,s.product_category_name,s.product_name_lenght,s.product_photos_qty,s.product_weight_g,s.product_length_cm,s.product_height_cm,s.product_width_cm,current_date);



-- creating order  streams 
create stream order_stream on  table OLIST.RAW.RAW_ORDERS;
--
drop stream OLIST.SOURCE.order_stream;

merge into OLIST.SOURCE.SRC_ORDERS o 
using OLIST.SOURCE.order_stream s 
    on o.order_id = s.order_id 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set o.customer_id = s.customer_id,
        o.order_status = s.order_status,
        o.order_purchase_timestamp = s.order_purchase_timestamp,
        o.order_approved_at = s.order_approved_at,
        o.order_delivered_carrier_date = s.order_delivered_carrier_date,
        o.order_estimated_delivery_date = s.order_estimated_delivery_date,
        o.order_delivered_customer_date = s.order_delivered_customer_date,
        o.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_estimated_delivery_date,order_delivered_customer_date,last_updated_date)
values(s.order_id,s.customer_id,s.order_status,s.order_purchase_timestamp,s.order_approved_at,s.order_delivered_carrier_date,s.order_estimated_delivery_date,s.order_delivered_customer_date,current_date);



-- creating sellers  stream
create stream sellers_stream on  table OLIST.RAW.SELLERS;

select * from OLIST.SOURCE.SRC_SELLERS;
--
drop stream OLIST.SOURCE.sellers_stream;

merge into OLIST.SOURCE.SRC_SELLERS sl
using OLIST.SOURCE.sellers_stream s 
    on sl.seller_id = s.seller_id 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set sl.seller_id = s.seller_id,
        sl.seller_zip_code_prefix = s.seller_zip_code_prefix,
        sl.seller_city = s.seller_city,
        sl.seller_state = s.seller_state,
        sl.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(seller_id,seller_zip_code_prefix,seller_city,seller_state,last_updated_date)
    values(s.seller_id,s.seller_zip_code_prefix,s.seller_city,s.seller_state,current_date);



select * from OLIST.SOURCE.SRC_sellers limit 50;
truncate table OLIST.SOURCE.SRC_SELLERS;

select * from OLIST.SOURCE.sellers_STREAM;





drop stream product_stream;