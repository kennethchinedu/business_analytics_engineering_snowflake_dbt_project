-- creating product streams
create stream product_stream on  table OLIST.RAW.RAW_PRODUCTS;
--


-- configuring streams and tasks on raw  product table 
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


-- creating geolocation  stream
create stream geolocation_stream on  table OLIST.RAW.RAW_GEOLOCATION;

select * from OLIST.RAW.RAW_GEOLOCATION;
--
drop stream OLIST.SOURCE.geolocation_stream;

merge into OLIST.SOURCE.SRC_GEOLOCATION g
using OLIST.SOURCE.geolocation_stream s 
    on g.GEOLOCATION_ZIP_CODE_PREFIX = s.GEOLOCATION_ZIP_CODE_PREFIX 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set g.geolocation_zip_code_prefix  = s.geolocation_zip_code_prefix ,
        g.geolocation_lat = s.geolocation_lat ,
        g.geolocation_lng = s.geolocation_lng ,
        g.geolocation_city = s.geolocation_city,
        g.geolocation_state  = s.geolocation_state,
        g.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(geolocation_zip_code_prefix,geolocation_lat,geolocation_lng,geolocation_city,geolocation_state,last_updated_date)
    values(s.geolocation_zip_code_prefix,s.geolocation_lat,s.geolocation_lng,s.geolocation_city,s.geolocation_state,current_date);


-- creating order_item  stream
create stream order_item_stream on  table OLIST.RAW.raw_order_item;


select * from OLIST.RAW.RAW_GEOLOCATION;
--
drop stream OLIST.SOURCE.geolocation_stream;

merge into OLIST.SOURCE.SRC_ORDER_ITEM ot
using  OLIST.SOURCE.ORDER_ITEM_STREAM s 
    on ot.order_id = s.order_id 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set ot.order_id  = s.order_id ,
        ot.order_item = s.order_item ,
        ot.product_id = s.product_id ,
        ot.seller_id = s.seller_id,
        ot.shipping_limit_date  = s.shipping_limit_date,
        ot.price = s.price,
        ot.freight_value = s.freight_value,
        ot.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(order_id,order_item,product_id,seller_id,shipping_limit_date,price,freight_value,last_updated_date)
    values(s.order_id,s.order_item,s.product_id,s.seller_id,s.shipping_limit_date,s.price,s.freight_value,current_date);

-- creating customers stream
create stream customers_stream on  table OLIST.RAW.RAW_CUSTOMERS;




-- creating order_item  stream
create stream order_item_stream on  table OLIST.RAW.raw_order_item;


--
drop stream OLIST.RAW.raw_order_item;

merge into OLIST.SOURCE.SRC_CUSTOMERS cs
using   OLIST.SOURCE.CUSTOMERS_STREAM s
    on cs.customer_id = s.customer_id 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set cs.customer_id  = s.customer_id ,
        cs.customer_unique_id = s.customer_unique_id,
        cs.customer_zip_code_prefix = s.customer_zip_code_prefix ,
        cs.customer_city = s.customer_city,
        cs.customer_state  = s.customer_state,
        cs.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'

    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state,last_updated_date) values(s.customer_id,s.customer_unique_id,s.customer_zip_code_prefix,s.customer_city,s.customer_state,current_date);




-- creating order_item  stream
create stream product_cat_stream on  table OLIST.RAW.PRODUCT_CATEGORY;


--
drop stream OLIST.RAW.raw_order_item;

merge into OLIST.SOURCE.PRODUCT_CATEGORY pc 
using   OLIST.SOURCE.PRODUCT_CAT_STREAM s
    on pc.product_category_name = s.product_category_name 
when matched 
    and s.METADATA$ACTION = 'DELETE'
    and s.METADATA$ISUPDATE = 'FALSE'
then delete 
when matched 
    and s.METADATA$ACTION = 'INSERT'
    and s.METADATA$ISUPDATE = 'TRUE'
    then update 
    set pc.product_category_name  = s.product_category_name ,
        pc.product_category_name_english = s.product_category_name_english,
        pc.last_updated_date = current_date
when not matched 
    and s.METADATA$ACTION = 'INSERT'

    and s.METADATA$ISUPDATE = 'FALSE'
    then insert(product_category_name,product_category_name_english,last_updated_date) values(s.product_category_name,s.product_category_name_english,current_date);
    
