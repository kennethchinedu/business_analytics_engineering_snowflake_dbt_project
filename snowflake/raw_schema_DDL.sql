-- SETTING UP TABLES TO COLLECT RAW DATA FROM AWS S3 BUCKET

-- Creating customer table 
create or replace table raw_customers (
    customer_id text unique primary key ,
    customer_unique_id text unique ,
    customer_zip_code_prefix int,
    customer_city text,
    customer_state text
);

--Creating geolocation table  
create or replace table raw_geolocation (
    geolocation_zip_code_prefix int,
    geolocation_lat int,
    geolocation_lng int,
    geolocation_city text,
    geolocation_state text 
);

--Creating order item table
create or replace table raw_order_item (
    order_id text,
    order_item int,
    product_id text,
    seller_id text,
    shipping_limit_date date,
    price int,
    freight_value int 
);

-- Creating order payment table
create or replace table order_payment(
    order_id text,
    payment_sequential int,
    payment_type text,
    payment_installments int,
    payment_value int 
);

-- Creating order review table
create or replace table order_review(
    review_id text,
    order_id text,
    review_score int,
    review_comment_title text,
    review_comment_message text,
    review_creation_date date,
    review_answer_timestamp date 
);
drop table RAW_orders;
-- Creating order table 
create or replace table raw_orders (
    order_id text ,
    customer_id text ,
    order_status text,
    order_purchase_timestamp text,
    order_approved_at text,
    order_delivered_carrier_date text,
    order_estimated_delivery_date text,
    order_delivered_customer_date text
);


-- creating product table
create or replace table raw_products (
    product_id text,
    product_category_name text,
    product_name_lenght int,
    product_description_lenght text,
    product_photos_qty int,
    product_weight_g int,
    product_length_cm int,
    product_height_cm int,
    product_width_cm int
);


--creating sellers table
create or replace table sellers (
    seller_id text unique primary key,
    seller_zip_code_prefix int,
    seller_city text,
    seller_state text 
);

-- creating product category table
create  or replace table product_category (
    product_category_name text,
    product_category_name_english text
);










