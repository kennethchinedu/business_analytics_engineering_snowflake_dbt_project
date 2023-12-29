t// Creating file format for csv files
CREATE OR REPLACE file format csv_format
    type = csv 
    field_delimiter = ','
    skip_header = 1
    empty_field_as_null = TRUE
    ERROR_ON_COLUMN_COUNT_MISMATCH=FALSE;

// Creating file format for csv files
CREATE OR REPLACE file format pipe_csv_format
    type = csv 
    field_delimiter = '|'
    skip_header = 1
    empty_field_as_null = TRUE;


create or replace storage integration s3_int
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE 
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::187770715104:role/aws_s3_snowflake'
    STORAGE_ALLOWED_LOCATIONS = (
    's3://olist-ecommerce-data/sellers/',
    's3://olist-ecommerce-data/products/',
    's3://olist-ecommerce-data/product_category/',
    's3://olist-ecommerce-data/orders/', 
    's3://olist-ecommerce-data/order_reviews/', 
    's3://olist-ecommerce-data/order_payment/', 
    's3://olist-ecommerce-data/customers/', 
    's3://olist-ecommerce-data/geolocation/', 
    's3://olist-ecommerce-data/order_items/');


DESC integration s3_int;

    
// Creating  stage to hold data
CREATE OR REPLACE STAGE sellers_stage
    URL = 's3://olist-ecommerce-data/sellers/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

--customers stage 
CREATE OR REPLACE STAGE customers_stage
    URL = 's3://olist-ecommerce-data/customers/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

--geolocation stage 
CREATE OR REPLACE STAGE geolocation_stage
    URL = 's3://olist-ecommerce-data/geolocation/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

    
--geolocation stage 
CREATE OR REPLACE STAGE geolocation_stage
    URL = 's3://olist-ecommerce-data/geolocation/'
    STORAGE_INTEGRATION = s3_int;



--order item stage 
CREATE OR REPLACE STAGE order_items_stage
    URL = 's3://olist-ecommerce-data/order_items/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;


--order payment stage 
CREATE OR REPLACE STAGE order_payment_stage
    URL = 's3://olist-ecommerce-data/order_payment/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;


--order review stage 
CREATE OR REPLACE STAGE order_review_stage
    URL = 's3://olist-ecommerce-data/order_reviews/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

--order stage 
CREATE OR REPLACE STAGE order_stage
    URL = 's3://olist-ecommerce-data/orders/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

--product category stage 
CREATE OR REPLACE STAGE product_category_stage
    URL = 's3://olist-ecommerce-data/product_category/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;

--product stage 
CREATE OR REPLACE STAGE product_stage
    URL = 's3://olist-ecommerce-data/products/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;


--seller stage 
CREATE OR REPLACE STAGE seller_stage
    URL = 's3://olist-ecommerce-data/sellers/'
    STORAGE_INTEGRATION = s3_int
    FILE_FORMAT = csv_format;




//// SETTING UP SNOWPIPES TO LOAD DATA AUTOMATICALLY INTO TABLES /////


//Setting up snowpipe for orders 
CREATE OR REPLACE pipe order_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.RAW_ORDERS
from @OLIST.SOURCE.ORDER_STAGE
file_format = csv_format
pattern = '.*olist_orders_dataset.*';

-- setting up snowpipe for products
CREATE OR REPLACE pipe product_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.RAW_PRODUCTS
from @OLIST.SOURCE.PRODUCT_STAGE
FILE_FORMAT = csv_format
pattern = '.*olist_products_dataset.*';

-- setting up snowpipe for sellers
--
CREATE OR REPLACE pipe sellers_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.SELLERS
from @OLIST.SOURCE.SELLER_STAGE
FILE_FORMAT = csv_format
pattern = '.*olist_sellers_dataset.*';

-- setting up snowpipe for payments
CREATE OR REPLACE pipe order_payment_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.ORDER_PAYMENT
from @OLIST.SOURCE.ORDER_PAYMENT_STAGE
FILE_FORMAT = csv_format
on_error = continue
pattern = '.*olist_order_payment_dataset.*';


-- setting up snowpipe for order_item
CREATE OR REPLACE pipe order_item_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.RAW_ORDER_ITEM
from @OLIST.SOURCE.order_items_stage
FILE_FORMAT = csv_format
pattern = '.*olist_order_items_dataset.*';


select * from OLIST.RAW.raw_order_item limit 50;

select SYSTEM$PIPE_STATUS('order_item_pipe');


list @OLIST.SOURCE.order_items_stage;

truncate table OLIST.RAW.raw_order_item;



-- setting up snowpipe for geolocation
CREATE OR REPLACE pipe geolocation_pipe
AUTO_INGEST = TRUE 
AS
copy into OLIST.RAW.RAW_GEOLOCATION
from @OLIST.SOURCE.geolocation_stage
FILE_FORMAT = csv_format
on_error = continue
pattern = '.*olist_geolocation_dataset.*';




// Describing pipe to get ARN
DESC pipe sellers_pipe;

///////////////////////////////////////////////////////

////////////////////////////////////////////////////





list @OLIST.SOURCE.product_STAGE;
alter pipe product_pipe set pipe_execution_paused = false;

truncate table  OLIST.RAW.RAW_ORDERS;
select * from OLIST.RAW.RAW_ORDERS;
select * from OLIST.RAW.PRODUCTS;
truncate table OLIST.RAW.PRODUCTS;





    













    














    

    

    
// Listig fils under s3 bucket
list @sellers_stage;


//Setting up snowpipe
CREATE OR REPLACE pipe raw_csv_pipe
AUTO_INGEST = TRUE 
AS
copy into REAL_ESTATE_DB.RAW.HOUSE_LISITING_RAW
from @REAL_ESTATE_DB.RAW.AWS_S3_CSV
pattern = '.*response.*';



select * from REAL_ESTATE_DB.RAW.HOUSE_LISITING_RAW;
// Describing pipe to get ARN
DESC pipe raw_csv_pipe;

// Confirming pipe function with query 
select * from REAL_ESTATE_DB.RAW.HOUSE_LISITING_RAW;

