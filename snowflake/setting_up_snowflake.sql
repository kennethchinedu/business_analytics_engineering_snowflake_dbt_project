-- Use an admin role
USE ROLE ACCOUNTADMIN;

-- Create the `transform` role
CREATE ROLE IF NOT EXISTS transform;
GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN;

-- Create the default warehouse if necessary
CREATE WAREHOUSE IF NOT EXISTS COMPUTE_WH;
GRANT OPERATE ON WAREHOUSE COMPUTE_WH TO ROLE TRANSFORM;

-- Create the `dbt` user and assign to role
CREATE USER IF NOT EXISTS dbt
  PASSWORD='password'
  LOGIN_NAME='dbt'
  MUST_CHANGE_PASSWORD=FALSE
  DEFAULT_WAREHOUSE='COMPUTE_WH'
  DEFAULT_ROLE='transform'
  DEFAULT_NAMESPACE='OLIST.RAW'
  COMMENT='DBT user used for data transformation';
GRANT ROLE transform to USER dbt;


-- Create our database and schemas
CREATE DATABASE IF NOT EXISTS olist;
CREATE SCHEMA IF NOT EXISTS olist.source;
CREATE SCHEMA IF NOT EXISTS olist.raw;

-- Set up permissions to role `transform`
GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform; 
GRANT ALL ON DATABASE olist to ROLE transform;
GRANT ALL ON ALL SCHEMAS IN DATABASE olist to ROLE transform;
GRANT ALL ON FUTURE SCHEMAS IN DATABASE olist to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA olist.raw to ROLE transform;
GRANT ALL ON ALL TABLES IN SCHEMA olist.source to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA olist.raw to ROLE transform;
GRANT ALL ON FUTURE TABLES IN SCHEMA olist.source to ROLE transform;
