USE ROLE ACCOUNTADMIN;

-- Creating the roles and hirachy as ACCOUNTADMIN -> Admin -> Developer
CREATE OR REPLACE ROLE  Admin;
CREATE OR REPLACE ROLE  Developer;
GRANT ROLE Developer TO ROLE Admin;
GRANT ROLE Admin TO ROLE ACCOUNTADMIN;


-- Creating the role and hirachy as ACCOUNTADMIN -> PII
CREATE OR REPLACE ROLE PII;
GRANT ROLE PII TO ROLE ACCOUNTADMIN;


USE ROLE ACCOUNTADMIN;
DROP DATABASE IF EXISTS assignment_db;

-- Creating the warehouse assignment_wh using role AccountAdmin
CREATE OR REPLACE WAREHOUSE assignment_wh
WITH WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'Medium'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 300
    COMMENT = 'This warehouse created by AccountAdmin role';


-- Granting permisions to the Admin role
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE Admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE Admin;



-- Swiching to role Admin
USE ROLE Admin;
-- Creating database using role Admin
CREATE OR REPLACE DATABASE assignment_db;
-- Creating schema
CREATE SCHEMA IF NOT EXISTS assignment_db.my_schema;

-- Creating table to load json data
CREATE TABLE JSON_TABLE(
    JSON_DATA VARIANT
);


-- PUT file:///Users/mohangundluri/Downloads/data.json @%JSON_TABLE;
-- list all the stage files
LIST @%JSON_TABLE;

-- Creating file format for json
CREATE OR REPLACE FILE FORMAT JSON_FILE_FORMAT
    TYPE = JSON;

-- Coping data to JSON_TABLE
COPY INTO  JSON_TABLE FROM @%JSON_TABLE
    file_format = JSON_FILE_FORMAT;


select * from json_table;


-- Creating external stage
CREATE STAGE External_stage
    URL='s3://employee-data-bucket-1/'
    CREDENTIALS=(AWS_KEY_ID='' AWS_SECRET_KEY='');

-- list all the stage files
LIST @External_stage;


-- Creating table to load data from external stage
CREATE OR REPLACE TABLE External_Employee_Table(
    Name VARCHAR(50) NOT NULL,
    Phone VARCHAR(15) NOT NULL,
    Email VARCHAR(100) NOT NULL,
    Country VARCHAR(20) NOT NULL,
    Address VARCHAR(100) NOT NULL,
    etl_ts timestamp default current_timestamp() ,
    etl_by VARCHAR(100) default 'SNOW_SIGHT',
    file_name VARCHAR(100)
);


-- Creating table to load data from Internal stage
CREATE OR REPLACE TABLE Internal_Employee_Table(
    Name VARCHAR(50) NOT NULL,
    Phone VARCHAR(15) NOT NULL,
    Email VARCHAR(100) NOT NULL,
    Country VARCHAR(20) NOT NULL,
    Address VARCHAR(100) NOT NULL,
    etl_ts timestamp default current_timestamp() ,
    etl_by VARCHAR(100) default 'SNOW_SIGHT',
    file_name VARCHAR(100)
);


-- Creating file format for csv file
CREATE OR REPLACE FILE FORMAT CSV_FILE_FORMAT
    TYPE='CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1;


-- Coping data into External_Employee_Table from external stage
COPY INTO External_Employee_Table(Name, Phone, Email, Country, Address, file_name)
FROM (SELECT $1 AS Name , $2 AS Phone, $3 AS Email, $4 AS Country , $5 AS Address, METADATA$FILENAME AS file_name FROM @External_stage(
    FILE_FORMAT => CSV_FILE_FORMAT
));


SELECT * FROM External_Employee_Table;



-- Coping data into Internal_Employee_Table from internal stage
COPY INTO External_Employee_Table(Name, Phone, Email, Country, Address, file_name)
FROM (SELECT $1 AS Name , $2 AS Phone, $3 AS Email, $4 AS Country , $5 AS Address, METADATA$FILENAME AS file_name FROM @Internal_stage(
    FILE_FORMAT => CSV_FILE_FORMAT
));


SELECT * FROM Internal_Employee_Table;


-- Creating file format for the parquet files
CREATE OR REPLACE FILE FORMAT Parquet_file_format
    TYPE = 'Parquet';


-- Creating stage for parquet file
CREATE OR REPLACE STAGE Internal_stage_for_parquet FILE_FORMAT = Parquet_file_format;

list @Internal_stage_for_parquet;



SELECT *  FROM @internal_stage_for_parquet/data.parquet ;
-- Infering schema
SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@internal_stage_for_parquet' , FILE_FORMAT => 'Parquet_file_format'));


-- Creating masking
CREATE OR REPLACE MASKING POLICY email_mask AS (val string) returns string ->
  CASE
    WHEN current_role() IN ('PII') THEN VAL
    ELSE '***Masked***'
  END;


  -- Applying masking to the email
ALTER TABLE
    IF EXISTS External_Employee_Table
MODIFY
    EMAIL
SET
    MASKING POLICY email_mask;


  -- Applying masking to the phone
ALTER TABLE
    IF EXISTS External_Employee_Table
MODIFY
    Phone
SET
    MASKING POLICY email_mask;



-- Using role ACCOUNTADMIN
USE ROLE ACCOUNTADMIN;

-- Creating developer role
CREATE USER IF NOT EXISTS Developer1 PASSWORD='Developer1' DEFAULT_ROLE = Developer;

-- Granting previleges to the pii role
GRANT ALL PRIVILEGES ON WAREHOUSE ASSIGNMENT_WH TO ROLE PII;
GRANT USAGE ON DATABASE ASSIGNMENT_DB TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.Internal_Employee_Table TO ROLE PII;
GRANT SELECT ON TABLE assignment_db.my_schema.External_Employee_Table TO ROLE PII;


-- Using role PII
USE ROLE PII;
-- selecting from the internal_stage_for_parquet
SELECT * FROM  TABLE(INFER_SCHEMA(LOCATION => '@internal_stage_for_parquet' , FILE_FORMAT => 'Parquet_file_format'));



-- Creating user with role developer
CREATE USER IF NOT EXISTS Developer1 PASSWORD='Developer1' DEFAULT_ROLE =' Developer'  DEFAULT_WAREHOUSE = 'assignment_wh'  MUST_CHANGE_PASSWORD = FALSE;;
GRANT ROLE Developer TO USER Developer1;


-- Using developer1 user
USE user 'Developer1' PASSWORD='Developer1';


-- selecting from the internal_stage_for_parquet
SELECT * FROM  TABLE(INFER_SCHEMA(LOCATION => '@internal_stage_for_parquet' , FILE_FORMAT => 'Parquet_file_format'));
