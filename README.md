# Snowflake-Assignment
<hr>

* Created **Admin** and **PII** roles and granted the roles to **ACCOUNTADMIN** role. <br>
* Created **Developer** role and granted to the role **Admin** <br>
* Created warehouse **assignment_wh** of medium size  with the role **AccountAdmin**<br>

```
CREATE OR REPLACE WAREHOUSE assignment_wh
WITH WAREHOUSE_TYPE = 'STANDARD'
    WAREHOUSE_SIZE = 'Medium'
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 3
    AUTO_RESUME = TRUE
    AUTO_SUSPEND = 300
    COMMENT = 'This warehouse created by AccountAdmin role';
 ```

Granting all the privileges on the warehouse **assignment_wh** and  CREATE DATABASE privilege  to the role **Admin** .<br>
```
GRANT ALL PRIVILEGES ON WAREHOUSE assignment_wh TO ROLE Admin;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE Admin;
```

* Created database **assignment_db**, schema **my_schema** and created table **employee**


By using **Admin** role created the database **assignment_db** and schema assignment_db.my_schema **assignment_db.my_schema** <br>

```
CREATE OR REPLACE DATABASE assignment_db;
CREATE SCHEMA IF NOT EXISTS assignment_db.my_schema;
```

Created Variant table **JSON_TABLE** <br>
```
CREATE TABLE JSON_TABLE(
    JSON_DATA VARIANT
);
```

Used the variant table **JSON_TABLE** as the internal stage and staged the data.json <br>

Created json FileFormat **JSON_FILE_FORMAT**, parquet FileFormat **Parquet_file_format** and csv FileFormat **CSV_FILE_FORMAT** <br>

Created external stage **External_stage** and staged the files from the aws s3 bucket **s3://employee-data-bucket-1/**<br>

Created two tables **External_Employee_Table** and **Internal_Employee_Table** <br>

Copied the data from the external stage to the **External_Employee_Table** <br>
```
COPY INTO External_Employee_Table(Name, Phone, Email, Country, Address, file_name)
FROM (SELECT $1 AS Name , $2 AS Phone, $3 AS Email, $4 AS Country , $5 AS Address, METADATA$FILENAME AS file_name FROM @External_stage(
    FILE_FORMAT => CSV_FILE_FORMAT
));
```
Copied the data from the internal stage to the **Internal_Employee_Table** <br>

```
COPY INTO Internal_Employee_Table(Name, Phone, Email, Country, Address, file_name)
FROM (SELECT $1 AS Name , $2 AS Phone, $3 AS Email, $4 AS Country , $5 AS Address, METADATA$FILENAME AS file_name FROM @Internal_stage(
    FILE_FORMAT => CSV_FILE_FORMAT
));
```

Created Internal stage for the parquet file **Internal_stage_for_parquet** <br>
Inferring the schema of the parquet file <br>
```
SELECT * FROM TABLE(INFER_SCHEMA(LOCATION => '@internal_stage_for_parquet' , FILE_FORMAT => 'Parquet_file_format'));
```

Created masking policies for the email and for the phone number of the employee, applied the masing policies to the table **External_Employee_Table**<br>

```
ALTER TABLE
    IF EXISTS External_Employee_Table
MODIFY
    EMAIL
SET
    MASKING POLICY email_mask
```

Granted the necessary privileges to the role **PII** to check the making policies are working in properly.
