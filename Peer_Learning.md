## Snowflake Peer learnong
<hr>

### Rohith`s Approach--
* Created **Admin** and **PII** roles and granted the roles to **ACCOUNTADMIN** role. <br>
* Created **Developer** role and granted to the role **Admin** <br>
* Created warehouse **assignment_wh** of medium size and granted all the privileges on the warehouseand granted the create database to the role **Admin**<br>
* Created database **assignment_db**, schema **my_schema** and created table **employee**

```
CREATE or REPLACE table employee (
    ID integer,
    firstname varchar(100),
    lastname varchar(100),
    email varchar(100),
    phoneNumber varchar(10),
    city varchar(50),
    etl_ts timestamp default current_timestamp(),
        -- for getting the time at which the record is getting inserted
    etl_by varchar default 'snowsight',
        -- for getting application name from which the record was inserted
    filename varchar -- for getting the name of the file USEd to insert data into the table.
);
```

Created csv File format **my_csv_format** <br>
```
CREATE OR REPLACE FILE FORMAT my_csv_format TYPE = 'CSV' FIELD_DELIMITER = ',' SKIP_HEADER = 1;
```

Created Internal stage **internal_stage** and external stage **external_stage** <br>
Added employee.csv file in to the internal stage.<br>
Created Storage integration with AWS. Staged the files from aws in the external stage <br>


Created 2 tables **employee_int** and **employee_ext** <br>
```
CREATE or REPLACE TABLE employee_int LIKE employee;
CREATE or REPLACE TABLE employee_ext LIKE employee;
```

Copied the data in the internal stage into the table **employee_int**  <br>
```
COPY INTO employee_int(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1, $2, $3, $4, $5, $6, METADATA$FILENAME FROM @internal_stage/employeesf.csv.gz)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;
```

Copied the data in the external stage into the table **employee_ext**  <br>
```
COPY INTO employee_ext(ID, firstname, lastname, email, phonenumber, city, filename)
FROM (SELECT $1,$2,$3,$4,$5,$6,metadata$filename FROM @external_stage)
FILE_FORMAT = my_csv_format
ON_ERROR = CONTINUE;
```

Created stage for parquet file **parquet_int** and created **my_parquet_format** file format <br>

Created Masking Policies for email and for contact_mask
```
CREATE OR REPLACE MASKING POLICY email_mask AS (VAL string) RETURNS string ->
  CASE
    WHEN CURRENT_ROLE() IN ('PII') THEN VAL
    ELSE '**masked**'
  END;
```

Applied the masking policies for the internal stage table and external stage table <br>
```
ALTER TABLE IF EXISTS employee_int MODIFY Email SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_ext MODIFY Email SET MASKING POLICY email_mask;
ALTER TABLE IF EXISTS employee_ext MODIFY phonenumber SET MASKING POLICY contact_mask;
ALTER TABLE IF EXISTS employee_int MODIFY phonenumber SET MASKING POLICY contact_mask;
```

Finally Grating the necessary privileges to the role **Developer** and **PII** to check masking policies works properly or not.<br>

### Rohith`s Approach--

* Created **Admin** and **PII** roles and granted the roles to **ACCOUNTADMIN** role. <br>
* Created **Developer** role and granted to the role **Admin** <br>
* Created warehouse **assignment_wh** of XSMALL size, standard type  and granted all the privileges on the warehouseand granted the create database to the role **Admin**<br>
* Created database **assignment_db**, schema **my_schema** and created table **emp**<br>
```
CREATE DATABASE assignment_db;
CREATE SCHEMA my_schema;
```
Table emp <br>
```
CREATE OR REPLACE TABLE emp(
    EMPLOYEE_ID NUMBER,
    FIRST_NAME STRING,
    LAST_NAME STRING,
    EMAIL STRING,
    PHONE_NUMBER STRING,
    HIRE_DATE STRING,
    JOB_ID STRING,
    SALARY NUMBER,
    COMMISSION_PCT STRING,
    MANAGER_ID STRING,
    DEPARTMENT_ID STRING);
```
Created csv FileFormat **my_csv_format** <br>
Created Internal stage **My_int_stage** and staged the file **employees.csv** by using below command.<br>

``` 
put file://~/Desktop/employees.csv @my_int_stage;
```

Copied the data in the internal stage into the table **emp** <br>
```
COPY INTO emp FROM @my_int_stage
file_format=(format_name=my_csv_format, skip_header=1)
```

Created Storage integration with AWS. Staged the files from aws in the external stage <br>

Created external stage **my_ext_stage** and copied the data from the external stage to the variant table **emp_V**<br>
Created parquet FileFormat ** my_parquet_forma** <br>

Inferring the schema of the parquet file <br>
```
SELECT * FROM TABLE(
    INFER_SCHEMA(
        LOCATION=>'@my_ext_stage/userdata1.parquet',
        FILE_FORMAT=>'my_parquet_format')
    );
```

Created masking policies for email and salary and altered the table and set the masking policies.<br>
Granted required and necessary privileges to the Developer role to check the masking policies are working in properly
