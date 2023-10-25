-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS employee;
DROP TABLE IF EXISTS employee.employees;
CREATE TABLE IF NOT EXISTS employee.employees 
USING csv
OPTIONS (
  path "/FileStore/employees.csv", 
  header "true",
  inferSchema="true"
)

-- COMMAND ----------

DESC TABLE EXTENDED crepantherx.employees

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT *
FROM employee.employees

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head /FileStore/tables/salaries.csv

-- COMMAND ----------

CREATE TABLE employee.salaries 
USING csv
OPTIONS(path "/FileStore/tables/salaries.csv")

-- COMMAND ----------

SELECT * 
FROM employee.salaries

-- COMMAND ----------

DESC TABLE employee.salaries

-- COMMAND ----------

DROP TABLE IF EXISTS employee.salaries

-- COMMAND ----------

DROP TABLE IF EXISTS employee.salaries;
CREATE TABLE IF NOT EXISTS employee.salaries
(
  emp_no INT
  , salary INT
  , from_date DATE
  , to_date DATE
)
USING csv
OPTIONS(
path "/FileStore/tables/salaries.csv"
, header true
)


-- COMMAND ----------

CREATE TABLE tbl_name (name varchar(), age int, )

-- COMMAND ----------

DESC TABLE employee.salaries

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC
-- MAGIC head /FileStore/tables/department.csv

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS department
(
  dept_no STRING
  , 
)

-- COMMAND ----------


DROP TABLE IF EXISTS employee.departments;
CREATE TABLE IF NOT EXISTS employee.departments 
USING csv
OPTIONS (
  path "/FileStore/tables/department.csv", 
  header "true",
  inferSchema="true"
)


-- COMMAND ----------

DESC TABLE employee.departments

-- COMMAND ----------

DROP TABLE IF EXISTS employee.departments;

-- COMMAND ----------


CREATE TABLE IF NOT EXISTS employee.departments 
(
  dept_no STRING
  , dept_name STRING
)
USING csv
OPTIONS (
  path "/FileStore/tables/department.csv", 
  header true
)

-- COMMAND ----------

SELECT * 
FROM employee.departments

-- COMMAND ----------

DESC TABLE employee.departments

-- COMMAND ----------

LOAD DATA INPATH "/FileStore/tables/department.csv" INTO TABLE departments

-- COMMAND ----------

SELECT *
FROM employee.salaries

-- COMMAND ----------

SELECT 
  emp_no
  , salary
  , RANK() OVER (PARTITION BY emp_no ORDER BY salary DESC) as rk
FROM employee.salaries
ORDER BY emp_no

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS workers
(
  name STRING
  , dept STRING
  , salary INT
  , age INT
)

-- COMMAND ----------

INSERT INTO workers VALUES
('Lisa', 'Sales', 10000, 35),
('Evan', 'Sales', 32000, 38),
('Fred', 'Engineering', 21000, 28),
('Alex', 'Sales', 30000, 33),
('Tom', 'Engineering', 23000, 33),
('Jane', 'Marketing', 29000, 28),
('Jeff', 'Marketing', 35000, 38),
('Paul', 'Engineering', 29000, 23),
('Chloe', 'Engineering', 23000, 25);

-- COMMAND ----------

SELECT *
FROM workers

-- COMMAND ----------

SELECT 
  name
  , dept
  , RANK() OVER (PARTITION BY dept ORDER BY salary) as rk
FROM 
  workers

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev_migration_test.test (
cac string,
sfdc_b2c_accountuuid string,
cdbor_cidn string,
ban string,
sourcesystem string,
billingsystem string,
pkg_id string,
package_type string,
eligibility_status string,
migration_status string,
scheduled_migration_date date,
last_updated timestamp,
service_id string,
service_type string,
srv_subtype string,
srv_ineligibility_reason_code string,
package_service_count int,
part_num string,
name string,
type string,
dac_mapping string,
mapped_product_name string,
product_code string,
action string,
id string,
relies_on string,
b2c_price string,
additional_comms_required string,
product_char string,
product_char_value string,
orderitem_char string,
orderitem_char_value string,
attributename string,
attributevalue string,
ocs_notif_pref string,
characteristics struct<characteristics:string>,
bill_cycle string,
legacy_bc_date date,
sf_ad_date date,
charge_model string,
bundle_products string,
serviceadborId string,
charge_cycle string
)USING csv OPTIONS (path "/home/d979967/lodgement_delta_extract_fixed_sample.xlsx", header "true", inferSchema="true") S

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev_migration_test.test 
(
cac string,
sfdc_b2c_accountuuid string,
cdbor_cidn string,
ban string,
sourcesystem string,
billingsystem string,
pkg_id string,
package_type string,
eligibility_status string,
migration_status string,
scheduled_migration_date date,
last_updated timestamp,
service_id string,
service_type string,
srv_subtype string,
srv_ineligibility_reason_code string,
package_service_count int,
part_num string,
name string,
type string,
dac_mapping string,
mapped_product_name string,
product_code string,
action string,
id string,
relies_on string,
b2c_price string,
additional_comms_required string,
product_char string,
product_char_value string,
orderitem_char string,
orderitem_char_value string,
attributename string,
attributevalue string,
ocs_notif_pref string,
characteristics struct<characteristics:string>,
bill_cycle string,
legacy_bc_date date,
sf_ad_date date,
charge_model string,
bundle_products string,
serviceadborId string,
charge_cycle string
)
STORED AS ORC USING csv OPTIONS (path "/home/d979967/lodgement_delta_extract_fixed_sample.xlsx", header "true", inferSchema="true")

-- COMMAND ----------

LOAD DATA LOCAL INPATH '/home/d979967/lodgement_delta_extract_fixed_sample.csv' INTO TABLE dev_migration_test.test;

-- COMMAND ----------

INSERT INTO dev_migration_test.test SELECT * FROM dev_migration_test.test1

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS dev_migration_test.test1 
(
cac string,
sfdc_b2c_accountuuid string,
cdbor_cidn string,
ban string,
sourcesystem string,
billingsystem string,
pkg_id string,
package_type string,
eligibility_status string,
migration_status string,
scheduled_migration_date date,
last_updated timestamp,
service_id string,
service_type string,
srv_subtype string,
srv_ineligibility_reason_code string,
package_service_count int,
part_num string,
name string,
type string,
dac_mapping string,
mapped_product_name string,
product_code string,
action string,
id string,
relies_on string,
b2c_price string,
additional_comms_required string,
product_char string,
product_char_value string,
orderitem_char string,
orderitem_char_value string,
attributename string,
attributevalue string,
ocs_notif_pref string,
characteristics struct<characteristics:string>,
bill_cycle string,
legacy_bc_date date,
sf_ad_date date,
charge_model string,
bundle_products string,
serviceadborId string,
charge_cycle string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
LINES TERMINATED BY '\n'
STORED AS TEXTFILE;

-- COMMAND ----------

LOAD DATA LOCAL INPATH '/home/d979967/lodgement_delta_extract_fixed_sample.csv' INTO TABLE dev_migration_test.test1;

-- COMMAND ----------

DROP TABLE IF EXISTS dev_migration_test.test1;

-- COMMAND ----------

INSERT INTO dev_migration_test.test SELECT * FROM  dev_migration_test.test1;
LOAD DATA LOCAL INPATH '/home/d979967/lodgement_delta_extract_fixed_sample.csv' INTO TABLE dev_migration_test.test1;
hdfs dfs -ls /apps/hive/warehouse/dev_migration_test.db/test
hadoop fs -get /apps/hive/warehouse/dev_migration_test.db/test/000000_0 .
