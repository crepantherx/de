-- Databricks notebook source
CREATE DATABASE IF NOT EXISTS joins
COMMENT 'In detail example of each type of join'
LOCATION '/user'

-- COMMAND ----------

DROP DATABASE IF EXISTS joins

-- COMMAND ----------

DESC DATABASE joins

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS joins.A(id int);
CREATE TABLE IF NOT EXISTS joins.B(id int);


-- COMMAND ----------

INSERT INTO joins.B VALUES(1), (1), (0), (1), (NULL)
INSERT INTO joins.A VALUES(1), (1), (0), (1), (NULL)

-- COMMAND ----------

select * from joins.A FULL OUTER join joins.B on A.id = B.id

-- COMMAND ----------

ALTER TABLE contacts 
CHANGE phone phone BIGINT 
AFTER name

-- COMMAND ----------

show databases

-- COMMAND ----------

use contacts;

-- COMMAND ----------

desc joins.A

-- COMMAND ----------

-- MAGIC %fs ls /FileStore/tables/
