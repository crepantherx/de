-- Databricks notebook source
DROP TABLE IF EXISTS log;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS log 
USING TEXT
OPTIONS (path '/mnt/azure-storage-blob/log/*/*/*/', header=true)

-- COMMAND ----------

SELECT *
FROM log

-- COMMAND ----------

refresh table log
