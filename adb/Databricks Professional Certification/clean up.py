# Databricks notebook source
# MAGIC %fs ls /FileStore/

# COMMAND ----------

# MAGIC %fs rm dbfs:/FileStore/tables/

# COMMAND ----------

dbutils.fs.rm("/FileStore/*")

# COMMAND ----------


