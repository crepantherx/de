# Databricks notebook source
df = spark.read.table("employee.employees")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %sql desc employee.employees

# COMMAND ----------


