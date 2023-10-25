# Databricks notebook source


# COMMAND ----------

df = spark.read.load('abfss://users@contosolake.dfs.core.windows.net/NYCTripSmall.parquet', format='parquet')
display(df.limit(10))

# COMMAND ----------


