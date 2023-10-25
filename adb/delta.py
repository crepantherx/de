# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE TABLE crepantherx.friend_list 
# MAGIC (
# MAGIC   name STRING,
# MAGIC   age INT
# MAGIC )
# MAGIC USING delta
# MAGIC OPTIONS
# MAGIC (
# MAGIC   path "./FileStore/natural"
# MAGIC )

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE crepantherx

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE crepantherx.emp
# MAGIC USING csv
# MAGIC LOCATIONS

# COMMAND ----------

dislay(spark.sql('DESCRIBE DETAIL'))

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE DETAIL crepantherx.friend_list

# COMMAND ----------

df = spark \
.read \
.option("header", "true") \
.format("csv") \
.load("/FileStore/tables/salaries.csv")


# COMMAND ----------

df \
.write\
.partitionBy("emp_no")\
.format("delta")\
.save("/tmp/crepantherx-delta/")


# COMMAND ----------


