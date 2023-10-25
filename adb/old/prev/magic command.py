# Databricks notebook source
ls


# COMMAND ----------

# MAGIC %fs
# MAGIC cat /FileStore/employees.csv

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/

# COMMAND ----------

# MAGIC %fs 
# MAGIC cat "dbfs:/FileStore/employees.csv"

# COMMAND ----------

# MAGIC %sh
# MAGIC cat file:/FileStore/employees.csv

# COMMAND ----------

# MAGIC %sh
# MAGIC ls /usr

# COMMAND ----------


