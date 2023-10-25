# Databricks notebook source
# MAGIC %sql create database test

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table test.a (a int, b int, c int, d int, e int)

# COMMAND ----------

# MAGIC %sql 
# MAGIC create table test.b (a int, b int, c int, d int, e int, f int, g int)

# COMMAND ----------

# MAGIC %sql
# MAGIC insert into test.b values
# MAGIC (1,2,3,4,5,6,7)
# MAGIC , (1,2,3,4,5,6,7)
# MAGIC , (1,2,3,4,5,6,7)
# MAGIC , (1,2,3,4,5,6,7)
# MAGIC , (1,2,3,4,5,6,7)
# MAGIC , (1,2,3,4,5,6,7)

# COMMAND ----------

# MAGIC %sql select a,b,c from b;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO test.a (a,b,c)
# MAGIC select a,b,c from test.b;

# COMMAND ----------

# MAGIC %python
# MAGIC dbutils.fs.head("file:/databricks/spark/python/pyspark/sql/utils.py")

# COMMAND ----------

# MAGIC %sh curl -O "file:/databricks/spark/python/pyspark/sql/utils.py"

# COMMAND ----------

# MAGIC %sql SELECT * FROM a

# COMMAND ----------


