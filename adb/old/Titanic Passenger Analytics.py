# Databricks notebook source
# MAGIC %fs ls /FileStore/tables

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder\
                    .master("Local[*]")\
                    .appName("Titanic Analysis")\
                    .getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

spark.conf.set("spark.sql.repl.eagerEval.enabled", True)

# COMMAND ----------

DF_titanic = spark.read.csv("/FileStore/tables/titanic.csv", inferSchema=True, header=True)

# COMMAND ----------

tt = DF_titanic

# COMMAND ----------

tt

# COMMAND ----------

tt.groupBy(tt.sex).avg("age").alias("age")

# COMMAND ----------

tt.select(tt.survived, tt.cabin).where(tt.cabin == "C22 C26").groupBy(tt.survived).count()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM employees

# COMMAND ----------

spark.conf.get("spark.sql.files.maxPartitionBytes")

# COMMAND ----------

print(134217728/1024/1024)


# COMMAND ----------



