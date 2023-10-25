# Databricks notebook source
# /FileStore/tables/titanic.csv
import pyspark
from pyspark.sql import SparkSession

# COMMAND ----------

spark = SparkSession.builder.appName("titanic").getOrCreate()

# COMMAND ----------

df = spark.read.csv("/FileStore/tables/titanic.csv", header=True, sep=',', inferSchema=True)

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy('Sex').count().show()

# COMMAND ----------

df.groupBy('Sex').mean().show()

# COMMAND ----------

df.groupBy('Sex').mean().select(["Sex","avg(Age)"]).show()

# COMMAND ----------

df.orderBy(df['Age'].desc()).show()

# COMMAND ----------

from pyspark.sql.functions import mean, countDistinct

# COMMAND ----------

df.select(mean('age').alias("avg")).show()

# COMMAND ----------

df.select(countDistinct('Sex')).show()

# COMMAND ----------

df.filter(df["Sex"] == 'male').show()

# COMMAND ----------

df.filter(df["cabin"] == 'C148').show()

# COMMAND ----------


