# Databricks notebook source
import pyspark

# COMMAND ----------

from pyspark.sql import SparkSession

# COMMAND ----------

ss = SparkSession.builder.appName("Bridge").getOrCreate()

# COMMAND ----------

type(ss)

# COMMAND ----------

df = ss.read.json("/FileStore/tables/Student.json")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.columns

# COMMAND ----------

df.count()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.head(2)

# COMMAND ----------

import 
