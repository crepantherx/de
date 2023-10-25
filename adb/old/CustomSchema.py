# Databricks notebook source
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

schema = StructType([StructField("name", StringType(), True), StructField("grade", IntegerType(), True)])

# COMMAND ----------

spark = SparkSession.builder.appName("CS").getOrCreate()

# COMMAND ----------

 df = spark.read.json("/FileStore/tables/Student-1.json", schema=schema )

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select(["name", "Grade"]).show()

# COMMAND ----------

df.withColumn("Class", df.grade + 5).show()

# COMMAND ----------

df.withColumnRenamed("name", "Name").show()

# COMMAND ----------

df.createOrReplaceTempView("std")

# COMMAND ----------

spark.sql("select * from std where name='Sudhir Singh'").show()

# COMMAND ----------


