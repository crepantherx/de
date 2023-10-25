# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

SCHEMA_employees = StructType([
    StructField("emp_no",       IntegerType()),
    StructField("birth_date",   DateType()),
    StructField("first_name",   StringType()),
    StructField("last_name",    StringType()),
    StructField("gender",       StringType()),
    StructField("hire_date",    DateType()),
])

# COMMAND ----------

DF_ORIGINAL_employees = spark.read.parquet("/FileStore/employee/employees.parquet", header=True, schema=SCHEMA_employees)

# COMMAND ----------

DF_ORIGINAL_employees.select("*").show()

# COMMAND ----------


