# Databricks notebook source
from pyspark.sql.types import *

# COMMAND ----------

SCHEMA_employees = StructType([
    StructField("emp_no", IntegerType(), True),
    StructField("birth_date", DateType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("gender", StringType(), True),
    StructField("hire_date", DateType(), True)
])

SCHEMA_dept_emp = StructType([
    StructField("emp_no", IntegerType(), True),
    StructField("dept_no", StringType(), True),
    StructField("from_date", DateType(), True),
    StructField("to_date", DateType(), True)
])

SCHEMA_departments = StructType([
    StructField("dept_no", StringType(), True),
    StructField("dept_name", StringType(), True)
])

SCHEMA_salaries = StructType([
    StructField("emp_no", IntegerType(), True),
    StructField("salary", FloatType(), True),
    StructField("from_date", DateType(), True),
    StructField("to_date", DateType(), True)
])

SCHEMA_dept_manager = StructType([
    StructField("emp_no", IntegerType(), True),
    StructField("dept_no", StringType(), True),
    StructField("from_date", DateType(), True),
    StructField("to_date", DateType(), True)
])

# COMMAND ----------


