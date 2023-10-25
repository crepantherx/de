# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Window
import pyspark.sql.functions as F

spark = SparkSession.builder.appName("Employees Analytics").getOrCreate()

# COMMAND ----------

DF_employees = spark.read.csv("/FileStore/tables/employees.csv", header=True)
DF_salaries = spark.read.csv("/FileStore/tables/salaries.csv", header=True)
DF_dept_emp = spark.read.csv("/FileStore/tables/dept_emp.csv", header=True)

# COMMAND ----------

employees = DF_employees.alias("employees")
salaries = DF_salaries.alias("salaries")
dept_emp = DF_dept_emp.alias("dept_emp")

# COMMAND ----------

joined_df = employees.join(salaries, on='emp_no', how='inner').join(dept_emp, on='emp_no', how='inner')

# COMMAND ----------

joined_df.show(4)

# COMMAND ----------

df = joined_df.select("employees.emp_no", "dept_emp.dept_no", "salaries.salary", "salaries.from_date", "salaries.to_date")

# COMMAND ----------

window = Window.partitionBy("dept_no").orderBy("salary")

# COMMAND ----------

df.select("dept_no", F.rank().over(window)).show()

# COMMAND ----------


