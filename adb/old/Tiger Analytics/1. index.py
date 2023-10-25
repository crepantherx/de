# Databricks notebook source
df = spark.read.format("csv") \
.option("header", "true") \
.load("/FileStore/employees.csv")

# COMMAND ----------

df.show(10)

# COMMAND ----------

from pyspark.sql.functions import count

# COMMAND ----------

result_df = df.select("gender", "emp_no").groupBy("gender").agg(count("emp_no"))

# COMMAND ----------

display(result_df)

# COMMAND ----------

data = [("sudhir", "9891353333"), ("navin", "8587001379")]
contact_info = spark.createDataFrame(data).toDF("name", "phone")

# COMMAND ----------

contact_info.show()

# COMMAND ----------


