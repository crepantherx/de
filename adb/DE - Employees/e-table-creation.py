# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/landing

# COMMAND ----------

# MAGIC %sql create database if not exists employee

# COMMAND ----------

s = spark.read.csv("/FileStore/landing/salaries.csv", header=True, inferSchema=True)
(s.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("overwrite")
  .saveAsTable("employee.salaries")
  )

# COMMAND ----------

dm = spark.read.csv("/FileStore/landing/dept_manager.csv", header=True, inferSchema=True)
(dm.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("overwrite")
  .saveAsTable("employee.dept_manager")
  )

# COMMAND ----------

de = spark.read.csv("/FileStore/landing/dept_emp.csv", header=True, inferSchema=True)
(de.write
  .format("delta")
  .option("mergeSchema", "true")
  .mode("overwrite")
  .saveAsTable("employee.dept_emp")
  )

# COMMAND ----------

d = spark.read.csv("/FileStore/landing/departments.csv", header=True, inferSchema=True)
(d.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("employee.departments")
  )

# COMMAND ----------

e = spark.read.csv("/FileStore/landing/employees.csv", header=True, inferSchema=True)
(e.write
  .format("delta")
  .mode("overwrite")
  .saveAsTable("employee.employees")
  )

# COMMAND ----------

# MAGIC %sql DROP TABLE employee.salaries

# COMMAND ----------

# MAGIC %sql show tables in employee

# COMMAND ----------

# MAGIC %sql DESC employee.employees

# COMMAND ----------

# MAGIC %sql create database e

# COMMAND ----------

# MAGIC %sql create table e.employees as select * from employee.employees

# COMMAND ----------

# MAGIC %sql desc employee.salaries

# COMMAND ----------

# MAGIC %sql SELECT * FROM employee.salaries where to_date = "9999-01-01"

# COMMAND ----------

# MAGIC %sql show tables in e

# COMMAND ----------

# MAGIC %sql drop schema e

# COMMAND ----------


