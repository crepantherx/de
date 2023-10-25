# Databricks notebook source
spark.sql('CREATE DATABASE IF NOT EXISTS employee')

# COMMAND ----------

spark.sql("USE DATABASE default")

# COMMAND ----------

spark.sql("CREATE TABLE employees USING delta OPTIONS('path' '/prod/employees_delta')")
spark.sql("CREATE TABLE dept_emp USING delta OPTIONS('path' '/prod/dept_emp_delta')")
spark.sql("CREATE TABLE departments USING delta OPTIONS('path' '/prod/departments_delta')")
spark.sql("CREATE TABLE salaries USING delta OPTIONS('path' '/prod/salaries_delta')")
spark.sql("CREATE TABLE dept_manager USING delta OPTIONS('path' '/prod/dept_manager_delta')")

# COMMAND ----------

spark.sql("SELECT * FROM salaries").show()

# COMMAND ----------


