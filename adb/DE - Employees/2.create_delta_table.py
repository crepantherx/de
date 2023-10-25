# Databricks notebook source
# MAGIC %run ./1.employees_schema

# COMMAND ----------

DF_employees = spark.read.csv("/land/employees.csv", header=True, schema=SCHEMA_employees)
DF_dept_emp = spark.read.csv("/land/dept_emp.csv", header=True, schema=SCHEMA_dept_emp)
DF_departments = spark.read.csv("/land/departments.csv", header=True, schema=SCHEMA_departments)
DF_salaries = spark.read.csv("/land/salaries.csv", header=True, schema=SCHEMA_salaries)
DF_dept_manager = spark.read.csv("/land/dept_manager.csv", header=True, schema=SCHEMA_dept_manager)

# COMMAND ----------

DF_employees.write.format('delta').mode('overwrite').save('/prod/employee/employees_delta')
DF_dept_emp.write.format('delta').mode('overwrite').save('/prod/employee/dept_emp_delta')
DF_departments.write.format('delta').mode('overwrite').save('/prod/employee/dept_departments')
DF_salaries.write.format('delta').mode('overwrite').save('/prod/employee/salaries_delta')
DF_dept_manager.write.format('delta').mode('overwrite').save('/prod/employee/dept_manager_delta')
