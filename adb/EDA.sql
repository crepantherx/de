-- Databricks notebook source
-- MAGIC %python
-- MAGIC NtcnhQTTuPnzef2qaYnO5HG+sm4Z8W1AXjCw1gUVb8ezoLMIVda45GB+b81EPzzHWJxOyQiU5P2B+AStMlsJjg==

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.mount(
-- MAGIC     source = "wasbs://employees@sacrepantherx.blob.core.windows.net",
-- MAGIC     mount_point = "/mnt/azure-blob-employees",
-- MAGIC     extra_configs = {"fs.azure.account.key.sacrepantherx.blob.core.windows.net":"NtcnhQTTuPnzef2qaYnO5HG+sm4Z8W1AXjCw1gUVb8ezoLMIVda45GB+b81EPzzHWJxOyQiU5P2B+AStMlsJjg=="}
-- MAGIC )

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /mnt/azure-blob-employees

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_employees = spark \
-- MAGIC .read \
-- MAGIC .format("csv") \
-- MAGIC .options(header=True, inferSchema=True) \
-- MAGIC .load("/mnt/azure-blob-employees/employees.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_salaries = spark \
-- MAGIC .read \
-- MAGIC .format("csv") \
-- MAGIC .options(header=True, inferSchema=True) \
-- MAGIC .load("/mnt/azure-blob-employees/salaries.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_departments = spark \
-- MAGIC .read \
-- MAGIC .format("csv") \
-- MAGIC .options(header=True, inferSchema=True) \
-- MAGIC .load("/mnt/azure-blob-employees/departments.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_dept_emp = spark \
-- MAGIC .read \
-- MAGIC .format("csv") \
-- MAGIC .options(header=True, inferSchema=True) \
-- MAGIC .load("/mnt/azure-blob-employees/dept_emp.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_dept_manager = spark \
-- MAGIC .read \
-- MAGIC .format("csv") \
-- MAGIC .options(header=True, inferSchema=True) \
-- MAGIC .load("/mnt/azure-blob-employees/dept_manager.csv")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_employees \
-- MAGIC .write \
-- MAGIC .format("parquet") \
-- MAGIC .mode("overwrite") \
-- MAGIC .save("/mnt/azure-blob-employees/employees")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_salaries \
-- MAGIC .write \
-- MAGIC .format("parquet") \
-- MAGIC .mode("overwrite") \
-- MAGIC .save("/mnt/azure-blob-employees/salaries")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_dept_emp \
-- MAGIC .write \
-- MAGIC .format("parquet") \
-- MAGIC .mode("overwrite") \
-- MAGIC .save("/mnt/azure-blob-employees/dept_emp")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_dept_manager \
-- MAGIC .write \
-- MAGIC .format("parquet") \
-- MAGIC .mode("overwrite") \
-- MAGIC .save("/mnt/azure-blob-employees/dept_manager")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC DF_departments \
-- MAGIC .write \
-- MAGIC .format("parquet") \
-- MAGIC .mode("overwrite") \
-- MAGIC .save("/mnt/azure-blob-employees/departments")

-- COMMAND ----------

-- MAGIC %fs
-- MAGIC ls /tmp

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM parquet.`/mnt/azure-blob-employees/employees`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC CREATE DATABASE employee

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DROP TABLE IF EXISTS employee.employees;
-- MAGIC CREATE TABLE IF NOT EXISTS employee.employees 
-- MAGIC USING delta
-- MAGIC OPTIONS (PATH '/mnt/azure-blob-employees/delta/employees/')
-- MAGIC AS SELECT * FROM parquet.`/mnt/azure-blob-employees/employees`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM employee.employees

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC /*by DDL*/
-- MAGIC
-- MAGIC CREATE TABLE employee.salaries
-- MAGIC USING delta
-- MAGIC LOCATION '/mnt/azure-blob-employees/delta/'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC CREATE TABLE IF NOT EXISTS employee.salaries
-- MAGIC USING delta 
-- MAGIC OPTIONS (PATH '/mnt/azure-blob-employees/delta/salaries/')
-- MAGIC AS SELECT * FROM parquet.`/mnt/azure-blob-employees/salaries/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM employee.salaries

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC
-- MAGIC CREATE TABLE IF NOT EXISTS employee.departments (dept_no STRING, dept_name STRING)
-- MAGIC USING delta
-- MAGIC LOCATION '/mnt/azure-blob-employees/delta/departments/'

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM parquet.`/mnt/azure-blob-employees/departments/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE DETAIL employee.departments

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC INSERT INTO employee.departments
-- MAGIC TABLE parquet.`/mnt/azure-blob-employees/departments/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM employee.departments

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE employee.dept_emp
-- MAGIC USING delta
-- MAGIC OPTIONS (PATH '/mnt/azure-blob-employees/delta/dept_emp/')
-- MAGIC AS SELECT * FROM parquet.`/mnt/azure-blob-employees/dept_emp/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT *
-- MAGIC FROM employee.dept_emp

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE employee.dept_manager
-- MAGIC USING delta
-- MAGIC LOCATION '/mnt/azure-blob-employees/delta/dept_manager'
-- MAGIC AS SELECT * FROM parquet.`/mnt/azure-blob-employees/dept_manager/`

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC SELECT * FROM employee.dept_manager

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE HISTORY employee.salaries

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC DESCRIBE HISTORY crepantherx.friend_list

-- COMMAND ----------

CREATE VIEW emp_detail AS
SELECT E.emp_no, DE.dept_no, D.dept_name, E.last_name, E.first_name, E.birth_date, E.hire_date, E.gender, S.from_date, S.to_date, S.salary
FROM employee.employees AS E
JOIN employee.dept_emp AS DE ON E.emp_no = DE.emp_no
JOIN employee.salaries AS S ON S.emp_no = DE.emp_no
JOIN employee.departments AS D ON DE.dept_no = D.dept_no
JOIN employee.dept_manager AS DM ON E.emp_no = DM.emp_no

-- COMMAND ----------

SELECT *
FROM emp_detail

-- COMMAND ----------

SELECT 
  emp_no
  , SUM(salary) AS CTC
FROM emp_detail
GROUP BY emp_no
ORDER BY emp_no

-- COMMAND ----------


