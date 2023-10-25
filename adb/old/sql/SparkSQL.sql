-- Databricks notebook source
-- MAGIC %python
-- MAGIC salaries = spark.read.csv("/FileStore/tables/salaries.csv", header=True)
-- MAGIC df.show()

-- COMMAND ----------

-- MAGIC %python
-- MAGIC salaries.createOrReplaceTempView("salaries")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC df.createOrReplaceTempView("employees")

-- COMMAND ----------

SELECT *, concat(substring(first_name, 0,3), substring(last_name, length(last_name) - 2, length(last_name))) as EmployeeNameCode
FROM employees

-- COMMAND ----------

SELECT concat("sudhir" , "zen")
FROM employees

-- COMMAND ----------

SELECT datediff(hire_date, birth_date) 
FROM employees

-- COMMAND ----------

SELECT months_between(hire_date, birth_date) 
FROM employees

-- COMMAND ----------

SELECT *, ROW_NUMBER() OVER (PARTITION BY diff ORDER BY hire_date DESC) as rk
FROM 
(
SELECT hire_date, datediff(LAG(hire_date) OVER (ORDER BY hire_date), hire_date) as diff
FROM employees
)


-- COMMAND ----------

SELECT ["hi", "by"]

-- COMMAND ----------

SELECT array(1,2,3,4,5)

-- COMMAND ----------

SELECT "hello" "bye"

-- COMMAND ----------

SELECT *
FROM employees

-- COMMAND ----------

SELECT last_name, CONCAT(first_name, LEAD(first_name) OVER (PARTITION BY last_name ORDER BY last_name))
FROM employees

-- COMMAND ----------

DROP TABLE IF EXISTS salaries;
CREATE TABLE IF NOT EXISTS salaries 
()
