# Databricks notebook source
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Local[*]").getOrCreate()
sc = spark.sparkContext

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DateType, IntegerType

SCHEMA_employees = StructType([
    StructField("emp_no", IntegerType()),
    StructField("birth_date", DateType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("hire_date", DateType())
])

SCHEMA_departments = StructType([
    StructField("dept_no", StringType()),
    StructField("dept_name", StringType())
])

SCHEMA_dept_emp = StructType([
    StructField("emp_no", IntegerType()),
    StructField("dept_no", StringType()),
    StructField("from_date", DateType()),
    StructField("to_date", DateType())
])

SCHEMA_salaries = StructType([
    StructField("emp_no", IntegerType()),
    StructField("salary", IntegerType()),
    StructField("from_date", DateType()),
    StructField("to_date", DateType())
])

DF_ORIG_employees = spark.read.csv("/FileStore/tables/employees.csv", header=True, schema=SCHEMA_employees)
DF_ORIG_departments = spark.read.csv("/FileStore/tables/department.csv", header=True, schema=SCHEMA_departments)
DF_ORIG_dept_emp = spark.read.csv("/FileStore/tables/dept_emp.csv",header=True, schema=SCHEMA_dept_emp)
DF_ORIG_salaries = spark.read.csv("/FileStore/tables/salaries.csv", header=True, schema=SCHEMA_salaries)

DF_ORIG_employees.createOrReplaceTempView("employees")
DF_ORIG_departments.createOrReplaceTempView("departments")
DF_ORIG_dept_emp.createOrReplaceTempView("dept_emp")
DF_ORIG_salaries.createOrReplaceTempView("salaries")

# COMMAND ----------

# Employees with their department
spark.sql(
    """
        SELECT DENSE_RANK() OVER (PARTITION BY D.dept_name ORDER BY E.emp_no) as RK
               , E.emp_no
               , E.first_name
               , S.salary
               , E.hire_date
               , E.gender
               , DE.dept_no
               , D.dept_name
               
        FROM employees AS E
        
        JOIN dept_emp AS DE ON E.emp_no = DE.emp_no
        JOIN departments AS D ON DE.dept_no = D.dept_no
        JOIN salaries AS S ON S.emp_no = E.emp_no
        
        WHERE E.hire_date BETWEEN "1998-01-01" AND "2000-12-30"
   
    """).show(10)

# COMMAND ----------

# 4th Highest salary

spark.sql(
    """
    WITH E AS (SELECT * FROM employees) select * from E limit 3
    """).show(10)

# COMMAND ----------

'''
    /FileStore/tables/department.csv
    /FileStore/tables/employees.csv
    /FileStore/tables/dept_emp.csv
    /FileStore/tables/salaries.csv
'''

# COMMAND ----------



# COMMAND ----------

spark.sql("""

SELECT DENSE_RANK() OVER (PARTITION BY D.dept_name ORDER BY E.emp_no) as RK
       , E.emp_no
       , E.first_name
       , S.salary
       , E.hire_date
       , E.gender
       , DE.dept_no
       , D.dept_name

FROM employees AS E

JOIN dept_emp AS DE ON E.emp_no = DE.emp_no
JOIN departments AS D ON DE.dept_no = D.dept_no
JOIN salaries AS S ON S.emp_no = E.emp_no

WHERE E.hire_date BETWEEN "1998-01-01" AND "2000-12-30"

""").show()

# COMMAND ----------


