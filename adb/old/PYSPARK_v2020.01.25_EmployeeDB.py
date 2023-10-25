# Databricks notebook source
from pyspark import SparkContext
from pyspark.sql import SparkSession

sc    = SparkContext.getOrCreate()
spark = SparkSession.builder\
                    .master("Local[*]")\
                    .appName("sql")\
                    .getOrCreate()

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

SCHEMA_employees = StructType([
    StructField("emp_no",       IntegerType()),
    StructField("birth_date",   DateType()),
    StructField("first_name",   StringType()),
    StructField("last_name",    StringType()),
    StructField("gender",       StringType()),
    StructField("hire_date",    DateType())
])

SCHEMA_departments = StructType([
    StructField("dept_no",      StringType()),
    StructField("dept_name",    StringType())
])

SCHEMA_dept_emp = StructType([
    StructField("emp_no",       IntegerType()),
    StructField("dept_no",      StringType()),
    StructField("from_date",    DateType()),
    StructField("to_date",      DateType())
])

SCHEMA_salaries = StructType([
    StructField("emp_no",       IntegerType()),
    StructField("salary",       IntegerType()),
    StructField("from_date",    DateType()),
    StructField("to_date",      DateType())
])


# COMMAND ----------

DF_employees    = spark.read.csv("file:///E:\\Telstra\\BD\\DE\\db\\employees.csv", header=True, schema = SCHEMA_employees)
DF_departments  = spark.read.csv("file:///E:\\Telstra\\BD\\DE\\db\\department.csv", header=True, schema = SCHEMA_departments)
DF_dept_emp     = spark.read.csv("file:///E:\\Telstra\\BD\\DE\\db\\dept_emp.csv", header=True, schema = SCHEMA_dept_emp)
DF_salaries     = spark.read.csv("file:///E:\\Telstra\\BD\\DE\\db\\salaries.csv", header=True, schema = SCHEMA_salaries)

# COMMAND ----------

DF_employees.createOrReplaceTempView("employees")
DF_departments.createOrReplaceTempView("departments")
DF_dept_emp.createOrReplaceTempView("dept_emp")
DF_salaries.createOrReplaceTempView("salaries")

# COMMAND ----------

spark.sql('''
          
          SELECT count(E.gender), D.dept_name
          FROM employees AS E
          JOIN dept_emp AS DE ON E.emp_no=DE.emp_no
          JOIN departments AS D ON D.dept_no = DE.dept_no
          JOIN salaries AS S ON E.emp_no = S.emp_no
          GROUP BY E.gender, D.dept_name
          
          ''').show()


# COMMAND ----------

spark.sql('''
          
SELECT p.dept_name, [p.M], [p.F]
FROM (
    SELECT E.gender, D.dept_name, E.emp_no
    FROM employees.employees AS E
    JOIN employees.dept_emp AS DE ON E.emp_no=DE.emp_no
    JOIN employees.departments AS D ON D.dept_no = DE.dept_no
    JOIN employees.salaries AS S ON E.emp_no = S.emp_no
) as DB
PIVOT 
(
COUNT(emp_no) 
FOR gender IN ([M],[F])
) as p

		

''').show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

DECLARE @name VARCHAR(50)   /* Declare All Required Variables */
DECLARE db_cursor CURSOR FOR   /* Declare Cursor Name*/
SELECT name
FROM myDB.students
WHERE parent_name IN ('Sara', 'Ansh')
OPEN db_cursor   /* Open cursor and Fetch data into @name */ 
FETCH next
FROM db_cursor
INTO @name
CLOSE db_cursor   /* Close the cursor and deallocate the resources */
DEALLOCATE db_cursor
23. What are Entities 

# COMMAND ----------

spark.sql('''
          SELECT count(*)
          FROM employees
          GROUP BY gender
          
          ''').show()

# COMMAND ----------


