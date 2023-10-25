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

'''
    /FileStore/tables/department.csv
    /FileStore/tables/employees.csv
    /FileStore/tables/dept_emp.csv
    /FileStore/tables/salaries.csv
'''

# COMMAND ----------



# COMMAND ----------

help(filter)
dir(filter)

# COMMAND ----------

rdd = sc.parallelize(['A','A','B','A','C','A','D','E','F','G','H'])

# COMMAND ----------

rdd.map(lambda letter: (letter,1)).reduceByKey(lambda k,v: k+v).collect()

# COMMAND ----------

sc.parallelize([1,2,3]).fold(0, add)

# COMMAND ----------

help(rdd.fold)

# COMMAND ----------

rdd.takeSample(True, 2)

# COMMAND ----------

spark.sql("select * from employees").show()

# COMMAND ----------

spark.sql("""

SELECT salary, first_name
FROM employees as E
JOIN salaries as S ON E.emp_no = S.emp_no
(SELECT salary
FROM salaries
ORDER BY salary DESC 
LIMIT 4)

""").show()

# COMMAND ----------

spark.sql("""

SELECT * FROM
(SELECT *
FROM employees as E
JOIN salaries as S ON E.emp_no = S.emp_no
ORDER BY salary DESC
LIMIT 4)
ORDER BY salary ASC
LIMIT 1

""").show()

# COMMAND ----------

spark.sql("""

WITH nthsal AS
(
    SELECT *,
        DENSE_RANK() OVER (order by salary desc) as RK
    FROM employees as E
    JOIN salaries as S ON E.emp_no = S.emp_no
    ORDER BY salary DESC
)
SELECT *
FROM nthsal
WHERE RK=4

""").show()

# COMMAND ----------

spark.sql("""

WITH nthsal AS
(
    SELECT *
    FROM employees as E
    JOIN salaries as S ON E.emp_no = S.emp_no
    ORDER BY salary DESC
)
SELECT *
FROM nthsal


""").show()

# COMMAND ----------

spark.sql("""

    SELECT emp_no
    FROM employees
    UNION
    SELECT emp_no
    FROM salaries
    ORDER BY emp_no

""").show()

# COMMAND ----------

spark.sql("""

INSERT INTO employees VALUES(999999,"1998-10-03", "Sudhir", "Singh","M","1998-10-03")

""").show()



# COMMAND ----------

spark.sql("""
select * from employees where emp_no=999999
""").show()

# COMMAND ----------


