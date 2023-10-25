# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder \
                    .master("Local[*]")\
                    .appName("Tut")\
                    .enableHiveSupport()\
                    .getOrCreate()

sc    = spark.sparkContext

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------


SCHEMA_employees = StructType([
    StructField("emp_no", IntegerType()),
    StructField("birth_date", DateType()),
    StructField("first_name", StringType()),
    StructField("last_name", StringType()),
    StructField("gender", StringType()),
    StructField("hire_date", DateType())
])

DF_Employee = spark.read.csv("/FileStore/tables/employees.csv", header=True, schema=SCHEMA_employees)

# COMMAND ----------

DF_Employee.show()

# COMMAND ----------

DF_Employee.createOrReplaceTempView('employees')

# COMMAND ----------

spark.sql('''
    SELECT * FROM employees
''').show()

# COMMAND ----------

DF_Employee.printSchema()

# COMMAND ----------

df = DF_Employee

# COMMAND ----------

employees.groupBy(employees.gender).count().show()%

# COMMAND ----------

# MAGIC  %load_ext nb_black

# COMMAND ----------

rows = [
    Row("sudhir", "8487"),
    Row("navin", "9891"),
    Row("sachin", "3333")
]

# COMMAND ----------

print(row)

# COMMAND ----------

rdd = sc.parallelize(rows)

# COMMAND ----------

rdd.collect()

# COMMAND ----------

df = spark.createDataFrame(rdd, schema=SCHEMA)

# COMMAND ----------

from pyspark.sql.types import *

SCHEMA = StructType([
    StructField('name', StringType()),
    StructField('otp', IntegerType())
])


# COMMAND ----------

df.show()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

df.select(F.col('gender')).limit(10).show(1)

# COMMAND ----------

df.select(F.expr('gender as sex')).show(1)

# COMMAND ----------

df.selectExpr('gender as sex', 'gender').show(2)

# COMMAND ----------

df.select(F.expr('*'), F.lit(32).alias('spare')).show()

# COMMAND ----------

df.withColumn('one', F.expr('ls'))

# COMMAND ----------


