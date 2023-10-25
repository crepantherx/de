# Databricks notebook source
DF_employees = spark.read.format("delta").load("/prod/employee/employees_delta")

# COMMAND ----------

DF_employees.printSchema()

# COMMAND ----------

e = DF_employees.alias('employees')

# COMMAND ----------

e.show(2)

# COMMAND ----------

employees.describe().show()

# COMMAND ----------

e.na

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

null_counts = e.select( [F.count(F.when(F.col(c).isNull(), c)).alias(c) for c in e.columns])
null_counts.show()

# COMMAND ----------

condition = reduce(lambda x, y: x | y, [F.isnull(c) for c in e.columns])

# COMMAND ----------

from functools import reduce

# COMMAND ----------

e.filter(condition).select(*e.columns).show()

# COMMAND ----------

e.show(10)

# COMMAND ----------

df = e.withColumn('year', F.date_format(e['hire_date'], 'MM')).groupBy('year').agg(F.count('emp_no').alias('emplooyees')).sort(F.col('year').desc()).toPandas()

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

e.withColumn('month', F.date_format(e['hire_date'], 'MM')).groupBy('month').count().show()

# COMMAND ----------

import matplotlib.pyplot as plt

# COMMAND ----------

e.withColumn('month', F.date_format(e['hire_date'], 'MM')).groupBy('month').count().sort(F.col('month').asc()).toPandas().plot(kind = 'bar',
        x = 'month',
        y = 'count',
        color = 'red')

# COMMAND ----------

df = DF_employees.toPandas()

# COMMAND ----------

import numpy as np
import pandas as pd
from matplotlib import pyplot as plt

# COMMAND ----------

df.info()

# COMMAND ----------

df.birth_date = pd.to_datetime(df.birth_date)

# COMMAND ----------

df.gender = pd.Categorical(df.gender)

# COMMAND ----------

df.groupby([df.hire_date.dt.year, df.gender]).count().plot(legend=True, kind='bar')

# COMMAND ----------

df.hire_date.dt.year

# COMMAND ----------

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

employees = pd.read_csv("/dbfs/land/employees.csv", index_col='emp_no')
salaries = pd.read_csv('/dbfs/land/salaries.csv', index_col='emp_no')

employee_salary_data = employees.join(salaries, on='emp_no')

employee_salary_data['from_date'] = pd.to_datetime(employee_salary_data['from_date'], errors='coerce')
employee_salary_data['to_date'] = pd.to_datetime(employee_salary_data['to_date'], errors='coerce')
employee_salary_data['hire_date'] = pd.to_datetime(employee_salary_data['hire_date'], errors='coerce')
employee_salary_data['birth_date'] = pd.to_datetime(employee_salary_data['birth_date'], errors='coerce')

# COMMAND ----------

dbutils.fs.ls('/land/employees.csv')

# COMMAND ----------


