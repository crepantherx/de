# Databricks notebook source
import pandas as pd

# COMMAND ----------

# MAGIC %fs ls /FileStore/landing/

# COMMAND ----------

s = spark.read.csv("dbfs:/FileStore/landing/salaries.csv", header=True, inferSchema=True).toPandas()
e = spark.read.csv("dbfs:/FileStore/landing/employees.csv", header=True, inferSchema=True).toPandas()
d = spark.read.csv("dbfs:/FileStore/landing/dept_emp.csv", header=True, inferSchema=True).toPandas()

es = pd.merge(e, s, how='inner', on="emp_no")
esd = pd.merge(es, d, how='inner', on='emp_no')

# COMMAND ----------

esd.head()

# COMMAND ----------

temp = esd.loc[:, ~esd.columns.isin(['from_date_y', 'to_date_y'])]

# COMMAND ----------

df = temp.copy()

# COMMAND ----------

df.rename(columns={
    'from_date_x': 'from_date',
    'to_date_x': 'to_date'
}, inplace=True)

# COMMAND ----------

df

# COMMAND ----------

df.info()

# COMMAND ----------

df['to_date'] = df['to_date'].astype(str) 

# COMMAND ----------

ndf = df[df['to_date']=='9999-01-01'][['emp_no', 'dept_no', 'salary']]

# COMMAND ----------

ndf

# COMMAND ----------

ndf.columns

# COMMAND ----------

gndf = ndf.groupby(['dept_no'])

# COMMAND ----------

gndf.agg({
    'salary': 'max'
})

# COMMAND ----------

df.iloc[-1 , :]

# COMMAND ----------

df.nlargest(15, 'salary').iloc[-1]

# COMMAND ----------

df.nlargest(15, 'salary').iloc[-1].reset_index(drop=True)

# COMMAND ----------

lambda x: x.nlargest(7, 'salary').iloc[-1])

# COMMAND ----------

def fn(x):
    return x.nlargest(7,'salary').iloc[-1]

# COMMAND ----------

result = (ndf.groupby('dept_no')
             .apply(lambda x: x.nlargest(7, 'salary').iloc[-1])
             .reset_index(drop=True))

print(result[['emp_no', 'salary', 'dept_no']])

# COMMAND ----------

df.loc[(df['dept_no']=='d007') & (df['to_date'] == '9999-01-01'), :].sort_values(by='salary', ascending=False).head(10)

# COMMAND ----------

(df['dept_no']=='d009') & (df['to_date'] == '9999-01-01')

# COMMAND ----------

df.index

# COMMAND ----------

ndf.head(3)

# COMMAND ----------

def fn(x):
    return x.nlargest(7, 'salary').iloc[-1]

# COMMAND ----------

ndf.groupby(['dept_no']).apply(fn).reset_index(drop=True)

# COMMAND ----------

result = (ndf.groupby('dept_no')
             .apply(lambda x: x.nlargest(7, 'salary').iloc[-1])
             .reset_index(drop=True))

result

# COMMAND ----------

gedsffs\\\\\\\\sdfssd
