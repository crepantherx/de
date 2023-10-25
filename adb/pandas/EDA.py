# Databricks notebook source
import pandas as pd

# COMMAND ----------

df = spark.read.csv("dbfs:/FileStore/train.csv", header=True).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC #### General Info

# COMMAND ----------

df.info()

# COMMAND ----------

df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### Null Handling

# COMMAND ----------

dropna() -> any or all not null rows, any or all not null columns
isna(), isnull() -> true, false
# df.loc[df.isna().any(axis=1), df.isna().any(axis=0)]

# COMMAND ----------

df.columns

# COMMAND ----------

df[df['Cabin'].isnull()]

# COMMAND ----------

def fn(a: int,b: int=2):
    print(a+b)

# COMMAND ----------

df.loc[df['Cabin'].isna(), :]

# COMMAND ----------

df.isna()

# COMMAND ----------



# COMMAND ----------

help(df.isna)

# COMMAND ----------

df.dropna(axis=0, how='any')

# COMMAND ----------

help(df.isna)

# COMMAND ----------

help(df.dropna)

# COMMAND ----------

df[ ----row cond---------]

# COMMAND ----------



# COMMAND ----------

df.dropna(axis=0, how='any')

# COMMAND ----------

df.loc[df.isna().any(axis=1), df.isna().any(axis=0)]

# COMMAND ----------

df.count()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

df.isna().any(axis=0)

# COMMAND ----------

df[df.isna().any(axis=1)]

# COMMAND ----------

df.loc[:,df.isna().any(axis=0)]

# COMMAND ----------

df[df.isna().any(axis=0)]

# COMMAND ----------

df['Cabin'].isnull()

# COMMAND ----------

help(df.any)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #### agg

# COMMAND ----------

display(df)

# COMMAND ----------

df.info()

# COMMAND ----------

avg fare of each class
agg         group

# COMMAND ----------

df['Age'] = df['Age'].astype(float)

# COMMAND ----------

df['Fare'] = df['Fare'].astype(float)

# COMMAND ----------

group = df.groupby(['Pclass'])
a = group.agg({
    'Fare': ['mean', 'min', 'max']
    , 'Age': ['mean', 'std']
})

# COMMAND ----------

a.columns

# COMMAND ----------

a

# COMMAND ----------

df

# COMMAND ----------

df['Age'] = df['Age'].fillna(0)

# COMMAND ----------

df['Cabin'] = df['Cabin'].fillna("-")

# COMMAND ----------



# COMMAND ----------

# MAGIC %md ####Windows

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md #### Employees

# COMMAND ----------

dbutils.fs.mkdirs("/FileStore/landing/")

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/landing/

# COMMAND ----------

# MAGIC %fs rm -r /FileStore/landing/

# COMMAND ----------

# MAGIC %fs mkdirs /FileStore/landing/

# COMMAND ----------


