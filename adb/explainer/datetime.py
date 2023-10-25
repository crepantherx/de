# Databricks notebook source
import pandas as pd

# COMMAND ----------

# .dt.to_datetime()
# .dt.tz_convert()
# .dt.tz_localize()

coerce=True

NaT

NaN

# COMMAND ----------

salaries = spark.read.csv("/FileStore/landing/salaries.csv", header=True, inferSchema=True).toPandas()

# COMMAND ----------

salaries.display()

# COMMAND ----------

salaries.info()

# COMMAND ----------

salaries['from_date'].to_datetime(coerce=True)

# COMMAND ----------

salaries['from_date'] = pd.to_datetime(salaries['from_date'], errors="coerce")

# COMMAND ----------

salaries['to_date'] = pd.to_datetime(salaries['to_date'], errors="coerce")

# COMMAND ----------

salaries['to_date'] = salaries['to_date'].dt.date
salaries['from_date'] = salaries['from_date'].dt.date

# COMMAND ----------

salaries['wex'] = salaries['to_date'] - salaries['from_date']

# COMMAND ----------

salaries['wax'] = salaries['wex'].dt.days

# COMMAND ----------

salaries.drop(columns=['wex'], axis=1, inplace=True)

# COMMAND ----------

salaries.head(3)

# COMMAND ----------

import datetime as dt

# COMMAND ----------

dt.datetime.strptime(x, "%Y-%m-%d")

# COMMAND ----------

salaries['from_date'] = salaries['from_date'].astype(str)

# COMMAND ----------

salaries.loc[0,'to_date']

# COMMAND ----------

salaries['from_date'] = salaries['from_date'].apply(lambda x: dt.datetime.strptime(x, "%Y-%m-%d"))

# COMMAND ----------

salaries.display()

# COMMAND ----------

salaries

# COMMAND ----------


