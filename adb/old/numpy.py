# Databricks notebook source
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

df = spark.read.csv('/land/salaries.csv', header=True).toPandas()

# COMMAND ----------

df.info

# COMMAND ----------

df.info()

# COMMAND ----------

df['from_date'] = pd.to_datetime(df['from_date'], errors='coerce')
df['to_date'] = pd.to_datetime(df['to_date'], errors='coerce')

# COMMAND ----------

plt.bar(df['from_date'], df['salary'])

# COMMAND ----------


