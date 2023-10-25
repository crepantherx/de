# Databricks notebook source
import datetime as dt
import pytz
import time as tm
import pandas as pd
import numpy as np
import random

# COMMAND ----------

joining_date = '2023-02-14'
joining_date = dt.datetime.strptime(joining_date, "%Y-%m-%d").date()
current_date = dt.datetime.today().date()

# COMMAND ----------

work_experience = current_date - joining_date

# COMMAND ----------

DF_salaries = spark.read.csv("/FileStore/landing/salaries.csv", header=True, inferSchema=True).toPandas()
DF_salaries['work_experience'] = DF_salaries['to_date'] - DF_salaries['from_date']
DF_salaries['from_date'] = pd.to_datetime(DF_salaries['from_date'])

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

DF_salaries['to_date'] = pd.to_datetime(DF_salaries['to_date'], errors='coerce')

# COMMAND ----------

DF_salaries['work_experience'] = DF_salaries['to_date'] - DF_salaries['from_date']

# COMMAND ----------

DF_salaries['from_date'] = DF_salaries['from']

# COMMAND ----------

DF_salaries.drop(columns=['from'], axis=1, inplace=True)

# COMMAND ----------

DF_salaries['to_date'] = DF_salaries['to_date'].dt.date


# COMMAND ----------

DF_salaries['work_experience'] = DF_salaries['work_experience'].dt.days

# COMMAND ----------

DF_salaries.head(3)

# COMMAND ----------

DF_salaries.info()

# COMMAND ----------

# MAGIC %fs ls /FileStore/landing/

# COMMAND ----------

s = dt.datetime(1998,10,3)
e = dt.datetime(2023,8,12)
dr = e-s

# COMMAND ----------

random.randint(0,1000)

# COMMAND ----------

people_born = [random.randint(0,1000) for x in range(1,101)]

# COMMAND ----------

born = [
    s + dt.timedelta(
    days=random.randint(0, dr.days)
    , hours=random.randint(0, 23)
    , minutes=random.randint(0, 59)
    , seconds=random.randint(0, 59)
    ) 
    for _ in range(100)
]

# COMMAND ----------

dob = pd.Series(born, name='dob')
no = pd.Series(people_born, name='no')

# COMMAND ----------

survey = pd.DataFrame({
    dob.name: dob
    , no.name: no
})

# COMMAND ----------

survey.display()

# COMMAND ----------

pd.to_datetime(survey['dob'])

# COMMAND ----------

survey['dob'].dt.tz_localize('CET', ambiguous='infer')

# COMMAND ----------

in_america = pytz.timezone('America/New_York')

# COMMAND ----------

survey['dob'] = survey['dob'].apply(lambda x: in_america.localize(x))

# COMMAND ----------

pytz.timezone('America/New_York').localize(dob[0])

# COMMAND ----------

survey['born'] = survey['dob'].dt.tz_convert('US/Central')

# COMMAND ----------

survey['born'].dt.tz_localize('America/New_York')

# COMMAND ----------

dob.dt.tz_localize('America/New_York')

# COMMAND ----------


