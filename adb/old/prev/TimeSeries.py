# Databricks notebook source
!curl -O https://raw.githubusercontent.com/facebook/prophet/master/examples/example_wp_log_peyton_manning.csv

# COMMAND ----------

import fbprophet
import matplotlib.pyplot as plt
import pandas as pd

data = pd.read_csv("example_wp_log_peyton_manning.csv")
data["ds"] = pd.to_datetime(data["ds"])

fig = plt.figure(facecolor='w', figsize=(10, 6))
plt.plot(data.ds, data.y)

# COMMAND ----------

!pip install --upgrade pip

# COMMAND ----------

!pip install ephem

# COMMAND ----------

!pip install pystan

# COMMAND ----------

!pip install fbprophet

# COMMAND ----------

import stan
model_code = 'parameters {real y;} model {y ~ normal(0,1);}'
model = stan.StanModel(model_code=model_code)
y = model.sampling().extract()['y']
y.mean()  # with luck the result will be near 0


# COMMAND ----------

!pip install pystan

# COMMAND ----------


