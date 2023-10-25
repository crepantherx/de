# Databricks notebook source
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

from auto_ts import auto_timeseries

from sklearn.metrics import explained_variance_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, median_absolute_error
from sklearn.metrics import r2_score

# COMMAND ----------

!pip install auto-ts

# COMMAND ----------

!pip install pystan

# COMMAND ----------

df_rain_temp = pd.read_csv('df_rain_temp.csv')
df_rain_temp['Date'] = pd.to_datetime(df_rain_temp['Date'])

# COMMAND ----------

df_sa = pd.read_csv('SA_YearApril2021-April2022.csv')
df_sa['Audit_Date'] = pd.to_datetime(df_sa['Audit_Date'])

# COMMAND ----------

fulldata = pd.merge(left = df_sa, right = df_rain_temp, left_on ='Audit_Date', right_on ='Date', how ='left')
fulldata.drop('Date', inplace =True, axis=1)

fulldata = fulldata.fillna(value={'Rainfall': 0, 'Temp': 29})
fulldata = fulldata.sort_values('Audit_Date')

# COMMAND ----------

fulldata.shape

# COMMAND ----------

fulldata.drop('Team', axis=1, inplace =True)

# COMMAND ----------

fulldata.head()

# COMMAND ----------

fulldata.columns = ['ds', 'y', 'rainfall', 'temp']

# COMMAND ----------

fulldata.head()

# COMMAND ----------

fulldata = fulldata.loc[:,['ds', 'y','rainfall','temp']]

# COMMAND ----------

train = fulldata[fulldata['ds']<'2022-03-01']

test = fulldata[fulldata['ds']>='2022-03-01']

# COMMAND ----------

model = auto_timeseries(score_type='rmse', time_interval ='D', model_type=['best'])

# COMMAND ----------

model.fit(traindata=train[['ds', 'y', 'rainfall','temp']], ts_column ='ds', target ='y', cv=5)

# COMMAND ----------

model.get_leaderboard()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

model.get_cv_scores()

# COMMAND ----------

results_dict = model.get_ml_dict()

# COMMAND ----------

results_dict

# COMMAND ----------

results_dict['Prophet']['forecast']

# COMMAND ----------

future_predictions1 = model.predict(testdata = test[['ds', 'y', 'rainfall','temp']], model ='Prophet')
future_predictions2 = model.predict(testdata = test[['ds', 'y', 'rainfall','temp']], model ='auto_SARIMAX')
future_predictions3 = model.predict(testdata = test[['ds', 'y', 'rainfall','temp']], model ='VAR')

# COMMAND ----------

test1 = test.copy()
test1.reset_index(drop=True, inplace =True)

# COMMAND ----------

test1.head()

# COMMAND ----------

test1['yhat'] = future_predictions1['yhat']
test1 = test1.rename(columns = {'yhat':'Prophet_pred'})

# COMMAND ----------

test1.head()

# COMMAND ----------

future_predictions2.reset_index(drop=True, inplace =True)
future_predictions3.reset_index(drop=True, inplace =True)

# COMMAND ----------

test1['yhat'] = future_predictions2['yhat']
test1 = test1.rename(columns = {'yhat':'Arima_pred'})

# COMMAND ----------

test1.head()

# COMMAND ----------

test1['yhat'] = future_predictions3['yhat']
test1 = test1.rename(columns = {'yhat':'VAR'})

# COMMAND ----------

test1.head()

# COMMAND ----------

test1['dayname'] = test1['ds'].dt.day_name()
test1 = test1[~test1['dayname'].isin(['Saturday','Sunday'])]

# COMMAND ----------

test1.head()

# COMMAND ----------

test2 = test1.loc[:,['ds', 'y', 'Prophet_pred', 'Arima_pred', 'VAR']]

# COMMAND ----------

test2.head()

# COMMAND ----------

test3 = test2.copy()

test3['Prophet_pred'] = test3['Prophet_pred'].round()
test3['Arima_pred'] = test3['Arima_pred'].round()
test3['VAR'] = test3['VAR'].round()

# COMMAND ----------

test3.head()

# COMMAND ----------

from sklearn.metrics import explained_variance_score
from sklearn.metrics import mean_squared_error, mean_absolute_error, median_absolute_error
from sklearn.metrics import r2_score

def mean_absolute_percentage_error(y_true, y_pred): 
    """Calculates MAPE given y_true and y_pred"""
    y_true, y_pred = np.array(y_true), np.array(y_pred)
    return (np.sum(np.abs(y_true - y_pred)) / np.sum(y_true)) *100

MAPE = mean_absolute_percentage_error(test3['y'], test3['VAR'])

Accuracy =  abs(100-MAPE)

# COMMAND ----------

MAPE = mean_absolute_percentage_error(test3['y'], test3['Prophet_pred'])
Accuracy =  abs(100-MAPE)
print("Accuracy for Prophet:", Accuracy)

# COMMAND ----------

MAPE = mean_absolute_percentage_error(test3['y'], test3['Arima_pred'])
Accuracy =  abs(100-MAPE)
print("Accuracy for SARIMA:", Accuracy)

# COMMAND ----------

MAPE = mean_absolute_percentage_error(test3['y'], test3['VAR'])
Accuracy =  abs(100-MAPE)
print("Accuracy for VAR:", Accuracy)

# COMMAND ----------



# COMMAND ----------


