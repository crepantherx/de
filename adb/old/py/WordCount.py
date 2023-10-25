# Databricks notebook source
play = sc.textFile("/FileStore/play.txt")

# COMMAND ----------

play.collect()

# COMMAND ----------

df = play.flatMap(lambda x: x.split(" ")).filter(lambda x: x!="")

# COMMAND ----------

list("string")

# COMMAND ----------

string.punctuation

# COMMAND ----------

if '!' in string.punctuation:
    repla 

# COMMAND ----------

def remove_punc(word):
    w = word
    for character in word:
        if character in string.punctuation:
            w = w.replace(character, "")  
    return w

# COMMAND ----------

remove_punc("sudhir!%")

# COMMAND ----------

df = df.map(lambda x: x.lower()).map(lambda x: remove_punc(x))

# COMMAND ----------

type(df)

# COMMAND ----------

!pip install nltk

# COMMAND ----------

nltk.download('stopwords')

# COMMAND ----------

import nltk
from nltk.corpus import stopwords

# COMMAND ----------

sw = stopwords.words('english')

# COMMAND ----------

df = df.map(lambda x: (x,1)).reduceByKey(lambda a,b: a+b)

# COMMAND ----------

df = df.filter(lambda word: word[1] not in sw).filter(lambda word: word[0] != '')

# COMMAND ----------

df.sort()

# COMMAND ----------

import string

# COMMAND ----------

string.punctuation

# COMMAND ----------

help(spark.read)

# COMMAND ----------

df.map(lambda a:(a[1], a[0])).sortByKey(False).collect()

# COMMAND ----------


