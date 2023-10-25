# Databricks notebook source
spark.catalog.listTables()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /FileStore/tables/

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark import SparkContext

sc    = SparkContext.getOrCreate()
spark = SparkSession.builder \
                    .master("Local[*]") \
                    .appName("Interview") \
                    .enableHiveSupport() \
                    .getOrCreate()

# COMMAND ----------

spark

# COMMAND ----------

sc

# COMMAND ----------

file_path = "/FileStore/tables/play.txt"
poem = sc.textFile(file_path)

# COMMAND ----------

poem.collect()

# COMMAND ----------

poem.flatMap(lambda line: line.split(" "))\
            .filter(lambda word: word!=" ")\
            .map(rm_lower_punc)\
            .map(lambda word: (word, 1))\
            .reduceByKey(lambda a,b: (a+b))\
            .map(lambda word: (word[1],word[0]))\
            .filter(lambda pair: pair[1] not in stopwords)\
            .sortByKey(False).collect()

# COMMAND ----------

def rm_lower_punc(str):
    str = str.lower()
    punc = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~-'
    for ch in punc:
        str = str.replace(ch, '')
    return str

# COMMAND ----------

import nltk
from nltk.corpus import stopwords

nltk.download('stopwords')
stopwords = stopwords.words('english')

# COMMAND ----------

!pip install nltk

# COMMAND ----------

wc.sortByKey(False).collect()

# COMMAND ----------


