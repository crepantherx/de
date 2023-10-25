# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("Local[*]")\
                    .appName("Spark Simplified")\
                    .getOrCreate()

sc = spark.sparkContext


# COMMAND ----------

spark = SparkSession.builder\
                    .master("Local[*]")\
                    .appName("Spark Simplified")\
                    .getOrCreate()

sc = spark.sparkContext

# COMMAND ----------

file = "/FileStore/tables/play.txt"
RDD_file = sc.textFile(file)

# COMMAND ----------

def lower_clean_str(x):
    punc = '!"#$%&\'()*+,./:;<=>?@[\\]^_`{|}~-'
    lowercased_str = x.lower()
    for ch in punc:
        lowercased_str = lowercased_str.replace(ch,'')
    return lowercased_str

# COMMAND ----------

RDD_file.collect()

# COMMAND ----------

RDD_file = RDD_file.map(lower_clean_str)

# COMMAND ----------

RDD_file

# COMMAND ----------

RDD_file = RDD_file.flatMap(lambda line: line.split(" "))

# COMMAND ----------

RDD_file.collect()

# COMMAND ----------

RDD_file = RDD_file.filter(lambda word: word != '')

# COMMAND ----------

RDD_file = RDD_file.map(lambda word: (word,1))

# COMMAND ----------

RDD_file.collect()

# COMMAND ----------

RDD_file = RDD_file.reduceByKey(lambda v1,v2: (v1+v2)).sortByKey()

# COMMAND ----------

RDD_file.collect()

# COMMAND ----------

# most frequent word in descending order

RDD_file = RDD_file.map(lambda x: (x[1], x[0]))

# COMMAND ----------



# COMMAND ----------

!pip install nltk

# COMMAND ----------

import nltk

# COMMAND ----------

nltk.download('stopwords')

# COMMAND ----------

from nltk.corpus import stopwords
stopwords = stopwords.words('english')
stopwords

# COMMAND ----------

RDD_file = RDD_file.filter(lambda x: x[1] not in stopwords).sortByKey(False)

# COMMAND ----------

RDD_file.collect()

# COMMAND ----------

RDD_file.take(5)

# COMMAND ----------

RDD_file

# COMMAND ----------


