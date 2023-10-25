// Databricks notebook source
var poem = sc.textFile("/FileStore/play.txt")

// COMMAND ----------

var df = poem.flatMap(line => line.split(" ")).filter(word => word!="").map(word => (word,1)).reduceByKey(_+_)

// COMMAND ----------

df.saveAsTextFile("./word")

// COMMAND ----------

var file = spark.
