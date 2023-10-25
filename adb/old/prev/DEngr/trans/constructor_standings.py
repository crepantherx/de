# Databricks notebook source
# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

DF_constructor_standings = race_results\
.groupBy("race_year", "team")\
.agg(
    sum("points").alias("total_points"), 
    count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

wn = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
constructor_standings = DF_constructor_standings.withColumn("rank", rank().over(wn))

# COMMAND ----------

constructor_standings.write.mode("overwrite").format("parquet").saveAsTable("presentation.constructor_standings")

# COMMAND ----------


