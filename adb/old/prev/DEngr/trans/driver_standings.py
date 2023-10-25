# Databricks notebook source
# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

from pyspark.sql.functions import col, when, count, sum, desc, rank
from pyspark.sql.window import Window

# COMMAND ----------

race_results = spark.read.parquet(f"{presentation_folder_path}/race_results")

# COMMAND ----------

DF_driver_standings = race_results\
.groupBy("race_year", "driver_name", "driver_nationality", "team")\
.agg(
    sum("points").alias("total_points"), 
    count(when(col("position") == 1, True)).alias("wins")
)

# COMMAND ----------

wn = Window.partitionBy("race_year").orderBy(desc("total_points"), desc("wins"))
driver_standings = DF_driver_standings.withColumn("rank", rank().over(wn))

# COMMAND ----------

driver_standings.write.mode("overwrite").format("parquet").saveAsTable("presentation.driver_standings")

# COMMAND ----------

display(spark.read.parquet(f"{presentation_folder_path}/driver_standings"))
