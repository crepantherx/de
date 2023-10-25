# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from pyspark.sql.functions import current_timestamp, col, lit

# COMMAND ----------

SCHEMA_races = StructType([
    StructField('constructorId', IntegerType(), False)
    , StructField('driverId', IntegerType(), True)
    , StructField('fastestLap', StringType(), True)
    , StructField('fastestLapSpeed', FloatType(), True)
    , StructField('fastestLapTime', StringType(), True)
    , StructField('grid', IntegerType(), True)
    , StructField('laps', IntegerType(), True)
    , StructField('milliseconds', IntegerType(), True)
    , StructField('number', IntegerType(), True)
    , StructField('points', FloatType(), True)
    , StructField('position', IntegerType(), True)
    , StructField('positionOrder', IntegerType(), True)
    , StructField('positionText', StringType(), True)
    , StructField('raceId', IntegerType(), True)
    , StructField('rank', IntegerType(), True)
    , StructField('resultId', IntegerType(), True)
    , StructField('statusId', IntegerType(), True)
    , StructField('time', StringType(), True)
])

# COMMAND ----------

DF_ORIGINAL_results = spark.read.json(f"{raw_folder_path}/results.json", schema=SCHEMA_races)

# COMMAND ----------

DF_results = DF_ORIGINAL_results.withColumn("data_source", lit(v_data_source))\
                                .withColumnRenamed("resultId", "result_id")\
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("constructorId", "constructor_id")\
                                .withColumnRenamed("positionOrder", "position_order")\
                                .withColumnRenamed("positionText", "position_text")\
                                .withColumnRenamed("fastestLap", "fastest_lap")\
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                .drop(col("statusId"))
DF_results = add_ingestion_date(DF_results)

# COMMAND ----------

DF_results.write.mode("overwrite").format("parquet").partitionBy("race_id").saveAsTable("processed.results")

# COMMAND ----------

display(DF_results)

# COMMAND ----------


