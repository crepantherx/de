# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

SCHEMA_lap_times = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("lap", IntegerType(), True),
    StructField("position", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

DF_ORIGINAL_lap_times = spark.read.csv(f"{raw_folder_path}/lap_times/", schema=SCHEMA_lap_times)
# DF_ORIGINAL_lap_times = spark.read.csv("/mnt/raw/lap_times/lap_times_split*.csv", schema=SCHEMA_lap_times)

# COMMAND ----------

DF_lap_times = DF_ORIGINAL_lap_times\
                    .withColumn("data_source", lit(v_data_source))\
                    .withColumnRenamed("raceId", "race_id")\
                    .withColumnRenamed("driverId", "driver_id")
DF_lap_times = add_ingestion_date(DF_lap_times)

# COMMAND ----------

DF_lap_times.write.mode("overwrite").format("parquet").saveAsTable("processed.lap_times")

# COMMAND ----------


