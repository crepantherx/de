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

SCHEMA_pit_stops = StructType([
    StructField("raceId", IntegerType(), False),
    StructField("driverId", IntegerType(), True),
    StructField("stop", StringType(), True),
    StructField("lap", IntegerType(), True),
    StructField("time", StringType(), True),
    StructField("duration", StringType(), True),
    StructField("milliseconds", IntegerType(), True)
])

# COMMAND ----------

DF_ORIGINAL_pit_stops = spark.read.json(f"{raw_folder_path}/pit_stops.json", schema=SCHEMA_pit_stops, multiLine=True)

# COMMAND ----------

DF_pit_stops = DF_ORIGINAL_pit_stops\
                    .withColumn("data_source", lit(v_data_source))\
                    .withColumnRenamed("raceId", "race_id")\
                    .withColumnRenamed("driverId", "driver_id")\
                    .withColumn("ingestion_date", current_timestamp())
DF_pit_stops = add_ingestion_date(DF_pit_stops)

# COMMAND ----------

DF_pit_stops.write.mode("overwrite").format("parquet").saveAsTable("processed.pit_stops")
