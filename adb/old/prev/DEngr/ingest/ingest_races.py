# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import to_timestamp, concat, lit, col, current_timestamp

# COMMAND ----------

SCHEMA_races = StructType([
    StructField("raceId", IntegerType())
    , StructField("year", IntegerType())
    , StructField("round", IntegerType())
    , StructField("circuitId", IntegerType())
    , StructField("name", StringType())
    , StructField("date", StringType())
    , StructField("time", StringType())
    , StructField("url", StringType())
])

# COMMAND ----------

DF_ORIGINAL_races = spark.read.csv(f"{raw_folder_path}/races.csv", header=True, schema=SCHEMA_races)

# COMMAND ----------

DF_races = DF_ORIGINAL_races.withColumn("data_source", lit(v_data_source))\
                            .withColumn("race_timestamp", to_timestamp(concat(col("date"),lit(" "),col("time")), "yyyy-MM-dd HH:mm:ss"))\
                            .withColumn("ingestion_date", current_timestamp())\
                            .withColumnRenamed("raceId", "race_id")\
                            .withColumnRenamed("circuitId", "circuit_id")\
                            .withColumnRenamed("year", "race_year")\
                            .drop("url", "date", "time")
DF_races = add_ingestion_date(DF_races)

# COMMAND ----------

DF_races.write.mode("overwrite").format("parquet").partitionBy("race_year").saveAsTable("processed.races")
