# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from pyspark.sql.functions import current_timestamp, lit, col

# COMMAND ----------

SCHEMA_requirements = StructType([ 
      
      StructField("qualifyId"    , IntegerType(), False)
    , StructField("raceId"       , IntegerType(), True)
    , StructField("driverId"     , IntegerType(), True)
    , StructField("constructorId", IntegerType(), True)
    , StructField("number"       , IntegerType(), True)
    , StructField("position"     , IntegerType(), True)
    , StructField("q1"           , StringType(), True)
    , StructField("q2"           , StringType(), True)
    , StructField("q3"           , StringType(), True)
    
])

# COMMAND ----------

DF_ORIGINAL_requirements = spark.read.json(f"{raw_folder_path}/qualifying", multiLine=True, schema=SCHEMA_requirements)

# COMMAND ----------

DF_requirements = DF_ORIGINAL_requirements.withColumn("data_source", lit(v_data_source))\
                                          .withColumnRenamed("driverId", "driver_id") \
                                          .withColumnRenamed("raceId", "race_id") \
                                          .withColumnRenamed("qualifyId", "qualify_id") \
                                          .withColumnRenamed("constructorId", "constructor_id")          
DF_requirements = add_ingestion_date(DF_requirements)

# COMMAND ----------

DF_requirements.write.mode("overwrite").format("parquet").saveAsTable("processed.qualifying")
