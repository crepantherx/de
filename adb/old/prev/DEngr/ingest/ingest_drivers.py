# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql.functions import col, concat, lit

# COMMAND ----------

SCHEMA_name = StructType([
    StructField("forename", StringType(), True)
    , StructField("surname", StringType(), True)
])

SCHEMA_drivers = StructType([
    StructField("driverId", IntegerType(), False)
    , StructField("driverRef", StringType(), True)
    , StructField("number", IntegerType(), True)
    , StructField("code", StringType(), True)
    , StructField("name", SCHEMA_name)
    , StructField("dob", DateType(), True)
    , StructField("nationality", StringType(), True)
    , StructField("url", StringType(), True)
    
])

# COMMAND ----------

DF_ORIGINAL_drivers = spark.read.json(f"{raw_folder_path}/drivers.json", schema=SCHEMA_drivers)

# COMMAND ----------

DF_drivers = DF_ORIGINAL_drivers.withColumn("data_souce", lit(v_data_source))\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("driverRef", "driver_ref")\
                                .withColumn("name", concat(col("name.forename"), lit(" "), col("name.surname")))\
                                .drop(col("url"))
DF_drivers = add_ingestion_date(DF_drivers)

# COMMAND ----------

DF_drivers.write.mode("overwrite").format("parquet").saveAsTable("processed.drivers")
