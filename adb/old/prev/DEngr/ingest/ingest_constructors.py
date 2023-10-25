# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "/DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

SCHEMA_constructors = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

# COMMAND ----------

DF_ORIGINAL_constructors = spark.read.json(f"{raw_folder_path}/constructors.json", schema=SCHEMA_constructors)

# COMMAND ----------

DF_constructors = DF_ORIGINAL_constructors.withColumn("data_source", lit(v_data_source))\
                                          .withColumnRenamed("constructorId", "constructor_id")\
                                          .withColumnRenamed("constructorRef", "constructor_ref")\
                                          .drop("url")
DF_constructors = add_ingestion_date(DF_constructors)

# COMMAND ----------

DF_constructors.write.mode("overwrite").format("parquet").saveAsTable("processed.constructors")
