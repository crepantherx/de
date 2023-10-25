# Databricks notebook source
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

DF_races = spark.read.parquet(f"{processed_folder_path}/races")\
.withColumnRenamed("name", "race_name")\
.withColumnRenamed("race_timestamp", "race_date")

# COMMAND ----------

DF_circuits = spark.read.parquet(f"{processed_folder_path}/circuits")\
.withColumnRenamed("location", "circuit_location")

# COMMAND ----------

DF_drivers = spark.read.parquet(f"{processed_folder_path}/drivers")\
.withColumnRenamed("number", "driver_number")\
.withColumnRenamed("name", "driver_name")\
.withColumnRenamed("nationality", "driver_nationality")

# COMMAND ----------

DF_constructors = spark.read.parquet(f"{processed_folder_path}/constructors")\
.withColumnRenamed("name", "team")

# COMMAND ----------

DF_results = spark.read.parquet(f"{processed_folder_path}/results")\
.withColumnRenamed("time", "race_time")

# COMMAND ----------

DF_race_circuits = DF_races.join(DF_circuits, DF_races.circuit_id == DF_circuits.circuit_id, "inner")\
.select(DF_races.race_id, DF_races.race_year, DF_races.race_name, DF_races.race_date, DF_circuits.circuit_location)

# COMMAND ----------

DF_race_results = DF_results.join(DF_race_circuits, DF_results.race_id == DF_race_circuits.race_id)\
.join(DF_drivers, DF_results.driver_id == DF_drivers.driver_id)\
.join(DF_constructors, DF_results.constructor_id == DF_constructors.constructor_id)

# COMMAND ----------

race_results = DF_race_results.select("race_year", "race_name", "race_date", "circuit_location", "driver_name", "driver_number", "driver_nationality", "team", "grid", "fastest_lap", "race_time", "points", "position")\
.withColumn("created_date", current_timestamp())

# COMMAND ----------

# display(race_results.filter("race_year == 2020 and race_name == 'Abu Dhabi Grand Prix'").orderBy(race_results.points.desc()))

# COMMAND ----------

race_results.write.mode("overwrite").format("parquet").saveAsTable("presentation.race_results")
