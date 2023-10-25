# Databricks notebook source
dbutils.notebook.run("ingest_circuits", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_races", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_constructors", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_drivers", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_results", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_circuits", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_pit_stops", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_lap_times", 0, {'p_data_source': 'Ergast API'})

# COMMAND ----------

dbutils.notebook.run("ingest_requirements", 0, {'p_data_source': 'Ergast API'})
