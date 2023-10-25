# Databricks notebook source
spark.conf.set(
    "fs.azure.account.key.crepantherxadlsg2.dfs.core.windows.net",
    "AYvMlAIJ9rR41kzrHHgENVWoBAnqd57bi0HyDvd00qeliYwS0TydZi+yVnNKCcGebticQ7jQa0SC+AStenjvKA=="
)

# COMMAND ----------

spark.read \
.format("avro") \
.load("wasbs://log-stream@crepantherxadlsg2.blob.core.windows.net/crepantherx-eh/eh-crepantherx/0/2022/05/04/09/17/20.avro")

# COMMAND ----------


