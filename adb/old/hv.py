# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark     import SparkContext

sc    = SparkContext.getOrCreate()
spark = SparkSession.builder \
                    .master('Local[*]') \
                    .appName('Hive') \
                    .enableHiveSupport() \
                    .getOrCreate()


# COMMAND ----------

spark.sql("""
    CREATE DATABASE IF NOT EXISTS db;
""")

# COMMAND ----------

spark.sql("""
    CREATE TABLE IF NOT EXISTS db.course(
        course_id string, 
        ourse_name string, 
        author_name string, 
        no_of_reviews string
    )
""")

# COMMAND ----------

spark.sql("""
    INSERT INTO db.course 
    VALUES ('C1', 'Physics', 'crepantherx', 'Sudhir')
""")

# COMMAND ----------

spark.sql("""
    SELECT * FROM db.course
""").show()

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /user/hive/warehouse/
# MAGIC

# COMMAND ----------


