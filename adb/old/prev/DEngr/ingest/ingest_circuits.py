# Databricks notebook source
dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "/DEngr/includes/configuration"

# COMMAND ----------

# MAGIC %run "DEngr/includes/common_functions"

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
from pyspark.sql.functions import col, lit

# COMMAND ----------

SCHEMA_circuits = StructType(
    [
        StructField("circuitId",  IntegerType(), False),
        StructField("circuitRef", StringType(),  True),
        StructField("name",       StringType(),  True),
        StructField("location",   StringType(),  True),
        StructField("country",    StringType(),  True),
        StructField("lat",        DoubleType(),  True),
        StructField("lng",        DoubleType(),  True),
        StructField("alt",        IntegerType(), True),
        StructField("url",        StringType(),  True),
    ]
)

# COMMAND ----------

DF_ORIGINAL_circuits = spark.read.csv(
    f"{raw_folder_path}/circuits.csv"
    , header=True
    , schema = SCHEMA_circuits
)

# COMMAND ----------

DF_circuits = DF_ORIGINAL_circuits.withColumn("data_source", lit(v_data_source))\
                                  .withColumnRenamed("lat", "latitude")\
                                  .withColumnRenamed("lng", "longitude")\
                                  .withColumnRenamed("alt", "altitude")\
                                  .withColumnRenamed("circuitId", "circuit_id")\
                                  .withColumnRenamed("circuitRef", "circuit_ref")\
                                  .drop(col("url"))
DF_circuits = add_ingestion_date(DF_circuits)

# COMMAND ----------

DF_circuits.write.mode("overwrite").format("parquet").saveAsTable("processed.circuits")

# COMMAND ----------

# df_v1 = df.select(df.columns[:-1])
# "column_name, ..."

# df.column_name, ....
# df["column_name"], ....
# col(column_name), ....
## above 3 will let you give alias to column as well
## df['col'].alias("dfdf")

# above 4 will work for column name

# df_v2 = df.select(
#     col('circuitId').alias('circuit_id'),
#     col('circuitRef').alias('circuit_ref'),
#     col('name'),
#     col('location'),
#     col('country'),
#     col('lat').alias('latitue'),
#     col('lng').alias('longitude'),
#     col('alt').alias('altitude'),
# )
# display(df_v2)

# df4 = df3\
# #          .withColumn("env", lit("Production"))
# display(df4)

# COMMAND ----------

display(spark.read.parquet(f"{processed_folder_path}/circuits"))

# COMMAND ----------


