# Databricks notebook source
import pyspark
spark = pyspark.sql.SparkSession.builder.appName("Read Excel File").getOrCreate()

# COMMAND ----------

"/dbfs/FileStore/pest_control_weekly_all_weeks_master_v3__1_.xlsm"

# COMMAND ----------

# MAGIC %sh ls /tmp/excel/

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

import pyspark.pandas as pd
import openpyxl

# COMMAND ----------

df = pd.read_excel(
    "/FileStore/pest_control_weekly_all_weeks_master_v3__1_.xlsm",
    engine="openpyxl",
    dtype='str',
    sheet_name=None
)

# COMMAND ----------

df.head(10)

# COMMAND ----------

df.sheet_names

# COMMAND ----------

!pip install openpyxl

# COMMAND ----------

import pandas as pd
import openpyxl
excel_file_path = '/tmp/excel/source.xlsm'

# Split the Excel file into separate files for each sheet
split_file_paths = []

with pd.ExcelFile(excel_file_path) as xl_file:
    for sheet_name in xl_file.sheet_names:
        sheet_df = xl_file.parse(sheet_name)
        sheet_file_path = f'/tmp/excel/{sheet_name}.xlsx'  # Provide a directory to store split files
        sheet_df.to_excel(sheet_file_path, index=False)
        split_file_paths.append(sheet_file_path)

# Read each split file as a PySpark DataFrame
spark_dfs = {}

for file_path in split_file_paths:
    sheet_name = file_path.split('/')[-1].split('.')[0]
    df = spark.read.format("com.crealytics.spark.excel").option("header", "true").load(file_path)
    spark_dfs[sheet_name] = df

# COMMAND ----------

ls /tmp/driver-env.sh

# COMMAND ----------

# MAGIC %fs ls /tmp/excel/

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/pest_control_weekly_all_weeks_master_v3__1_.xlsm", "file:/tmp/excel/source.xlsm")

# COMMAND ----------

(
    spark.read.format("com.crealytics.spark.excel")
    .option("dataAddress", "MAIN!")
    .option("Header", "true")
    .load("/FileStore/pest_control_weekly_all_weeks_master_v3__1_.xlsm")
).display()

# COMMAND ----------

sheet_names = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "DG!").option("header", "true").load("/FileStore/pest_control_weekly_all_weeks_master_v3__1_.xlsm").sheetNames()
sheet_names
# spark_dfs = {}

# for sheet_name in sheet_names:
#     df = spark.read.format("com.crealytics.spark.excel").option("dataAddress", "A1").option("header", "true").option("sheetName", sheet_name).load(excel_file_path)
#     spark_dfs[sheet_name] = df

# COMMAND ----------

(
    spark.read
    .format("com.crealytics.spark.excel")
    .option("dataAddress", "TEST!")
    .option("Header", "false")
    .load("/FileStore/ordersandshipment_testcases-1.xlsx")
    
).display()

# COMMAND ----------

spark.read.excel()

# COMMAND ----------

pdf.display()

# COMMAND ----------

spark.jars.packages = com.crealytics:spark-excel_2.11:0.12.2

spark-shell -cp com.crealytics.spark.excel_2.11-0.12.2.jar


# COMMAND ----------


