// Databricks notebook source
val file_path = "/dbfs/FileStore/lodgement_delta_extract_fixed_sample.xlsx"

// COMMAND ----------

import org.apache.spark.sql._
import com.crealytics.spark.excel._

// COMMAND ----------

val df = spark.read
    .format("excel") // Or .format("excel") for V2 implementation
    .option("dataAddress", "'My Sheet'!B3:C35") // Optional, default: "A1"
    .option("header", "true") // Required
    .option("treatEmptyValuesAsNulls", "false") // Optional, default: true
    .option("setErrorCellsToFallbackValues", "true") // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    .option("usePlainNumberFormat", "false") // Optional, default: false, If true, format the cells without rounding and scientific notations
    .option("inferSchema", "false") // Optional, default: false
    .option("addColorColumns", "true") // Optional, default: false
    .option("timestampFormat", "MM-dd-yyyy HH:mm:ss") // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    .option("maxRowsInMemory", 20) // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
    .option("excerptSize", 10) // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    .option("workbookPassword", "pass") // Optional, default None. Requires unlimited strength JCE for older JVMs
//     .schema(myCustomSchema) // Optional, default: Either inferred schema, or all columns are Strings
    .load(file_path)

// COMMAND ----------

val df = spark.read.excel(
    header = true,  // Required
//     dataAddress = "'My Sheet'!B3:C35", // Optional, default: "A1"
    treatEmptyValuesAsNulls = false,  // Optional, default: true
    setErrorCellsToFallbackValues = false, // Optional, default: false, where errors will be converted to null. If true, any ERROR cell values (e.g. #N/A) will be converted to the zero values of the column's data type.
    usePlainNumberFormat = false,  // Optional, default: false. If true, format the cells without rounding and scientific notations
    inferSchema = false,  // Optional, default: false
    addColorColumns = true,  // Optional, default: false
    timestampFormat = "MM-dd-yyyy HH:mm:ss",  // Optional, default: yyyy-mm-dd hh:mm:ss[.fffffffff]
    maxRowsInMemory = 20,  // Optional, default None. If set, uses a streaming reader which can help with big files (will fail if used with xls format files)
    excerptSize = 10,  // Optional, default: 10. If set and if schema inferred, number of rows to infer schema from
    workbookPassword = "pass"  // Optional, default None. Requires unlimited strength JCE for older JVMs
).load(file_path)

// COMMAND ----------

val df = spark.read.excel(header = true).load(file_path)

// COMMAND ----------

// MAGIC %python
// MAGIC %pip install openpyxl

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC from pandas import ExcelFile

// COMMAND ----------

// MAGIC %python
// MAGIC pdf = pd.ExcelFile("/FileStore/lodgement_delta_extract_fixed_sample.xlsx", engine="openpyxl")

// COMMAND ----------

/FileStore/lodgement_delta_extract_fixed_sample.csv

// COMMAND ----------

val df = spark.read.csv("/FileStore/lodgement_delta_extract_fixed_sample.csv", )

// COMMAND ----------

print(df)

// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType,StringType,StructType,StructField, DateType, TimestampType}
import org.apache.spark.sql.{Row, SparkSession}

// COMMAND ----------

val SCHEMA_characteristics = StructType(Array(
    StructField("characteristics",StringType,true)
))

val SCHEMA_orc = StructType(Array(
    StructField("cac",StringType,true),
    StructField("sfdc_b2c_accountuuid",StringType,true),
    StructField("cdbor_cidn",StringType,true),
    StructField("ban", StringType, true),
    StructField("sourcesystem", StringType, true),
    StructField("billingsystem", StringType, true),
    StructField("pkg_id", StringType, true),
    StructField("package_type", StringType, true),
    StructField("eligibility_status", StringType, true),
    StructField("migration_status", StringType, true),
    StructField("scheduled_migration_date", DateType, true),
    StructField("last_updated", TimestampType, true),
    StructField("service_id", StringType, true),
    StructField("service_type", StringType, true),
    StructField("srv_subtype", StringType, true),
    StructField("srv_ineligibility_reason_code", StringType, true),
    StructField("package_service_count", IntegerType, true),
    StructField("part_num", StringType, true),
    StructField("name", StringType, true),
    StructField("type", StringType, true),
    StructField("dac_mapping", StringType, true),
    StructField("mapped_product_name", StringType, true),
    StructField("product_code", StringType, true),
    StructField("action", StringType, true),
    StructField("id", StringType, true),
    StructField("relies_on", StringType, true),
    StructField("b2c_price", StringType, true),
    StructField("additional_comms_required", StringType, true),
    StructField("product_char", StringType, true),
    StructField("product_char_value", StringType, true),
    StructField("orderitem_char", StringType, true),
    StructField("orderitem_char_value", StringType, true),
    StructField("attributename", StringType, true),
    StructField("attributevalue", StringType, true),
    StructField("ocs_notif_pref", StringType, true),
    StructField("characteristics", SCHEMA_characteristics, true),
    StructField("bill_cycle", StringType, true),
    StructField("legacy_bc_date", DateType, true),
    StructField("sf_ad_date", DateType, true),
    StructField("bundle_products", StringType, true),
    StructField("serviceadborId", StringType, true),
    StructField("charge_cycle", StringType, true)
  ))

// COMMAND ----------

val df = spark.read.csv("/FileStore/lodgement_delta_extract_fixed_sample.csv", schema=SCHEMA_orc)

// COMMAND ----------

val df = spark.read.format("csv")
      .option("header", "true")
      .schema(SCHEMA_orc)
      .load("/FileStore/lodgement_delta_extract_fixed_sample.xlsx")

// COMMAND ----------

display(df)

// COMMAND ----------

// MAGIC %python
// MAGIC pdf = pd.ExcelFile("/FileStore/lodgement_delta_extract_fixed_sample.xlsx", engine="openpyxl")

// COMMAND ----------


