# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder\
                    .master("Local[*]")\
                    .appName("Spark Simplified")\
                    .getOrCreate()

sc = spark.sparkContext

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

# abfs[s]1://crepantherxfs@crepantherxdlsg2.dfs.core.windows.net/employees.csv

# COMMAND ----------

configs = {"fs.azure.account.auth.type": "OAuth",
           "fs.azure.account.oauth.provider.type": 
           "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
           "fs.azure.account.oauth2.client.id": "<your-service-client-id>",
           "fs.azure.account.oauth2.client.secret": "crepantherx",
           "fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/<your-directory-id>/oauth2/token"}

# COMMAND ----------

df = spark.read.load('abfss://crepantherxfs@crepantherxdlsg2.dfs.core.windows.net/employees.csv', format='csv')
display(df.limit(10))

# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.crepantherxdlsg2.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))

# COMMAND ----------



# COMMAND ----------

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.dfs.core.windows.net",
    dbutils.secrets.get(scope="<scope-name>",key="<storage-account-access-key-name>"))

# COMMAND ----------



# COMMAND ----------

val SCHEMA_characteristics = StructType(Array(
    StructField("characteristics",StringType,true)
))

val simpleSchema = StructType(Array(
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

# COMMAND ----------

cac                     string
sfdc_b2c_accountuuid    string
cdbor_cidn              string
ban                     string
sourcesystem            string
billingsystem           string
pkg_id                  string
package_type            string
eligibility_status      string
migration_status        string
scheduled_migration_date        date
last_updated            timestamp
service_id              string
service_type            string
srv_subtype             string
srv_ineligibility_reason_code   string
package_service_count   int
part_num                string
name                    string
type                    string
dac_mapping             string
mapped_product_name     string
product_code            string
action                  string
id                      string
relies_on               string
b2c_price               string
additional_comms_required       string
product_char            string
product_char_value      string
orderitem_char          string
orderitem_char_value    string
attributename           string
attributevalue          string
ocs_notif_pref          string
characteristics         struct<characteristics:string>
bill_cycle              string
legacy_bc_date          date
sf_ad_date              date
charge_model            string
