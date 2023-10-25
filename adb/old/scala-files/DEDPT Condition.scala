// Databricks notebook source
import org.apache.spark.sql.functions.{col, lit, row_number, trim, udf}
import org.apache.spark.sql.functions._

// COMMAND ----------

def whenCondMethod(whenCond: String): org.apache.spark.sql.Column = {

  var condValue: org.apache.spark.sql.Column = null
  var cond = whenCond.split('|').toSeq
  val y = cond.head.split(':')
  if (y(1).toString.equalsIgnoreCase(y(2).toString)) condValue = when(expr(y(0)), lit(expr(y(1).toString))) else condValue = when(expr(y(0)), lit(y(1).toString))
  cond.tail.foreach { x =>
    val z = x.split(':')
    if (z(1).toString.equalsIgnoreCase(z(2).toString)) condValue = condValue.when(expr(z(0)), lit(expr(z(1).toString))) else condValue = condValue.when(expr(z(0)), lit(z(1).toString))
  }
  condValue = condValue.otherwise("NOCHANGE")
  condValue
}

// COMMAND ----------

val productTranslationDF = spark.sql("select * from dev_fixed_migration.test_pt")

// COMMAND ----------

val productTranslationDF = spark.read.format("csv").option("header","true").load("/FileStore/data_dev2.csv")

// COMMAND ----------

// Original Backup is at bottom
val productTranslationDF = spark.read.format("csv").option("header","true").load("/FileStore/data_dev2.csv")
val DF_elig_ban = productTranslationDF
.withColumn("mob", when(col("prod_id_srv") === "1-W35PL", 1).otherwise(0))
.withColumn("internet", when(col("prod_id_srv") === "1-1ACC6", 1).otherwise(0))
.withColumn("voice", when(col("prod_id_srv") === "1-1HZX1", 1).otherwise(0))
.groupBy("ban").agg(
  sum("mob").as("mob_srv")
  , sum("internet").as("internet_srv")
  , sum("voice").as("voice_srv")
)
.withColumn("new_mob_srv", when(col("mob_srv")>1, lit(">1")).when(col("mob_srv")===1, lit("1")).otherwise(col("mob_srv")))
.withColumn("new_internet_srv", when(col("internet_srv")>1, lit(">1")).when(col("internet_srv")===1, lit("1")).otherwise(col("internet_srv")))
.withColumn("new_voice_srv", when(col("voice_srv")>1, lit(">1")).when(col("voice_srv")===1, lit("1")).otherwise(col("voice_srv")))

// COMMAND ----------

val s1 = "mob_srv>=1 AND internet_srv=0 AND voice_srv=0:Y:elig_ban|mob_srv=0 AND internet_srv=1 AND voice_srv=0:N:elig_ban|mob_srv=0 AND internet_srv=0 AND voice_srv=1:N:elig_ban|mob_srv>=1 AND internet_srv=1 AND voice_srv=1:N:elig_ban|mob_srv>=1 AND internet_srv=1 AND voice_srv=0:N:elig_ban|mob_srv>=1 AND internet_srv=0 AND voice_srv=1:N:elig_ban|mob_srv=0 AND internet_srv>=1 AND voice_srv>=1:N:elig_ban|mob_srv=0 AND internet_srv=1 AND voice_srv=1:Y:elig_ban"

val s2 = "mob_srv>=1 AND internet_srv=0 AND voice_srv=0:Postpaid:package_type|mob_srv=0 AND internet_srv=1 AND voice_srv=0:NA:package_type|mob_srv=0 AND internet_srv=0 AND voice_srv=1:NA:package_type|mob_srv>=1 AND internet_srv=1 AND voice_srv=1:NA:package_type|mob_srv>=1 AND internet_srv=1 AND voice_srv=0:NA:package_type|mob_srv>=1 AND internet_srv=0 AND voice_srv=1:NA:package_type|mob_srv=0 AND internet_srv>=1 AND voice_srv>=1:NA:package_type|mob_srv=0 AND internet_srv=1 AND voice_srv=1:Fixed:package_type"

// COMMAND ----------

val DF_elig_ban = productTranslationDF
.withColumn("mob", when(col("prod_id_srv") === lit("1-W35PL"), 1).otherwise(0))
.withColumn("internet", when(col("prod_id_srv") === lit("1-1ACC6"), 1).otherwise(0))
.withColumn("voice", when(col("prod_id_srv") === lit("1-1HZX1"), 1).otherwise(0))
.groupBy("ban")
.agg(
  sum("mob").as("mob_srv")
  , sum("internet").as("internet_srv")
  , sum("voice").as("voice_srv")
)
.withColumn("elig_ban",whenCondMethod(s1))
.withColumn("package_type",whenCondMethod(s2))

// COMMAND ----------

DF_elig_ban.show

// COMMAND ----------


