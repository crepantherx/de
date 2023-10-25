// Databricks notebook source
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions._

// COMMAND ----------

val s = 
"""
    >1,0,0,Y,Postpaid
    1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >1,1,1,N,NA
    1,1,1,N,NA
    >1,1,0,N,NA
    1,1,0,N,NA
    >1,0,1,N,NA
    1,0,1,N,NA
    0,>1,>1,N,NA
    0,1,1,N,NA
    0,1,1,Y,Fixed
"""

val l = s
.trim()
.split("\n")
.map(_.trim)
.mkString("\n")
.split("\n")
.map(x=>x.split(","))
.map(x=>(x(0),x(1),x(2),x(3),x(4)))
.toList
val elig_ban_cond_col_names = List("mob_srv", "internet_srv", "voice_srv", "elig_ban", "package_type")
val DF_elig_ban_condition = Seq(l:_*).toDF(elig_ban_cond_col_names:_*).show

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

DF_elig_ban.alias("a").join(
    DF_elig_ban_condition.alias("b")
    , DF_elig_ban("new_mob_srv") === DF_elig_ban_condition("mob_srv") && 
      DF_elig_ban("new_internet_srv") === DF_elig_ban_condition("internet_srv") && 
      DF_elig_ban("new_voice_srv") === DF_elig_ban_condition("voice_srv")
    , "left"
).select("a.ban","a.mob_srv", "a.internet_srv", "a.voice_srv", "b.elig_ban", "b.package_type").show


// COMMAND ----------

// Backup - Don't run it

val DF_elig_ban = productTranslationDF
.withColumn("mob", when(col("prod_id_srv") === "1-W35PL", 1).otherwise(0))
.withColumn("internet", when(col("prod_id_srv") === "1-1ACC6", 1).otherwise(0))
.withColumn("voice", when(col("prod_id_srv") === "1-1HZX1", 1).otherwise(0))
.groupBy("ban").agg(
  sum("mob").as("mob_srv")
  , sum("internet").as("internet_srv")
  , sum("voice").as("voice_srv")
)
.withColumn("new_mob_srv", when(col("mob_srv")>1, lit(">=1")).when(col("mob_srv")===1, lit("1")).otherwise(col("mob_srv")))
.withColumn("new_internet_srv", when(col("internet_srv")>1, lit(">=1")).when(col("internet_srv")===1, lit("1")).otherwise(col("internet_srv")))
.withColumn("new_voice_srv", when(col("voice_srv")>1, lit(">=1")).when(col("voice_srv")===1, lit("1")).otherwise(col("voice_srv")))

// COMMAND ----------

val sqlContext = new org.apache.spark.sql.SQLContext(sc)
import sqlContext.implicits._

// COMMAND ----------

val productTranslationDF = spark.read.format("csv").option("header","true").load("/FileStore/data_dev2.csv")

// COMMAND ----------

display(productTranslationDF)

// COMMAND ----------


