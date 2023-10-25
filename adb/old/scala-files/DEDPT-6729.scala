// Databricks notebook source
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset}

spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

// COMMAND ----------

val elig_ban_df = spark.read.format("csv").option("header","true").load("/FileStore/data.csv")

// COMMAND ----------

val DF_elig_ban = elig_ban_df
.withColumn("mob", when(col("prod_id_srv") === "1-W35PL", 1).otherwise(0))
.withColumn("internet", when(col("prod_id_srv") === "1-1ACC6", 1).otherwise(0))
.withColumn("voice", when(col("prod_id_srv") === "1-1HZX1", 1).otherwise(0))
.groupBy("ban").agg(
  sum("mob").as("mob_srv")
  , sum("internet").as("internet_srv")
  , sum("voice").as("voice_srv")
)
.withColumn("new_mob_srv", when(col("mob_srv")>=1, lit(">=1")).otherwise(col("mob_srv")))
.withColumn("new_internet_srv", when(col("internet_srv")>=1, lit(">=1")).otherwise(col("internet_srv")))
.withColumn("new_voice_srv", when(col("voice_srv")>=1, lit(">=1")).otherwise(col("voice_srv")))

// COMMAND ----------

val csvData: Dataset[String] = spark.sparkContext.parallelize(
  """
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
  """.stripMargin.lines.toList).toDS()

val DF_cond = spark.read.option("inferSchema",true).csv(csvData).toDF("mob_srv","internet_srv","voice_srv","elig_ban","package_type")
DF_cond.show()

// COMMAND ----------

DF_elig_ban_dev.join(
    DF_cond,
    DF_elig_ban_dev("new_mob_srv") === DF_condition("mob_srv") && DF_elig_ban_dev("new_internet_srv") === DF_condition("internet_srv") && DF_elig_ban_dev("new_voice_srv") === DF_condition("voice_srv"),
    "left"
).show

// COMMAND ----------

val CONSTRANT = """
(">=1", "0", "0", "Y", "Postpaid"),
("0", ">=1", ">=1", "N", "NA"),
("0", "1", "0", "N", "NA"),
("0", "0", "1", "N", "NA"),
(">=1", "1", "1", "N", "NA"),
(">=1", "1", "0", "N", "NA"),
(">=1", "0", "1", "N", "NA"),
("0", "1", "1", "Y", "Fixed")
"""


// COMMAND ----------

DF_elig_ban_dev.show

// COMMAND ----------



// COMMAND ----------

DF_cond.filter($"mob_srv" === lit(">=1")).show

// COMMAND ----------

val DF_condition = CONSTRANT.split(",").toSeq.transpose.toDF("mob_srv", "internet_srv", "voice_srv", "elig_ban", "package_type").show

// COMMAND ----------

DF_condition.show

// COMMAND ----------

DF_elig_ban_dev.alias("a").join(
    DF_condition.alias("b"),
    DF_elig_ban_dev("new_mob_srv") === DF_condition("mob_srv") && DF_elig_ban_dev("new_internet_srv") === DF_condition("internet_srv") && DF_elig_ban_dev("new_voice_srv") === DF_condition("voice_srv"),
    "left"
).select("a.mob_srv", "a.internet_srv", "a.voice_srv", "b.elig_ban", "b.package_type").show

// COMMAND ----------

DF_condition.columns

// COMMAND ----------



// COMMAND ----------



