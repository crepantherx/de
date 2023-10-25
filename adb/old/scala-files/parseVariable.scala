// Databricks notebook source
import org.apache.spark.sql.{Dataset}

// COMMAND ----------

import org.apache.spark.sql.{Dataset, SparkSession}
val spark = SparkSession.builder().appName("CsvExample").master("local").getOrCreate()

import spark.implicits._
val csvData: Dataset[String] = spark.sparkContext.parallelize(
  """
    |mob_srv,internet_srv,voice_srv,elig_ban,package_type
    |>=1, 0, 0, Y, Postpaid
    |0, >=1, >=1, N, NA
    |0, 1, 0, N, NA
    |0, 0, 1, N, NA
    |>=1, 1, 1, N, NA
    |>=1, 1, 0, N, NA
    |>=1, 0, 1, N, NA
    |0, 1, 1, Y, Fixed
""".stripMargin.lines.toList).toDS()

val frame = spark.read.option("header", true).option("inferSchema",true).csv(csvData)
frame.show()
frame.printSchema()

// COMMAND ----------

val elig_ban_condition = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

val PARSED_DATASET_elig_ban_cond: Dataset[String] = spark.sparkContext.parallelize(elig_ban_condition.stripMargin.lines.toList).toDS()
val COLUMN_NAMES_elig_ban_condition: List[String] = List("mob_srv","internet_srv","voice_srv","elig_ban","package_type")


val DF_elig_ban_condition = spark.read
.option("inferSchema",true)
.csv(PARSED_DATASET_elig_ban_cond)
.toDF(COLUMN_NAMES_elig_ban_condition:_*)

DF_elig_ban_condition.show()

// COMMAND ----------

val s = """
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

val l = List(
(">=1", "0", "0", "Y", "Postpaid"), 
("0", ">=1", ">=1", "N", "NA"), 
("0", "1", "0", "N", "NA"), 
("0", "0", "1", "N", "NA"), 
(">=1", "1", "1", "N", "NA"), 
(">=1", "1", "0", "N", "NA"), 
(">=1", "0", "1", "N", "NA"), 
("0", "1", "1", "Y", "Fixed")
)

// COMMAND ----------

Seq(l:_*).toDF("mob_srv", "internet_srv", "voice_srv", "elig_ban", "package_type").show

// COMMAND ----------

val l = List(
  (">=1", "0", "0", "Y", "Postpaid"), 
  ("0", ">=1", ">=1", "N", "NA"), 
  ("0", "1", "0", "N", "NA"), 
  ("0", "0", "1", "N", "NA"), 
  (">=1", "1", "1", "N", "NA"), 
  (">=1", "1", "0", "N", "NA"), 
  (">=1", "0", "1", "N", "NA"), 
  ("0", "1", "1", "Y", "Fixed")
)

// COMMAND ----------

s.split('\n').map(_.trim.filter(_ >= ' ')).mkString

// COMMAND ----------

s.split('\n').map(_.trim.filter(_ >= ' ')).mkString
List(s"$s")

// COMMAND ----------

val k = s.split('\n').map(_.trim.filter(_ >= ' ')).mkString

// COMMAND ----------

List(s"$k")

// COMMAND ----------

List(s"$s")

// COMMAND ----------

k.split("\\n").map(_.trim).toList

// COMMAND ----------

val elig_ban_condition = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""
spark.sparkContext.parallelize(elig_ban_condition.stripMargin.lines.toList)

// COMMAND ----------

elig_ban_condition.stripMargin.lines.toList

// COMMAND ----------

#Day 3

// COMMAND ----------

val s = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

// COMMAND ----------

val ms = s.trim().split("\n").map(_.trim).mkString("\n")

// COMMAND ----------

val mms = ms.split("\n").map(_.toString).toList

// COMMAND ----------

Seq(mms:_*)

// COMMAND ----------

mms.map(_.toList)

// COMMAND ----------

val l = List(
  (">=1", "0", "0", "Y", "Postpaid"), 
  ("0", ">=1", ">=1", "N", "NA"), 
  ("0", "1", "0", "N", "NA"), 
  ("0", "0", "1", "N", "NA"), 
  (">=1", "1", "1", "N", "NA"), 
  (">=1", "1", "0", "N", "NA"), 
  (">=1", "0", "1", "N", "NA"), 
  ("0", "1", "1", "Y", "Fixed"))

// COMMAND ----------

Seq(l:_*).toDF().show

// COMMAND ----------

val s = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

// COMMAND ----------

val s = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

val ss = s.trim().split("\n").map(_.trim).mkString("\n")

ss.split("\n").map(x=>x.split(","))

// COMMAND ----------

val ll: List[String] = List()

// COMMAND ----------

List(1,2,3) :+ 4

// COMMAND ----------

val fileLines = """
football type game
John comment "football is the best game"
"""
fileLines.map { line =>
  val lineSplit = line.split(" ")(lineSplit(0), lineSplit(1), lineSplit.drop(2).mkString(" "))
}

// COMMAND ----------

ss.split("\n").map(x=>x.split(","))

// COMMAND ----------

(List(1,2,3),List(4,5,6),List(7,8,9)).zipped.toList

// COMMAND ----------

Seq(lll:_*)

// COMMAND ----------

val k = """(">=1", "0", "0", "Y", "Postpaid"), 
  ("0", ">=1", ">=1", "N", "NA"), 
  ("0", "1", "0", "N", "NA"), 
  ("0", "0", "1", "N", "NA"), 
  (">=1", "1", "1", "N", "NA"), 
  (">=1", "1", "0", "N", "NA"), 
  (">=1", "0", "1", "N", "NA"), 
  ("0", "1", "1", "Y", "Fixed")"""

// COMMAND ----------

l

// COMMAND ----------

val list = List((1,2), (3,4), (5,6), (7,8), (9,0))

// COMMAND ----------

list(2).getClass

// COMMAND ----------

type(list(2))

// COMMAND ----------

val s = 
"""
    >=1,0,0,Y,Postpaid
    0,1,0,N,NA
    0,0,1,N,NA
    >=1,1,1,N,NA
    >=1,1,0,N,NA
    >=1,0,1,N,NA
    0,>=1,>=1,N,NA
    0,1,1,Y,Fixed
"""

val ss = s.trim().split("\n").map(_.trim).mkString("\n")

ss.split("\n").map(x=>x.split(","))
