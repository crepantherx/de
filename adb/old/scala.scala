// Databricks notebook source
val lgprime = BigInt("3434433423423432")

// COMMAND ----------

var age = 9

// COMMAND ----------

if (age>=6 && age<=10){
  println("hello")
} else {
  println("bye")
}

// COMMAND ----------

object ScalaTutorial {
  def main(args: Array[String]) {
    var i = 0
    while (i <= 10) {
      println(i)
      i += 1
    }
    for (i <- 1 to 10)
      println(i)
  }
}
ScalaTutorial

// COMMAND ----------

ScalaTutorial

// COMMAND ----------

for (i <-0 until randLetters.length)

// COMMAND ----------

elig_ban_df

// COMMAND ----------

SELECT 
	ban
	, SUM(mob) as mob_srv
	, SUM(internet) as internet_srv
	, SUM(voice) as voice_srv  
FROM
	(
		SELECT 
			ban
			, CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
			, CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
			, CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
		
		FROM (
			SELECT UNNEST(ARRAY['1-1ACC6', '1-1HZX1', '1-W35PL']) AS prod_id_srv
			, UNNEST(ARRAY['2000101252730', '2000101252731', '2000101272730']) AS ban
		)
	)
s
GROUP BY ban;


// COMMAND ----------

DEDPT

// COMMAND ----------

val elig_ban_df = spark.read.format("csv").option("header","true").load("/FileStore/data.csv")

// COMMAND ----------

print(elig_ban_df)

// COMMAND ----------

elig_ban_df.show()

// COMMAND ----------

// DBTITLE 1,Conf set
spark.conf.set("spark.sql.repl.eagerEval.enabled", true)

// COMMAND ----------

val df = elig_ban_df.toDF()

// COMMAND ----------

display(df)

// COMMAND ----------

df.printSchema()

// COMMAND ----------

df.createOrReplaceTempView("elig_ban")

// COMMAND ----------

spark.sql()

// COMMAND ----------

val elig_ban_summary = spark.sql("""

SELECT 
	ban
	, SUM(mob) as mob_srv
	, SUM(internet) as internet_srv
	, SUM(voice) as voice_srv  
FROM
	(
		SELECT 
			ban
			, CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
			, CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
			, CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
		
		FROM elig_ban
	)

GROUP BY ban;
""")

// COMMAND ----------

elig_ban_summary.createOrReplaceTempView("elig_ban_summary")

// COMMAND ----------

spark.sql("""
  
  SELECT *
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 THEN 'Y'
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Y'
        ELSE 'N'
      END AS elig_ban
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 THEN 'Postpaid' 
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Fixed'
        ELSE 'NA'
      END AS package_type
  FROM elig_ban_summary

""").show()

// COMMAND ----------



// COMMAND ----------

mob_srv>=1, internet_srv=0, voice_srv=0 & mob_srv>=0, internet_srv=1, voice_srv=1

// COMMAND ----------

val query = """
WITH cte AS (
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  )
GROUP BY ban
)
SELECT *
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 OR THEN 'Y'
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Y'
        ELSE 'N'
      END AS elig_ban
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 THEN 'Postpaid' 
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Fixed'
        ELSE 'NA'
      END AS package_type
  FROM cte
"""

spark.sql(query).show()

// COMMAND ----------

elig_ban_df.withColumn().show()

// COMMAND ----------

val query = f"
WITH cte AS (
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  )
GROUP BY ban
)
SELECT *
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 OR mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Y'
        ELSE 'N'
      END AS elig_ban
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 THEN 'Postpaid' 
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Fixed'
        ELSE 'NA'
      END AS package_type
  FROM cte
"

spark.sql(query).show()

// COMMAND ----------

elig_ban_df.show()

// COMMAND ----------

spark.sql().show()

// COMMAND ----------

val p = "Postpaid"


// COMMAND ----------

val r = p.formatted("""
WITH cte AS (
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  )
GROUP BY ban
)
SELECT *
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 OR mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Y'
        ELSE 'N'
      END AS elig_ban
    , CASE 
        WHEN mob_srv>=1 AND internet_srv=0 AND voice_srv=0 THEN '%s' 
        WHEN mob_srv=0 AND internet_srv=1 AND voice_srv=1 THEN 'Fixed'
        ELSE 'NA'
      END AS package_type
  FROM cte
""")

// COMMAND ----------

val cond = """
>=1,0,0,Y,Postpaid
0,1,0,N,NA
0,0,1,N,NA
>=1,1,1,N,NA
>=1,1,0,N,NA
>=1,0,1,N,NA
0,>=1,>=1,N,NA
0,1,1,Y,Fixed"""

// COMMAND ----------

import org.apache.spark.sql.{Dataset}

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

val frame = spark.read.option("inferSchema",true).csv(csvData).toDF("mob_srv","internet_srv","voice_srv","elig_ban","package_type")
frame.show()
frame.printSchema()

// COMMAND ----------



// COMMAND ----------

frame.createOrReplaceTempView("cond")

// COMMAND ----------

spark.sql("""

SELECT *
FROM cond

""").show()

// COMMAND ----------

val new_elig = spark.sql("""
with elig as (
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  )
GROUP BY ban
)
select *
  , case when internet_srv >= 1 then '>=1' else cast(internet_srv as string) end as new_internet_srv
  , case when mob_srv >= 1 then '>=1' else cast(mob_srv as string) end as new_mob_srv
  , case when voice_srv >= 1 then '>=1' else cast(voice_srv as string) end as new_voice_srv
from elig
""")

// COMMAND ----------

spark.sql("""
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
  , case when exists (select * from cond b where b.mob_srv)
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  ) a
GROUP BY ban
""").show()

// COMMAND ----------

spark.sql("""select * from cond""").show()

// COMMAND ----------

new_elig.createOrReplaceTemp

// COMMAND ----------

spark.sql("""
SELECT * 
FROM new_elig

""")

// COMMAND ----------

df.printSchema()

// COMMAND ----------

 df.withColumn("gender",when(col("gender").equalTo("M"),lit("Male"))
                           .when(col("gender").equalTo("F"),lit("Female"))
                           .otherwise(lit(""))).show()

// COMMAND ----------

import org.apache.spark.sql.functions.{col, lit, when}

// COMMAND ----------

df.withColumn("mob", when(col("prod_id_srv") === "1-W35PL", lit("1")).otherwise(0))
  .withColumn("internet", when(col("prod_id_srv") === "1-1ACC6", lit("1")).otherwise(0))
  .withColumn("voice", when(col("prod_id_srv") === "1-1HZX1", lit("1")).otherwise(0)).show()



// COMMAND ----------

spark.sql("""
SELECT ban
  , SUM(mob) as mob_srv
  , SUM(internet) as internet_srv
  , SUM(voice) as voice_srv  
  , case when exists (select * from cond b where b.mob_srv)
FROM (
      SELECT ban
          , CASE when prod_id_srv='1-W35PL' then 1 ELSE 0 END as mob
          , CASE when prod_id_srv='1-1ACC6' then 1 ELSE 0 END as internet
          , CASE when prod_id_srv='1-1HZX1' then 1 ELSE 0 END as voice 
      FROM elig_ban
  ) a
GROUP BY ban
""").show()
