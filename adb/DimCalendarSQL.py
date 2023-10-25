# Databricks notebook source
from pyspark.sql.functions import (
    date_format, year, month, dayofmonth, dayofweek, weekofyear, 
    quarter, to_date, current_timestamp, date_add, date_sub, ceil, when, expr
)

# COMMAND ----------

current_date = to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))

# COMMAND ----------

df\
.withColumn("isoweekday", ((dayofweek(col("DayDate")+6)%7)+1)-1)\
.withColumn("isoweekday_2", (dayofweek(col("DayDate")+6)%7)+1)\
.withColumn("isoweekdate", date_add(col("DayDate"),5 - col("isoweekday_2")))\
.withColumn("isoweekcurrentdate", date_add(current_date,5 - col("isoweekday_2")))\
.withColumn("isoweekyear",weekofyear(col("isoweekdate")))\
.withColumn("Year", year(col("DayDate")))\
.withColumn("Month", month(col("DayDate")))\
.withColumn("DayofMonth",dayofmonth(col("DayDate")))\
.withColumn("MonthName",date_format(col("DayDate"), "MMMM"))\
.withColumn("DayName",date_format(col("DayDate"), "EEEE"))\
.withColumn("WeekEndDate",date_add(col("DayDate"), 7-col("isoweekday_2")))\
.withColumn("WeekofMonth",date_format(col("DayDate"), 'W'))\
.withColumn("FiscalYearByDate",when(month(col("DayDate")) >= 7 ,(year(col("DayDate")))+1).otherwise(year(col("DayDate"))))\
.withColumn("PGFiscalYear",when(month(col("WeekEndDate")) >= 7 ,(year(col("WeekEndDate")))+1).otherwise(year(col("WeekEndDate"))))\
.withColumn("FiscalMonthByDate",(month(col("DayDate")) + 5) % 12 + 1)\
.withColumn("PGFiscalMonth",(month(col("WeekEndDate")) + 5) % 12 + 1)\
.withColumn("FiscalQuarterByDate",(quarter(col("DayDate")) + 1) % 4 + 1)\
.withColumn("PGFiscalQuarter",(quarter(col("WeekEndDate")) + 1) % 4 + 1)\
.withColumn("FiscalWeekByDate",(col("isoweekyear") + 26) % 52 + 1 )\
.withColumn("PGFiscalWeek",(weekofyear(col("WeekEndDate"))+26)% 52 +1)\
.withColumn("DayOffset",-datediff(current_date,col("DayDate")))\
.withColumn("WeekOffset", ceil(-datediff(date_sub(current_date,7 - col("isoweekday_2")),date_sub(col("isoweekdate"),7 - col("isoweekday_2")))/7))\
.withColumn("CurrentFYPG",when(month(current_date) >= 7 ,(year(current_date))+1).otherwise(year(current_date)))\
.withColumn("FYOffset",col("FiscalYearByDate")-col("CurrentFYPG"))\
.withColumn("PGFYOffset",col("PGFiscalYear")-col("CurrentFYPG"))\
.display()

# COMMAND ----------

from pyspark.sql.functions import *
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
#explode, sequence, to_date

beginDate = '2018-01-01'
endDate = '2028-12-31'

(
  spark.sql(f"""
   
  select explode(sequence(to_date('{beginDate}'), to_date('{endDate}'), interval 1 day)) as DayDate
  """)
    .createOrReplaceTempView('dates')
)

# COMMAND ----------

df = spark.sql("select * from dates")

# COMMAND ----------

#from pyspark.sql.functions import date_format
#from pyspark.sql.functions import *
#iso_weekday = (dayofweek(my_date) + 5)%7 + 1
#iso_year= year(date_add(my_date, 4 - iso_weekday))
df = spark.sql("""select * from dates""")
#current_date = date_format(current_date(),"YYYY-mm-dd")
df = df.withColumn("isoweekday", ((dayofweek(col("DayDate")+6)%7)+1)-1)
df = df.withColumn("isoweekday_2", (dayofweek(col("DayDate")+6)%7)+1)
df = df.withColumn("isoweekdate", date_add(col("DayDate"),5 - col("isoweekday_2")))
df = df.withColumn("isoweekcurrentdate",date_add((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))),5 - col("isoweekday_2")))
df = df.withColumn("isoweekyear",weekofyear(col("isoweekdate")))
df = df.withColumn("Year", year(col("DayDate")))
df = df.withColumn("Month", month(col("DayDate")))
df = df.withColumn("DayofMonth",dayofmonth(col("DayDate")))
df = df.withColumn("MonthName",date_format(col("DayDate"), "MMMM"))
df = df.withColumn("DayName",date_format(col("DayDate"), "EEEE"))
df = df.withColumn("WeekEndDate",date_add(col("DayDate"), 7-col("isoweekday_2")))
df = df.withColumn("WeekofMonth",date_format(col("DayDate"), 'W'))
df = df.withColumn("FiscalYearByDate",when(month(col("DayDate")) >= 7 ,(year(col("DayDate")))+1)
                                  .otherwise(year(col("DayDate"))))
df = df.withColumn("PGFiscalYear",when(month(col("WeekEndDate")) >= 7 ,(year(col("WeekEndDate")))+1)
                                  .otherwise(year(col("WeekEndDate"))))
df = df.withColumn("FiscalMonthByDate",(month(col("DayDate")) + 5) % 12 + 1)
df = df.withColumn("PGFiscalMonth",(month(col("WeekEndDate")) + 5) % 12 + 1)
df = df.withColumn("FiscalQuarterByDate",(quarter(col("DayDate")) + 1) % 4 + 1)
df = df.withColumn("PGFiscalQuarter",(quarter(col("WeekEndDate")) + 1) % 4 + 1)
df = df.withColumn("FiscalWeekByDate",(col("isoweekyear") + 26) % 52 + 1 )
df = df.withColumn("PGFiscalWeek",(weekofyear(col("WeekEndDate"))+26)% 52 +1)
df = df.withColumn("DayOffset",-datediff((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))),col("DayDate")))
df = df.withColumn("WeekOffset", 
                   ceil(-datediff(
                   date_sub((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))),7 - col("isoweekday_2")),date_sub(col("isoweekdate"),7 - col("isoweekday_2")))/7))
df = df.withColumn("CurrentFYPG",when(month((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS')))) >= 7 ,(year((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))))+1))
                                  .otherwise(year((to_date(current_timestamp() -  expr('INTERVAL 6 HOURS'))))))
df = df.withColumn("FYOffset",col("FiscalYearByDate")-col("CurrentFYPG"))
df = df.withColumn("PGFYOffset",col("PGFiscalYear")-col("CurrentFYPG"))

#df = df.withColumn("TYLYLLY",when((col("WeekOffset") >= 0) ,'NA')
                   
                            #.otherwise(when((col("WeekOffset") < 0) & ((col("WeekOffset") >= -52) ,'TY'))
                            #.otherwise(when((col("WeekOffset") < -52) & ((col("WeekOffset") >= -104) ,'LY'))
                            #.otherwise(when((col("WeekOffset") < -104) & ((col("WeekOffset") >= -156) ,'LY'))
                            #.otherwise('NA')))))

#df.show(50)
                            
df.createOrReplaceTempView('DimCalendar')

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM DimCalendar

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT `DayDate`,
# MAGIC        `isoweekday` AS `DayofWeek`,
# MAGIC        `DayofMonth`,
# MAGIC        `WeekEndDate`,
# MAGIC        `Year`,
# MAGIC        `Month`,
# MAGIC        `MonthName`,
# MAGIC        `isoweekyear` AS `WeekofYear`,
# MAGIC        `WeekofMonth`,
# MAGIC        `DayName`,
# MAGIC        `PGFiscalYear`,
# MAGIC        `PGFiscalMonth`,
# MAGIC        `PGFiscalQuarter`,
# MAGIC        `PGFiscalWeek`,
# MAGIC        `FiscalYearByDate`,
# MAGIC        `FiscalMonthByDate`,
# MAGIC        `FiscalQuarterByDate`,
# MAGIC        `FiscalWeekByDate`,
# MAGIC        `DayOffset`,
# MAGIC        `WeekOffset`,
# MAGIC        `CurrentFYPG`,
# MAGIC        `PGFYOffset`,
# MAGIC        `FYOffset`,
# MAGIC        `TYLYLLY`,
# MAGIC        `LXW_YTDFilter`
# MAGIC FROM
# MAGIC (
# MAGIC   SELECT *,
# MAGIC   CASE WHEN `TYLYLLY` = 'NA' THEN 'NA'
# MAGIC      ELSE CONCAT(CAST(`TYLYStartingOffset` AS STRING),'|',CAST(`WeekOffset` AS STRING),'|',CAST(`PGFYOffset` AS STRING))
# MAGIC      END AS `LXW_YTDFilter`
# MAGIC   FROM
# MAGIC   (
# MAGIC     SELECT *,
# MAGIC     CASE WHEN `TYLYLLY` = 'TY' THEN -1
# MAGIC          WHEN `TYLYLLY` = 'LY' THEN -53
# MAGIC          WHEN `TYLYLLY` = 'LLY' THEN -105
# MAGIC          ELSE -999
# MAGIC          END AS `TYLYStartingOffset`
# MAGIC     FROM
# MAGIC     (
# MAGIC       SELECT *, 
# MAGIC       CASE WHEN `WeekOffset` >= 0 THEN 'NA'
# MAGIC            WHEN `WeekOffset` < 0 AND `WeekOffset` >= -52 THEN 'TY'
# MAGIC            WHEN `WeekOffset` < -52 AND `WeekOffset` >= -104 THEN 'LY'
# MAGIC            WHEN `WeekOffset` < -104 AND `WeekOffset` >= -156 THEN 'LLY'
# MAGIC            ELSE 'NA'
# MAGIC            END AS `TYLYLLY`
# MAGIC
# MAGIC       FROM DimCalendar 
# MAGIC      )A
# MAGIC   )B
# MAGIC )C

# COMMAND ----------


df1 = spark.sql("""SELECT `DayDate`,
       `isoweekday` AS `DayofWeek`,
       `DayofMonth`,
       `WeekEndDate`,
       `Year`,
       `Month`,
       `MonthName`,
       `isoweekyear` AS `WeekofYear`,
       `WeekofMonth`,
       `DayName`,
       `PGFiscalYear`,
       `PGFiscalMonth`,
       `PGFiscalQuarter`,
       `PGFiscalWeek`,
       `FiscalYearByDate`,
       `FiscalMonthByDate`,
       `FiscalQuarterByDate`,
       `FiscalWeekByDate`,
       `DayOffset`,
       `WeekOffset`,
       `CurrentFYPG`,
       `PGFYOffset`,
       `FYOffset`,
       `TYLYLLY`,
       `LXW_YTDFilter`
FROM
(
SELECT *,
CASE WHEN `TYLYLLY` = 'NA' THEN 'NA'
     ELSE CONCAT(CAST(`TYLYStartingOffset` AS STRING),'|',CAST(`WeekOffset` AS STRING),'|',CAST(`PGFYOffset` AS STRING))
     END AS `LXW_YTDFilter`
FROM
(
SELECT *,
CASE WHEN `TYLYLLY` = 'TY' THEN -1
     WHEN `TYLYLLY` = 'LY' THEN -53
     WHEN `TYLYLLY` = 'LLY' THEN -105
     ELSE -999
     END AS `TYLYStartingOffset`
FROM
(
SELECT *, 
CASE WHEN `WeekOffset` >= 0 THEN 'NA'
     WHEN `WeekOffset` < 0 AND `WeekOffset` >= -52 THEN 'TY'
     WHEN `WeekOffset` < -52 AND `WeekOffset` >= -104 THEN 'LY'
     WHEN `WeekOffset` < -104 AND `WeekOffset` >= -156 THEN 'LLY'
     ELSE 'NA'
     END AS `TYLYLLY`

FROM DimCalendar 
)A)B)C""")

# COMMAND ----------

# MAGIC %python
# MAGIC def create_sql_table( df1 ):
# MAGIC         
# MAGIC         try:
# MAGIC             
# MAGIC            # db_table = app_config['data_enrich']['dqc'][table]['output_sql_table']
# MAGIC             db_table = 'DimCalendar'
# MAGIC             jdbc_url = dbutils.secrets.get(scope='AZ-RG-CoreDataHub-PV-Launchpad', key="sql-connection").split(';')
# MAGIC
# MAGIC             jdbc_url = {i.split('=')[0] : i.split('=')[1].replace('"', "") for i in jdbc_url[:-1]}
# MAGIC
# MAGIC             server = jdbc_url['Server'].split(':')[1]
# MAGIC             database = jdbc_url['Initial Catalog']
# MAGIC             user = jdbc_url['User ID']
# MAGIC             password = jdbc_url['Password']
# MAGIC             url = "jdbc:sqlserver://" + server.split(',')[0] + ":" + server.split(',')[1] + ";DatabaseName=" + database
# MAGIC             
# MAGIC             # print(user, password)
# MAGIC             
# MAGIC             df1.write \
# MAGIC                   .format("jdbc") \
# MAGIC                   .option("url", url) \
# MAGIC                   .option("dbtable", db_table) \
# MAGIC                   .option("user", user) \
# MAGIC                   .option("password", password) \
# MAGIC                   .mode("overwrite") \
# MAGIC                   .save()
# MAGIC             
# MAGIC             #self.read_sql_table(url, db_table, user, password)
# MAGIC
# MAGIC             #logger.info("Successfully created SQL tables for the table {}.".format(table))
# MAGIC                 
# MAGIC         except Exception as e:
# MAGIC             
# MAGIC            # logger.error("Error while creating SQL table")
# MAGIC            # logging.shutdown()
# MAGIC             raise Exception(e)
# MAGIC

# COMMAND ----------

create_sql_table(df1)

# COMMAND ----------

#%python
#jdbc_url = dbutils.secrets.get(scope='AZ-RG-CoreDataHub-PV-Launchpad', key="sql-connection").split(';')
#
#jdbc_url = {i.split('=')[0] : i.split('=')[1].replace('"', "") for i in jdbc_url[:-1]}
#
#server = jdbc_url['Server'].split(':')[1]
#database = jdbc_url['Initial Catalog']
#user = jdbc_url['User ID']
#password = jdbc_url['Password']
#
#print(server)
#print(database)
#print(user)
#print(password)
#url = "jdbc:sqlserver://" + server.split(',')[0] + ":" + server.split(',')[1] + ";DatabaseName=" + database
#            
#
#retail_table = (spark.read
#              .format("jdbc")
#              .option("url", url)
#              .option("dbtable", "DimCalendar")
#              .option("user", user)
#              .option("password", password)
#              .load()
#            )    
#            
#display(retail_table)
