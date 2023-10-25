# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("Local[*]").getOrCreate()
sc = spark.sparkContext
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
df = spark.read.csv("/FileStore/tables/train.csv", header=True, inferSchema=True)
df.createOrReplaceTempView("db")
spark.sql("select * from db")
test_file = sc.textFile("/FileStore/tables/test.txt")


wordCount = sc.textFile("/FileStore/tables/test.txt").flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda k1,k2: k1+k2).cache().show()

sentenceCount = sc.textFile("/FileStore/tables/test.txt").flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda k1,k2: k1+k2).show(10)


# COMMAND ----------

df.show(5)

# COMMAND ----------

df.filter(df.Ticket != "113803").select("Name","Pclass","Sex", "Age").groupBy("Pclass").agg({"Age":"avg"}).orderBy("Pclass", ascending=False).limit(5)

# COMMAND ----------

from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

def greetPassenger(name):
    return "Hi " + name

# COMMAND ----------

fn = udf(greetPassenger, StringType())

# COMMAND ----------

df.select(fn(df.Name).alias("greet")).write.parquet("hello.parquet")

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

schema = StructType([
    StructField("name", StringType(), True),
    StructField("Age", IntegerType(), True),
    StructField("Sex", StringType(), True)
])

# COMMAND ----------

story = spark.read.csv("text.txt", schema= schema)

rdd_df = createDataFrame(rdd, schema)
