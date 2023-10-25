# Databricks notebook source
# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator

# Create a Spark session
spark = SparkSession.builder.appName("TitanicML").getOrCreate()

# Load the Titanic dataset
train_path = "/FileStore/train.csv"
test_path = "/FileStore/test.csv"
train_data = spark.read.csv(train_path, header=True, inferSchema=True)
test_data = spark.read.csv(test_path, header=True, inferSchema=True)

# Prepare the dataset for machine learning
indexer = StringIndexer(inputCols=["Sex", "Embarked"], outputCols=["SexIndex", "EmbarkedIndex"])
assembler = VectorAssembler(inputCols=["Pclass", "SexIndex", "Age", "SibSp", "Parch", "Fare", "EmbarkedIndex"],
                            outputCol="features")

# Transform the training data
train_data = indexer.fit(train_data).transform(train_data)
train_data = assembler.transform(train_data)
train_data = train_data.select("Survived", "features")

# Transform the testing data
test_data = indexer.fit(test_data).transform(test_data)
test_data = assembler.transform(test_data)
test_data = test_data.select("PassengerId", "features")

# Build and train the classifier
rf = RandomForestClassifier(labelCol="Survived", featuresCol="features", numTrees=100)
model = rf.fit(train_data)

# Make predictions on the testing data
predictions = model.transform(test_data)
predictions = predictions.select("PassengerId", "prediction")

# Evaluate the predictions
evaluator = BinaryClassificationEvaluator(labelCol="Survived")
accuracy = evaluator.evaluate(predictions)

# Show the accuracy
print("Accuracy:", accuracy)


# COMMAND ----------


