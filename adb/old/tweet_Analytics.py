# Databricks notebook source
# Stream data into Azure Databricks using Event Hubs

# you connect a data ingestion system with Azure Databricks to stream data into an Apache Spark cluster in near real-time. You set up data ingestion system using Azure Event Hubs and then connect it to Azure Databricks to process the messages coming through. To access a stream of data, you use Twitter APIs to ingest tweets into Event Hubs. Once you have the data in Azure Databricks, you can run analytical jobs to further analyze the data.

# By the end of this tutorial, you would have streamed tweets from Twitter (that have the term "Azure" in them) and read the tweets in Azure Databricks.

# Create an Azure Databricks workspace
# Create a Spark cluster in Azure Databricks
# Create a Twitter app to access streaming data
# Create notebooks in Azure Databricks
# Attach libraries for Event Hubs and Twitter API
# Send tweets to Event Hubs
# Read tweets from Event Hubs


# COMMAND ----------

# SendTweetsToEventHub - A producer notebook you use to get tweets from Twitter and stream them to Event Hubs.
# ReadTweetsFromEventHub - A consumer notebook you use to read the tweets from Event Hubs.
