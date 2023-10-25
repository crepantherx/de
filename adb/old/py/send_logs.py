# Databricks notebook source
partitions = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y/%m/%d/')
dbutils.fs.mv("file:" + finalpath, "dbfs:/mnt/storage-azure-blob/audit.log")
