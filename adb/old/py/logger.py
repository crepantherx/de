# Databricks notebook source
from datetime import datetime
from logging import config
import os
import logging
import pytz

# COMMAND ----------

current_dt = datetime.now(pytz.timezone('Asia/Kolkata')).strftime('%Y%m%d_%H%M%S')
directory = "/tmp/"
logfilename = "storage-mount" + current_dt + ".log"
finalpath = directory + logfilename

# COMMAND ----------

config.fileConfig("/tmp/audit.conf", disable_existing_loggers=False, defaults={ 'logfilename' : finalpath } )
logger = logging.getLogger("root")

# COMMAND ----------

dbutils.fs.cp("dbfs:/FileStore/audit.conf", "file:/tmp/audit.conf")

# COMMAND ----------

ls

# COMMAND ----------

dbutils.fs.cp("file:/tmp/audit.conf", "dbfs:/mnt/storage-azure-blob/audit.")
