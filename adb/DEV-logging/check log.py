# Databricks notebook source
# MAGIC %fs ls dbfs:/FileStore/logs/

# COMMAND ----------

ls /FileStore/logs/

# COMMAND ----------

import logging

logger = logging.getLogger('audit')
logger.setLevel(logging.DEBUG)
fh = logging.FileHandler('/dbfs/FileStore/logs/l.log', mode = 'a')

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

if(logger.hasHandlers()):
    logger.handlers.clear()

logger.addHandler(fh)

# COMMAND ----------

ls /dbfs/

# COMMAND ----------


