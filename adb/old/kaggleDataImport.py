# Databricks notebook source
!pip install -q kaggle

# COMMAND ----------

! mkdir ~/.kaggle

# COMMAND ----------

# MAGIC %fs
# MAGIC cp /FileStore/tables/kaggle.json ~/.kaggle/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls ~/.kaggle/
# MAGIC

# COMMAND ----------

!cp dbfs:~/FileStore/tables/kaggle.json ~/.kaggle/

# COMMAND ----------

!ls ~/.kaggle/

# COMMAND ----------

!mkdir ~/.kaggle

# COMMAND ----------

!chmod 600 ~/.kaggle/kaggle.json


# COMMAND ----------

!kaggle dataset list

# COMMAND ----------

from google.colab import files
files.upload()

# COMMAND ----------

import pyspark

# COMMAND ----------


