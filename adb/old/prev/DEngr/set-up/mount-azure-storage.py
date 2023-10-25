# Databricks notebook source
account   = "crepantherxstorageacc"
container = "raw"

# COMMAND ----------

# dbutils.fs.unmount("/mnt/")

# COMMAND ----------

def mount_storage(account, container):
    """ADLS is being mounted"""
    key       = "1V0Po37VpHjAzGoMuNmlHpsdkZw4tpb/phqlS2DfOSJadYCnoJr8QLRh+wkfJ+s3b6SghIBD1BGs+AStTqpH7Q=="
    path      = f"/mnt/{container}/"
    url       = f"wasbs://{container}@{account}.blob.core.windows.net"
    config    = {'fs.azure.account.key.' + account + '.blob.core.windows.net': key}
    
    dbutils.fs.mount(source=url, mount_point=path, extra_configs=config)
    

# COMMAND ----------

mount_storage(
    account="crepantherxstorageacc"
    , container="processed"
)

# COMMAND ----------

mount_storage(
    account="crepantherxstorageacc"
    , container="raw"
)

# COMMAND ----------

mount_storage(
    account="crepantherxstorageacc"
    , container="presentation"
)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls
# MAGIC

# COMMAND ----------


