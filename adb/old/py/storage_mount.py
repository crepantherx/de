# Databricks notebook source
# MAGIC %run ./logger

# COMMAND ----------

def unmount_storage():    
    try:
        logger.info('started unmounting aws-storage')
        dbutils.fs.unmount("/mnt/storage-aws-s3/")
        logger.info("unmounted AWS-S3")
    except Exception as e:
        logger.error(e)

    try:
        logger.info('started unmounting azure-storage')
        dbutils.fs.unmount("/mnt/storage-azure-blob/")
        logger.info("unmounted AWS-S3")
    except Exception as e:
        logger.error(e)
        
    print('Unmounted Successfully ...')

# COMMAND ----------

def mount_storage():
    try:
        logger.info('started mounting azure-storage')
        dbutils.fs.mount(
            source="wasbs://databricks@crepantherxsa.blob.core.windows.net"
            , mount_point="/mnt/storage-azure-blob/"
            , extra_configs={"fs.azure.account.key.crepantherxsa.blob.core.windows.net":"dxbVHvwVkSXps+QXIAiUCgIJiN60KZ0BiqJwHktC8AkDALGPiwwp0NhoEzaSVtuUxlGOlyPB6Tak+AStsezGCg=="}
        )
        logger.info('azure-storage mounted successfully')
    except Exception as e:
        logger.error(e)
        
    try:
        logger.info('started mounting aws-storage')
        dbutils.fs.mount(
            source = "s3a://AKIAWUSVQEFURW7XWIV6:MB5fWqJ+yATRcNngWkV5voECxUhp4nk+2UGiQYjh@crepantherx-db-employees"
            , mount_point = "/mnt/storage-aws-s3/"
        )
        logger.info('aws-storage mounted successfully')
    except Exception as e:
        logger.error(e)
        
    print('Mounted Successfully ...')

# COMMAND ----------

unmount_storage()

# COMMAND ----------

mount_storage()

# COMMAND ----------

display(dbutils.fs.mounts())

# COMMAND ----------

# MAGIC %run ./send_logs

# COMMAND ----------


