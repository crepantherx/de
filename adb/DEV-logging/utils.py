# Databricks notebook source
import logging
import time

# COMMAND ----------

generated_at = time.strftime("%Y-%m-%d-%H-%M-%S", time.localtime())
file_name = f"/FileStore/logs/log-{generated_at}.log"
logger = logging.getLogger('Databricks Logging')
log_file_handler = logging.FileHandler(filename=file_name)
logger.addHandler(log_file_handler)
logger.setLevel(logging.DEBUG)

# COMMAND ----------

logger.info("hello, this should be working by now")
logger.critical("hello, this is critical")


# COMMAND ----------

#here

# COMMAND ----------

class Logs:
    
    def __init__(self):
        
        try:
            pass
        except Exception as e:
            
            raise Exception(e)
    
    def create_log_file(self):
        
        try:
            
            file_data = datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d-%H-%M-%S')
            dir = '/FileStore/logs/'
            file_name = 'log-' + file_data + '.log'
            log_file = dir + file_name

            logger = logging.getLogger('zfs_logs')
            logger.setLevel(logging.DEBUG)

            fh = logging.FileHandler(log_file, mode = 'a')

            ch = logging.StreamHandler()
            ch.setLevel(logging.DEBUG)

            formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

            fh.setFormatter(formatter)
            ch.setFormatter(formatter)

            if(logger.hasHandlers()):

                logger.handlers.clear()

            logger.addHandler(fh)
            logger.addHandler(ch)

        except Exception as e:

            raise Exception(e)
        
    def get_latest_file(self, path):
        
        files = os.listdir(path)
        paths = [os.path.join(path, basename) for basename in files]
        return max(paths, key=os.path.getctime)
        
    def get_logger(self):

        log_file = self.get_latest_file('/FileStore/logs/')


        logger = logging.getLogger('zfs_logs')
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(log_file, mode = 'a')

        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        if(logger.hasHandlers()):

            logger.handlers.clear()

        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger
    
    def shutdown(self):
        
        try:
            
            for i in logger.handlers:
    
                i.close()
        
        except Exception as e:
        
            raise Exception(e)

# COMMAND ----------

logs = Logs()
logs.create_log_file()

# COMMAND ----------

logger = logs.get_logger()

# COMMAND ----------

logger.info("This is an info message")
logger.warning("This is a warning message")
logger.error("This is an error message")

# COMMAND ----------

class Logs:
    
    def __init__(self):
        pass
    
    def create_log_file(self):
        generated_at = datetime.datetime.now().strftime('%Y-%m-%d-%H-%M-%S')
        log_file = f"/dbfs/FileStore/logs/log-{generated_at}.log"
        logging.basicConfig(
            filename=log_file,
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

    def get_latest_file(self, path):
        files = os.listdir(path)
        return max(files, key=os.path.getctime)

    def get_logger(self):
        log_file = f"/dbfs/FileStore/logs/{self.get_latest_file('/dbfs/FileStore/logs/')}"

        logger = logging.getLogger('zfs_logs')
        logger.setLevel(logging.DEBUG)

        fh = logging.FileHandler(log_file, mode = 'a')
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)

        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

        fh.setFormatter(formatter)
        ch.setFormatter(formatter)

        if(logger.hasHandlers()):
            logger.handlers.clear()

        logger.addHandler(fh)
        logger.addHandler(ch)

        return logger
    
    def shutdown(self):
        for handler in self.get_logger().handlers:
            handler.close()

# COMMAND ----------

logs.get_latest_file("/FileStore/logs/")

# COMMAND ----------

#minimised noimport logging


# COMMAND ----------

import logging
import os
import time

# COMMAND ----------

class Logs:
    
    def __init__(self):
        logging.basicConfig(
            level=logging.DEBUG,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger('crepanthex-databricks-application')
        self.logger.setLevel(logging.DEBUG)
        self.create_log_file()
        
    def create_log_file(self):
        file_data = time.strftime('%Y-%m-%d-%H-%M-%S', time.localtime())
        dir = '/FileStore/logs/'
        file_name = f'log-{file_data}.log'
        log_file = os.path.join(dir, file_name)
        fh = logging.FileHandler(log_file, mode='a')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        
    def get_latest_file(self, path):
        files = os.listdir(path)
        paths = [os.path.join(path, basename) for basename in files]
        return max(paths, key=os.path.getctime)
        
    def get_logger(self):
        log_file = self.get_latest_file('/FileStore/logs/')
        fh = logging.FileHandler(log_file, mode='a')
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        return self.logger
    
    def shutdown(self):
        for i in self.logger.handlers:
            i.close()


# COMMAND ----------

logs = Logs()

# COMMAND ----------

logs.create_log_file()

# COMMAND ----------


