# Databricks notebook source
pip install azure.identity

# COMMAND ----------

import os
import logging
import datetime
from typing import Optional

from azure.identity import ClientSecretCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.core.exceptions import HttpResponseError
from logging.config import dictConfig

# COMMAND ----------

class AzureLogAnalyticsHandler(logging.Handler):
    def __init__(self, dce_endpoint, dcr_immutableid, stream_name, tenant_id, client_id, client_secret):
        super(AzureLogAnalyticsHandler, self).__init__()
        self.dce_endpoint = dce_endpoint
        self.dcr_immutableid = dcr_immutableid
        self.stream_name = stream_name
        self.credentials = ClientSecretCredential(
            tenant_id=tenant_id,
            client_id=client_id,
            client_secret=client_secret
        )
        self.client = LogsIngestionClient(endpoint=self.dce_endpoint, credential=self.credentials, logging_enable=True)

    def emit(self, record):
        try:
            record_dict = record.__dict__
            record_json = [
                {
                "TimeGenerated": datetime.datetime.utcnow().isoformat(),
                "Level": record_dict['levelname'],
                "Message": record_dict['msg']
                }
            ]
            self.client.upload(rule_id=self.dcr_immutableid, stream_name=self.stream_name, logs=record_json)
        except Exception as e:
            self.handleError(record)

# COMMAND ----------

dce_endpoint = "https://log-ingestion-sx8p.eastus-1.ingest.monitor.azure.com"
dcr_immutableid = "dcr-ee2f228698e24a55a20d270415cadb05"
stream_name = "Custom-adb_CL"
tenant_id="e714ef31-faab-41d2-9f1e-e6df4af16ab8"
client_id="9604ec29-b555-4b24-b7c3-7874545942b5"
client_secret="Me28Q~d2fFPhGVr_EJ8OIUOJxbCNDZlcVumC4bbP"

# COMMAND ----------

LOGGING = {
    'version': 1,
    'handlers': {
        'azure_log_analytics_handler': {
            'class': '__main__.AzureLogAnalyticsHandler',
            'dce_endpoint': dce_endpoint,
            'dcr_immutableid': dcr_immutableid,
            'stream_name': stream_name,
            'tenant_id': tenant_id,
            'client_id': client_id,
            'client_secret': client_secret,
            'level': 'DEBUG'
        }
    },
    'loggers': {
        __name__: {
            'handlers': ['azure_log_analytics_handler'],
            'level': 'DEBUG'
        }
    }
}
dictConfig(LOGGING)

logger = logging.getLogger(__name__)

# COMMAND ----------

logger.addHandler(AzureLogAnalyticsHandler)

# COMMAND ----------

logging.info("This is the new info message")

# COMMAND ----------

logging.debug("this is a debug message")

# COMMAND ----------


