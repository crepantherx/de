# Databricks notebook source
pip install azure.identity

# COMMAND ----------

pip install azure.monitor.ingestion

# COMMAND ----------

# Import required modules
import os
import logging
import datetime
import json

from azure.identity import ClientSecretCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.core.exceptions import HttpResponseError

# COMMAND ----------

# information needed to send data to the DCR endpoint
dce_endpoint = "https://log-ingestion-sx8p.eastus-1.ingest.monitor.azure.com" # ingestion endpoint of the Data Collection Endpoint object
dcr_immutableid = "dcr-ee2f228698e24a55a20d270415cadb05" # immutableId property of the Data Collection Rule
stream_name = "Custom-adb_CL" #name of the stream in the DCR that represents the destination table

credential = ClientSecretCredential(
    tenant_id="e714ef31-faab-41d2-9f1e-e6df4af16ab8",
    client_id="9604ec29-b555-4b24-b7c3-7874545942b5",
    client_secret="Me28Q~d2fFPhGVr_EJ8OIUOJxbCNDZlcVumC4bbP"
)

client = LogsIngestionClient(endpoint=dce_endpoint, credential=credential, logging_enable=True)

body = [
        {
        "TimeGenerated": "2023-01-12T15:04:48.423211Z",
        "Level": "comp",
        "Message": "Hello sudhir"
        }
    ]

try:
    client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=body)
except HttpResponseError as e:
    print(f"Upload failed: {e}")

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

# Set up the logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Create the Azure Log Analytics stream handler
dce_endpoint = "https://log-ingestion-sx8p.eastus-1.ingest.monitor.azure.com"
dcr_immutableid = "dcr-ee2f228698e24a55a20d270415cadb05"
stream_name = "Custom-adb_CL"
tenant_id="e714ef31-faab-41d2-9f1e-e6df4af16ab8"
client_id="9604ec29-b555-4b24-b7c3-7874545942b5"
client_secret="Me28Q~d2fFPhGVr_EJ8OIUOJxbCNDZlcVumC4bbP"
handler = AzureLogAnalyticsHandler(dce_endpoint, dcr_immutableid, stream_name, tenant_id, client_id, client_secret)

# Add the stream handler to the logger
logger.addHandler(handler)

# COMMAND ----------

logger.info("this si from old gen singh")

# COMMAND ----------

logger.debug('this is again from old debug')

# COMMAND ----------


