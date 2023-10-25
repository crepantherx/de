# Databricks notebook source
# Import required modules
import os
from azure.identity import ClientSecretCredential
from azure.monitor.ingestion import LogsIngestionClient
from azure.core.exceptions import HttpResponseError

# COMMAND ----------

# {
#   "appId": "9604ec29-b555-4b24-b7c3-7874545942b5",
#   "displayName": "sp_la",
#   "password": "Me28Q~d2fFPhGVr_EJ8OIUOJxbCNDZlcVumC4bbP",
#   "tenant": "e714ef31-faab-41d2-9f1e-e6df4af16ab8"
# }

# COMMAND ----------

# information needed to send data to the DCR endpoint
dce_endpoint = "https://log-ingestion-sx8p.eastus-1.ingest.monitor.azure.com" # ingestion endpoint of the Data Collection Endpoint object
dcr_immutableid = "dcr-ee2f228698e24a55a20d270415cadb05" # immutableId property of the Data Collection Rule
stream_name = "Custom-adb_CL" #name of the stream in the DCR that represents the destination table

# COMMAND ----------

credential = ClientSecretCredential(
    tenant_id="e714ef31-faab-41d2-9f1e-e6df4af16ab8",
    client_id="9604ec29-b555-4b24-b7c3-7874545942b5",
    client_secret="Me28Q~d2fFPhGVr_EJ8OIUOJxbCNDZlcVumC4bbP"
)

client = LogsIngestionClient(endpoint=dce_endpoint, credential=credential, logging_enable=True)

body = [
        {
        "TimeGenerated": "2023-03-12T15:04:48.423211Z",
        "Level": "Computer1",
        "Message": "Hello world"
        }
    ]

try:
    client.upload(rule_id=dcr_immutableid, stream_name=stream_name, logs=body)
except HttpResponseError as e:
    print(f"Upload failed: {e}")

# COMMAND ----------


