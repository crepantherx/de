# Databricks notebook source
import requests
import json
import time
import datetime
import logging
import base64
import hmac
import hashlib

# COMMAND ----------

workspace_id = 'ab79e362-1cc7-47ff-9007-c9c1907107c6'
workspace_key = 'piPRpBPsQO6YNeh841FIH8zoUs/QoYmlOrKr3YUai5r7d3VSnaANr1lt2XSOt35lHwQ20UozNgRperFigZ8Llw=='
uri = "https://log-ingestion-sx8p.eastus-1.ingest.monitor.azure.com"
dcr = "dcr-9c34dccf88624012976f754f7e50d727"

# COMMAND ----------

def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
    """Returns authorization header which will be used when sending data into Azure Log Analytics"""
    
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, 'UTF-8')
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode('utf-8')
    authorization = "SharedKey {}:{}".format(customer_id,encoded_hash)
    return authorization

def post_data(customer_id, shared_key, body, log_type):
    """Sends payload to Azure Log Analytics Workspace
    
    Keyword arguments:
    customer_id -- Workspace ID obtained from Advanced Settings
    shared_key -- Authorization header, created using build_signature
    body -- payload to send to Azure Log Analytics
    log_type -- Azure Log Analytics table name
    """
    
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = "2022-05-10T09:30:10.123Z"
    content_length = len(body)
    signature = build_signature(customer_id, shared_key, rfc1123date, content_length, method, content_type, resource)

    uri = 'https://' + customer_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': log_type,
        'x-ms-date': rfc1123date
    }

    response = requests.post(uri,data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        logging.info('Accepted payload:' + body)
    else:
        logging.error("Unable to Write: " + format(response.status_code))

# COMMAND ----------

azure_log_customer_id = 'ab79e362-1cc7-47ff-9007-c9c1907107c6' 
azure_log_shared_key =  'piPRpBPsQO6YNeh841FIH8zoUs/QoYmlOrKr3YUai5r7d3VSnaANr1lt2XSOt35lHwQ20UozNgRperFigZ8Llw=='

url = 'https://learn.microsoft.com/en-us/azure/azure-monitor/agents/data-sources-custom-logs#defining-a-custom-log'
demo_request = requests.get(url)
table_name = 'adb_CL'
data = {
    "TimeGenerated": "2022-05-10T09:30:10.123Z",
    "Level": "PythonLogSender",
    "Message": "hello world"
}
data_json = json.dumps(data)

try:
    post_data(azure_log_customer_id, azure_log_shared_key, data_json, table_name)
except Exception as error:
    print(error)

# COMMAND ----------


