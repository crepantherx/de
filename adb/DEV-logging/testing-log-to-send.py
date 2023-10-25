# Databricks notebook source
import requests
import json
import datetime
import hmac
import hashlib
import base64

# COMMAND ----------

workspace_id = 'ab79e362-1cc7-47ff-9007-c9c1907107c6'
workspace_key = 'piPRpBPsQO6YNeh841FIH8zoUs/QoYmlOrKr3YUai5r7d3VSnaANr1lt2XSOt35lHwQ20UozNgRperFigZ8Llw=='
log_type = 'adb_CL'
api_version = '2015-03-20'

# COMMAND ----------

# Format the Log Analytics API URL
url = f"https://{workspace_id}.ods.opinsights.azure.com/api/logs?api-version={api_version}"


# COMMAND ----------

# Define the function to generate the authorization header
def build_signature(customer_id, shared_key, date, content_length, method, content_type, resource):
    x_headers = f"x-ms-date:{date}"
    string_to_hash = f"{method}\n{content_length}\n{content_type}\n{x_headers}\n{resource}"
    bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(
        hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()
    ).decode()
    authorization = f"SharedKey {customer_id}:{encoded_hash}"
    return authorization

# COMMAND ----------

# Define the function to send the log data to Log Analytics
def post_data(customer_id, shared_key, body, log_type):
    method = "POST"
    content_type = "application/json"
    resource = f"/api/logs"
    content_length = len(body)
    date = datetime.datetime.utcnow().strftime("%a, %d %b %Y %H:%M:%S GMT")
    signature = build_signature(customer_id, shared_key, date, content_length, method, content_type, resource)
    headers = {
        "content-type": content_type,
        "Authorization": signature,
        "Log-Type": log_type,
        "x-ms-date": date,
    }
    response = requests.post(url, data=body, headers=headers)
    if (response.status_code >= 200) and (response.status_code <= 299):
        print("Accepted")
    else:
        print(f"Response code: {response.status_code}")

# COMMAND ----------

# Generate some sample log data
log_data = [
    {
        "TimeGenerated": datetime.datetime.utcnow().isoformat(),
        "Level": "This is a test log message",
        "Message": "Information",
    }
]
json_data = json.dumps(log_data)

# COMMAND ----------

logger.info("sfsfsdfsf")

# COMMAND ----------

# Send the log data to Log Analytics
post_data(workspace_id, workspace_key, json_data, log_type)

# COMMAND ----------



# COMMAND ----------

class Signature:
    
    def __init__(self):
        self.customer_id = 'sdfsdfsdfsd'
        self.shared_key = base64.b64encode(bytes('sdfsdfsdfsfsdfsgfg', 'utf-8'))
        self.method = 'POST'
        self.content_type = 'application/json'
        self.resource = '/api/logs'
        self.content_length = 45
        self.date = datetime.datetime.utcnow().strftime('%A %d %m %y %H:%M:%S GMT')
        self.x_headers = 'x-ms-date:' + self.date
        
    def build_signature(self):
        """Returns authorization header which will be used when sending data into Azure Log Analytics"""

        string = f"POST\n{content_length}\napplication/json\nx-ms-date:{date}\n/api/logs".encode('UTF-8')
        key = "sudhir singh is the key"
        hashed_bytes = hmac.new(key, string, digestmod=hashlib.sha256).digest()
        encoded_hash = base64.b64encode(hashed_bytes).decode('utf-8')
        authorization_header = f'SharedKey {self.customer_id}:{encoded_hash}'

        return authorization_header


# COMMAND ----------

s = Signature()
ss = s.build_signature()
ss

# COMMAND ----------

f"POST\n{content_length}\napplication/json\nx-ms-date:{date}\n/api/logs".encode('UTF-8')

# COMMAND ----------

datetime.datetime.utcnow().strftime('%A %d %m %y %H:%M:%S GMT')

# COMMAND ----------

string = f"POST\n34\napplication/json\nx-ms-date:{datetime.datetime.utcnow().strftime('%A %d %m %y %H:%M:%S GMT')}\n/api/logs".encode('UTF-8')
key = "sudhir singh is the key"

# COMMAND ----------

hashed_bytes = hmac.new(bytes(key,'UTF-8'), string, digestmod=hashlib.sha256).digest()

# COMMAND ----------

hashed_bytes

# COMMAND ----------


