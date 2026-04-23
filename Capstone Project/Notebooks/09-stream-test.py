# Databricks notebook source
dbutils.widgets.text("Environment", "dev", "Set the current environment/catalog name")
dbutils.widgets.text("Host", "", "Databricks Workspace URL")
dbutils.widgets.text("AccessToken", "", "Secure Access Token")

# COMMAND ----------

env = dbutils.widgets.get("Environment")
host = dbutils.widgets.get("Host")
token = dbutils.widgets.get("AccessToken")

# COMMAND ----------

# MAGIC %run ./02-setup

# COMMAND ----------

SH = SetupHelper(env)
SH.cleanup()

# COMMAND ----------

job_payload = \
{
        "name": "stream-test",
        "webhook_notifications": {},
        "timeout_seconds": 0,
        "max_concurrent_runs": 1,
        "tasks": [
            {
                "task_key": "stream-test-task",
                "run_if": "ALL_SUCCESS",
                "notebook_task": {
                    "notebook_path": "/Users/giridharreddyt@chintuchinu1687gmail.onmicrosoft.com/Real-Time-Health-Analytics-Lakehouse-Platform/Capstone Project/Notebooks/07-run",
                    "source": "WORKSPACE"
                },
                "existing_cluster_id": "0405-192415-ox6uufmq",
                "timeout_seconds": 0,
                "email_notifications": {}
            }
        ],
        "format": "MULTI_TASK"
    }

# COMMAND ----------

# Create a streaming job
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
response = _w.api_client.do('POST', '/api/2.1/jobs/create', body=job_payload)
job_id = response['job_id']
print(f"Created Job {job_id}")

# COMMAND ----------

# Trigger the streaming job
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
run_payload = {"job_id": job_id, "notebook_params": {"Environment":env, "RunType": "stream", "ProcessingTime": "1 seconds"}}
response = _w.api_client.do('POST', '/api/2.1/jobs/run-now', body=run_payload)
run_id = response["run_id"]
print(f"Started Job run {run_id}")

# COMMAND ----------

# Wait until job starts
import time
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
status_payload = {"run_id": run_id}
job_status="PENDING"
while job_status == "PENDING":
    time.sleep(20)
    response = _w.api_client.do('GET', '/api/2.1/jobs/runs/get', query=status_payload)
    job_status = response["tasks"][0]["state"]["life_cycle_state"]  
    print(job_status)    

# COMMAND ----------

# MAGIC %run ./03-history-loader

# COMMAND ----------

# MAGIC %run ./10-producer

# COMMAND ----------

# MAGIC %run ./04-bronze

# COMMAND ----------

# MAGIC %run ./05-silver

# COMMAND ----------

# MAGIC %run ./06-gold

# COMMAND ----------

import time

print("Sleep for 2 minutes and let setup and history loader finish...")
time.sleep(2*60)

#Validate setup and history load
HL = HistoryLoader(env)
PR = Producer()
BZ = Bronze(env)
SL = Silver(env)
GL = Gold(env)

SH.validate()
HL.validate()

#Produce some incremantal
PR.produce(1)
PR.validate(1)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(1)
SL.validate(1)
GL.validate(1)
 

#Produce some incremantal data and wait for micro batch
PR.produce(2)
PR.validate(2)

# COMMAND ----------

print("Sleep for 2 minutes and let microbatch pickup the data...")
time.sleep(2*60)

#Validate bronze, silver and gold layer 
BZ.validate(2)
SL.validate(2)
GL.validate(2)

# COMMAND ----------

#Terminate the streaming Job
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
cancel_payload = {"run_id": run_id}
_w.api_client.do('POST', '/api/2.1/jobs/runs/cancel', body=cancel_payload)
print(f"Canceled Job run {run_id}")

# COMMAND ----------

#Delete the Job
from databricks.sdk import WorkspaceClient

_w = WorkspaceClient()
delete_job_payload = {"job_id": job_id}
_w.api_client.do('POST', '/api/2.1/jobs/delete', body=delete_job_payload)
print(f"Deleted Job {job_id}")

# COMMAND ----------

dbutils.notebook.exit("SUCCESS")