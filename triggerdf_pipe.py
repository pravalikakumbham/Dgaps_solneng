from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import time
import json


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "start_date": datetime.now(),
}
def run_dataflow_and_pipeline():
    auth_url = "https://poc.datagaps.com/dataopssecurity/oauth2/token?"
    payload = (
        "username=Pravalika.kumbham&"
        "password=U2FsdGVkX18YiOvKnJsbRQ36Evdkmp2n3O18ohOltbw%3D&"
        "grant_type=password"
    )
    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Authorization": "Basic ZGF0YW9wc3N1aXRlLXJlc3RhcGktY2xpZW50OkJLRzBAOGxF",
    }

    auth_response = requests.post(auth_url, headers=headers, data=payload)
    auth_response.raise_for_status()
    access_token = auth_response.json().get("access_token")

    if not access_token:
        raise Exception("Auth failed — no access token returned")

    # Trigger DataFlow
    dataflow_id = "8dc029df-d40d-4e18-8907-1ef14600e9b5"
    df_trigger_url = (
        f"https://poc.datagaps.com/DataFlowService/api/v1.0/"
        f"dataFlows/executeDataFlow?dataflowId={dataflow_id}"
    )
    df_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
        "Accept-Encoding": "*",
    }

    df_response = requests.post(df_trigger_url, headers=df_headers)
    df_response.raise_for_status()
    df_data = df_response.json()
    df_run_id = df_data.get("dataFlowRunId")

    if not df_run_id:
        raise Exception("No DataFlow Run ID returned")

    print(f"Triggered DataFlow. Run ID: {df_run_id}")

    # Function to check DataFlow status
    def check_dataflow_status(run_id):
        status_url = (
            f"https://poc.datagaps.com/DataFlowService/api/v1.0/"
            f"dataFlows/dataflow-status?dataFlowRunId={run_id}"
        )
        try:
            status_resp = requests.get(status_url, headers=df_headers)
            status_resp.raise_for_status()
            status_data = status_resp.json()
            return status_data.get("status", "").upper()
        except requests.RequestException as e:
            print(f"Error checking DataFlow status: {e}")
            return "ERROR"

    # Poll DataFlow status
    print("Checking DataFlow status...")
    while True:
        status = check_dataflow_status(df_run_id)
        print(f"DataFlow Status: {status}")
        if status in ["SUCCESS", "FAILED", "ERROR"]:
            break
        time.sleep(90)

    
    # Trigger Pipeline
    pipeline_url = "https://poc.datagaps.com/piper/jobs"
    pipe_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    pipe_payload = {
        "pipelineId": "d3e8e4c639934e40bc5ef1908803ca60",
        "runName": "Test Pipeline from Python",
    }

    print("Triggering Pipeline...")
    pipeline_response = requests.post(pipeline_url, headers=pipe_headers, json=pipe_payload)
    pipeline_response.raise_for_status()
    pipeline_data = pipeline_response.json()

    pipeline_run_id = pipeline_data.get("id") or pipeline_data.get("jobId")
    if not pipeline_run_id:
        print("Error: Could not find pipeline run ID in response.")
        exit(1)

    # Poll Pipeline status
    status_url = f"https://poc.datagaps.com/piper/jobs/{pipeline_run_id}/status"
    while True:
        response = requests.get(status_url, headers=pipe_headers)
        if response.status_code != 200:
            print(f"Status check failed: {response.status_code}, {response.text}")
            break

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Failed to parse JSON response")
            break

        status = data.get("status")
        print(f"Pipeline Status: {status}")

        if status in ["COMPLETED", "FAILED", "ERROR"]:
            print(f"Pipeline finished with status: {status}")
            break

        time.sleep(300)
    
    # Define Airflow DAG
with DAG(
    dag_id="datagaps_trigger_dag",
    default_args=default_args,
    schedule=None,  
    catchup=False,
    tags=["datagaps", "dataflow", "pipeline"],
) as dag:
    trigger_jobs = PythonOperator(
        task_id="trigger_dataflow_and_pipeline",
        python_callable=run_dataflow_and_pipeline,
    )
    

