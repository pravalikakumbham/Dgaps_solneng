import requests
import base64
import time
import json
import sys

def trigger_pipeline():
    client_id = "dataopssuite-restapi-client"
    client_secret = "6##YFooD"
    username = "pravalika.kumbham"
    password = "U2FsdGVkX18FotQBvS/DvRtdTITRuwXv1mPbKGIyhQY="
    pipeline_id = "7142a2c1029143fdaa5e538c2f9f962e"

    auth_url = "https://poc.datagaps.com/dataopssecurity/oauth2/token"
    basic_auth_str = f"{client_id}:{client_secret}"
    base64_auth_str = base64.b64encode(basic_auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {base64_auth_str}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }

    response = requests.post(auth_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Auth failed: {response.status_code}, {response.text}")
        sys.exit(1)

    access_token = response.json().get("access_token")
    print(" Authentication successful")

    pipeline_url = "https://poc.datagaps.com/piper/jobs"
    pipe_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    pipe_payload = {
        "pipelineId": pipeline_id,
        "runName": "Trigger Pipeline using Github actions",
    }

    print(" Triggering Pipeline...")
    pipeline_response = requests.post(pipeline_url, headers=pipe_headers, json=pipe_payload)

    if pipeline_response.status_code != 200:
        print(f"Pipeline trigger failed: {pipeline_response.status_code}, {pipeline_response.text}")
        sys.exit(1)

    pipeline_data = pipeline_response.json()
    pipeline_run_id = pipeline_data.get("id") or pipeline_data.get("jobId")
    print(f" Pipeline triggered successfully, Run ID: {pipeline_run_id}")

    return access_token, pipeline_run_id


def pipeline_status(bearer_token, pipeline_run_id):
    status_url = f"https://poc.datagaps.com/piper/jobs/{pipeline_run_id}/status"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    print(" Checking pipeline status every 60 seconds...")

    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code != 200:
            print(f" Status check failed: {response.status_code}, {response.text}")
            sys.exit(1)

        try:
            data = response.json()
            print(data)
        except json.JSONDecodeError:
            print("Failed to parse JSON response")
            sys.exit(1)

        status = data.get("status")

        if status in ["COMPLETED", "FAILED", "ERROR"]:
            print(f" Pipeline finished with status: {status}")
            return status

        time.sleep(60)  


token, run_id = trigger_pipeline()
final_status = pipeline_status(token, run_id)
print(final_status)

if final_status == "COMPLETED":
    sys.exit(0)
else:
    print(final_status)
    sys.exit(1)
    


