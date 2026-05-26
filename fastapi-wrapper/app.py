import os
import requests
from fastapi import FastAPI, Header, HTTPException
from requests.auth import HTTPBasicAuth
from datetime import datetime, timezone

app = FastAPI()

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD")


RBAC = {
    "clientA": ["sample_dag"],
    "clientB": []
}


@app.post("/trigger/{dag_id}")
def trigger_dag(
    dag_id: str,
    x_client_id: str = Header(...),
    #x_org_id: str = Header(...)
):

    if dag_id not in RBAC.get(x_client_id, []):
        raise HTTPException(
            status_code=403,
            detail="Client not authorized"
        )

    payload = {
        "logical_date": datetime.now(timezone.utc).isoformat(),
        "conf": {
            "client_id": x_client_id,
            #"org_id": x_org_id
        }
    }

    login_resp = requests.post(
        f"{AIRFLOW_BASE_URL}/auth/token",
        json={
            "username": AIRFLOW_USERNAME,
            "password": AIRFLOW_PASSWORD
        }
    )

    token = login_resp.json()["access_token"]

    response = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v2/dags/{dag_id}/dagRuns",
        headers={
            "Authorization": f"Bearer {token}"
        },
        json=payload,
    )

    return {
        "status": "success",
        "airflow_response": response.json(),
        "response_text": response.text
    }