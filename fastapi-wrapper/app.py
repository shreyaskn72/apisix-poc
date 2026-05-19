import os
import requests
from fastapi import FastAPI, Header, HTTPException
from requests.auth import HTTPBasicAuth

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
        "conf": {
            "client_id": x_client_id,
            #"org_id": x_org_id
        }
    }

    response = requests.post(
        f"{AIRFLOW_BASE_URL}/api/v1/dags/{dag_id}/dagRuns",
        auth=HTTPBasicAuth(
            AIRFLOW_USERNAME,
            AIRFLOW_PASSWORD
        ),
        json=payload,
    )

    return {
        "status": "success",
        "airflow_response": response.json()
    }