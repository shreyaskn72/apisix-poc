import os
import requests
from fastapi import FastAPI, Header, HTTPException

from datetime import datetime, timezone

app = FastAPI()

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL")



RBAC = {
    "clientA": ["sample_dag"],
    "clientB": ["sample_dag"]
}

CLIENTS = {
    "clientA": {
        "username": "clientA",
        "password": "clientApass"
    },
    "clientB": {
        "username": "clientB",
        "password": "clientBpass"
    }
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

    client = CLIENTS.get(x_client_id)

    if not client:
        raise HTTPException(
            status_code=403,
            detail="Unknown client"
        )

    login_resp = requests.post(
        f"{AIRFLOW_BASE_URL}/auth/token",
        json={
            "username": client["username"],
            "password": client["password"]
        }
    )


    if login_resp.status_code not in [200,201]:
        raise HTTPException(
            status_code=login_resp.status_code,
            detail=f"Airflow authentication failed: {login_resp.text}",
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