# APISIX + FastAPI + Airflow RBAC POC

This POC demonstrates:

* Client identification using API keys
* APISIX gateway authentication
* FastAPI wrapper authorization (RBAC)
* Airflow DAG triggering
* Per-client audit visibility

This implementation covers:

## Scope (POC)

### Step 1

Client calls APISIX with API key

### Step 2

APISIX validates API key and injects client identity headers

### Step 3

FastAPI wrapper performs RBAC and triggers Airflow DAG

---

# Final POC Architecture

```text
Client
   ↓
API Key
   ↓
APISIX Gateway
   ↓
Inject Headers:
  X-Client-Id
  X-Org-Id
   ↓
FastAPI Wrapper
   ↓
RBAC Validation
   ↓
Airflow API
```

---

# Folder Structure

```text
poc/
├── docker-compose.yml
├── apisix/
│   └── apisix.yaml
├── fastapi-wrapper/
│   ├── app.py
│   ├── requirements.txt
│   └── Dockerfile
├── airflow/
│   ├── dags/
│   │   └── sample_dag.py
│   └── airflow.env
└── README.md
```

---

# STEP 1 — Install Prerequisites

Install:

* Docker Desktop
* Docker Compose

Verify:

```bash
docker --version
docker compose version
```

---

# STEP 2 — Create docker-compose.yml

Create:

```yaml
version: '3.9'

services:

  apisix:
    image: apache/apisix:3.9.1-debian
    container_name: apisix
    ports:
      - "9080:9080"
      - "9180:9180"
    volumes:
      - ./apisix/config.yaml:/usr/local/apisix/conf/config.yaml:ro
    depends_on:
      - fastapi-wrapper

  fastapi-wrapper:
    build: ./fastapi-wrapper
    container_name: fastapi-wrapper
    ports:
      - "8000:8000"
    environment:
      AIRFLOW_BASE_URL: http://airflow-webserver:8080
      AIRFLOW_USERNAME: admin
      AIRFLOW_PASSWORD: admin
    depends_on:
      - airflow-webserver

  postgres:
    image: postgres:15
    container_name: airflow-postgres
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow

  airflow-webserver:
    image: apache/airflow:2.9.3
    container_name: airflow-webserver
    restart: always
    depends_on:
      - postgres
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: webserver
    ports:
      - "8081:8080"

  airflow-scheduler:
    image: apache/airflow:2.9.3
    container_name: airflow-scheduler
    restart: always
    depends_on:
      - airflow-webserver
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: scheduler
```

---

# STEP 3 — Create Airflow Environment File

Create:

```text
airflow/airflow.env
```

Content:

```env
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres/airflow
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__RBAC=True
AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_AIRFLOW_WWW_USER_FIRSTNAME=admin
_AIRFLOW_WWW_USER_LASTNAME=admin
_AIRFLOW_WWW_USER_ROLE=Admin
_AIRFLOW_WWW_USER_EMAIL=admin@example.com
```

---

# STEP 4 — Create Sample DAG

Create:

```python
airflow/dags/sample_dag.py
```

Content:

```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_client(**context):
    conf = context["dag_run"].conf

    print("Triggered By Client:", conf.get("client_id"))
    print("Organization:", conf.get("org_id"))


with DAG(
    dag_id="sample_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    task = PythonOperator(
        task_id="print_client_info",
        python_callable=print_client,
    )
```

---

# STEP 5 — Create FastAPI Wrapper

Create:

```text
fastapi-wrapper/requirements.txt
```

Content:

```text
fastapi
uvicorn
requests
```

---

# STEP 6 — Create FastAPI Dockerfile

Create:

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY app.py .

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]
```

---

# STEP 7 — Create FastAPI Wrapper Code

Create:

```python
fastapi-wrapper/app.py
```

Content:

```python
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
    x_org_id: str = Header(...)
):

    if dag_id not in RBAC.get(x_client_id, []):
        raise HTTPException(
            status_code=403,
            detail="Client not authorized"
        )

    payload = {
        "conf": {
            "client_id": x_client_id,
            "org_id": x_org_id
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
```

---

# STEP 8 — Configure APISIX

Create:

```text
apisix/apisix.yaml
```

Content:

```yaml
apisix:
  node_listen: 9080

routes:
  - uri: /trigger/*
    methods:
      - POST

    plugins:
      key-auth: {}

      proxy-rewrite:
        headers:
          set:
            X-Client-Id: clientA
            X-Org-Id: orgA

    upstream:
      type: roundrobin
      nodes:
        "fastapi-wrapper:8000": 1

consumers:
  - username: clientA
    plugins:
      key-auth:
        key: clientA-secret-key
```

---

# STEP 9 — Start Everything

Run:

```bash
docker compose up -d
```

---

# STEP 10 — Initialize Airflow Database

Run:

```bash
docker exec -it airflow-webserver airflow db migrate
```

Create admin user:

```bash
docker exec -it airflow-webserver airflow users create \
  --username admin \
  --password admin \
  --firstname admin \
  --lastname admin \
  --role Admin \
  --email admin@example.com
```

---

# STEP 11 — Access Services

| Service | URL                                                      |
| ------- | -------------------------------------------------------- |
| APISIX  | [http://localhost:9080](http://localhost:9080)           |
| FastAPI | [http://localhost:8000/docs](http://localhost:8000/docs) |
| Airflow | [http://localhost:8081](http://localhost:8081)           |

Airflow Login:

```text
admin/admin
```

---

# Configure Route Using Admin API
```bash

curl http://127.0.0.1:9180/apisix/admin/routes/1 \
-X PUT \
-H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" \
-d '
{
  "uri": "/trigger/*",
  "methods": ["POST"],
  "plugins": {
    "key-auth": {},
    "proxy-rewrite": {
      "headers": {
        "set": {
          "X-Client-Id": "clientA",
          "X-Org-Id": "orgA"
        }
      }
    }
  },
  "upstream": {
    "type": "roundrobin",
    "nodes": {
      "fastapi-wrapper:8000": 1
    }
  }
}'
```
# Create Consumer
```bash
curl http://127.0.0.1:9180/apisix/admin/consumers \
-X PUT \
-H "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" \
-d '
{
  "username": "clientA",
  "plugins": {
    "key-auth": {
      "key": "clientA-secret-key"
    }
  }
}'
```

# STEP 12 — Trigger DAG Through APISIX

Run:

```bash
curl -X POST \
http://localhost:9080/trigger/sample_dag \
-H "apikey: clientA-secret-key"
```

Expected Response:

```json
{
  "status": "success"
}
```

---

# STEP 13 — Verify RBAC

Currently:

```python
RBAC = {
    "clientA": ["sample_dag"],
    "clientB": []
}
```

If you configure another APISIX consumer:

```yaml
- username: clientB
  plugins:
    key-auth:
      key: clientB-secret-key
```

and call:

```bash
curl -X POST \
http://localhost:9080/trigger/sample_dag \
-H "apikey: clientB-secret-key"
```

Expected:

```json
{
  "detail": "Client not authorized"
}
```

---

# STEP 14 — Verify Client Metadata Inside Airflow

Open:

```text
Airflow UI
→ DAG Runs
→ Task Logs
```

You should see:

```text
Triggered By Client: clientA
Organization: orgA
```

This proves:

* APISIX identified client
* FastAPI enforced RBAC
* Airflow received tenant metadata

---

# What This POC Demonstrates

## APISIX Responsibilities

* API key validation
* client identification
* request routing

---

## FastAPI Responsibilities

* authorization
* RBAC
* DAG validation
* audit orchestration

---

## Airflow Responsibilities

* workflow execution
* DAG orchestration
* metadata visibility

---

# Recommended Next Phase After POC

After this works:

## Phase 2

Replace hardcoded RBAC with database.

Example:

```text
client_id | dag_id
-------------------
clientA   | dag1
clientA   | dag2
clientB   | dag3
```

---

## Phase 3

Replace Airflow admin user with:

* Azure AD OAuth
* Service principals
* Bearer tokens

---

## Phase 4

Add:

* Datadog logs
* request tracing
* request_id propagation
* structured audit logs

---

## Phase 5

Move to Kubernetes

Using:

* Helm
* ArgoCD
* Kubernetes secrets
* APISIX ingress

---

# Recommended Production Improvements

## Do NOT Keep In Production

* hardcoded RBAC
* local admin credentials
* plaintext secrets
* basic auth

---

# Production Architecture Target

Eventually migrate to:

```text
APISIX
   ↓
FastAPI
   ↓
Azure AD OAuth
   ↓
Airflow OAuth/OIDC
```

This POC is intentionally simplified to help you demonstrate:

* API gateway auth
* wrapper RBAC
* DAG triggering
* tenant isolation
* audit propagation
