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
│   └── config.yaml
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
version: "3.9"

services:

  postgres:
    image: postgres:15
    container_name: airflow-postgres
    restart: always
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    ports:
      - "5432:5432"

  airflow-init:
    image: apache/airflow:3.0.6
    container_name: airflow-init
    depends_on:
      - postgres
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
    command: >
      bash -c "
      airflow db migrate &&
      airflow users create
      --username admin
      --password admin
      --firstname admin
      --lastname admin
      --role Admin
      --email admin@example.com
      "

  airflow-api-server:
    image: apache/airflow:3.0.6
    container_name: airflow-api-server
    hostname: airflow-api-server
    restart: always
    depends_on:
      - postgres
      - airflow-init
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
    command: api-server
    ports:
      - "8081:8080"

  airflow-scheduler:
    image: apache/airflow:3.0.6
    container_name: airflow-scheduler
    hostname: airflow-scheduler
    restart: always
    depends_on:
      - postgres
      - airflow-init
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
    command: scheduler

  airflow-dag-processor:
    image: apache/airflow:3.0.6
    container_name: airflow-dag-processor
    hostname: airflow-dag-processor
    restart: always
    depends_on:
      - postgres
    env_file:
      - ./airflow/airflow.env
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./airflow/logs:/opt/airflow/logs
    command: dag-processor

  fastapi-wrapper:
    build: ./fastapi-wrapper
    container_name: fastapi-wrapper
    restart: always
    depends_on:
      - airflow-api-server
    environment:
      AIRFLOW_BASE_URL: http://airflow-api-server:8080
      AIRFLOW_USERNAME: admin
      AIRFLOW_PASSWORD: admin
    ports:
      - "8000:8000"



  etcd:
    image: quay.io/coreos/etcd:v3.5.12
    container_name: etcd
    restart: always
    environment:
      ETCD_NAME: etcd
      ETCD_DATA_DIR: /etcd-data
      ETCD_ADVERTISE_CLIENT_URLS: http://etcd:2379
      ETCD_LISTEN_CLIENT_URLS: http://0.0.0.0:2379






  apisix:
    image: apache/apisix:3.9.1-debian
    container_name: apisix
    restart: always
    depends_on:
      - etcd
      - fastapi-wrapper
    ports:
      - "9080:9080"
      - "9180:9180"
    environment:
      APISIX_STAND_ALONE: "false"
    volumes:
      - ./apisix/config.yaml:/usr/local/apisix/conf/config.yaml:ro




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
#AIRFLOW__WEBSERVER__RBAC=True
#AIRFLOW__API__AUTH_BACKENDS=airflow.api.auth.backend.basic_auth,airflow.api.auth.backend.session
AIRFLOW__API_AUTH__JWT_SECRET=randomsecret
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin
_AIRFLOW_WWW_USER_FIRSTNAME=admin
_AIRFLOW_WWW_USER_LASTNAME=admin
_AIRFLOW_WWW_USER_ROLE=Admin
_AIRFLOW_WWW_USER_EMAIL=admin@example.com
AIRFLOW__CORE__AUTH_MANAGER=airflow.providers.fab.auth_manager.fab_auth_manager.FabAuthManager
AIRFLOW__CORE__HOSTNAME_CALLABLE=socket.getfqdn
AIRFLOW__LOGGING__BASE_LOG_FOLDER=/opt/airflow/logs
AIRFLOW__CORE__EXECUTION_API_SERVER_URL=http://airflow-api-server:8080/execution/
```

---

# STEP 4 — Create Sample DAG

Create:

```python
airflow/dags/sample_dag.py
```

Content:

```python
from airflow.decorators import dag, task
from datetime import datetime


@dag(
    dag_id="sample_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["rbac", "client-trigger"],
)
def sample_dag():

    @task
    def print_client_info(**context):
        conf = context["dag_run"].conf

        print("Triggered By Client:", conf.get("client_id"))
        print("Organization:", conf.get("org_id", "N/A"))

    print_client_info()


dag = sample_dag()


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
```

---

# STEP 8 — Configure APISIX

Create:

```text
apisix/config.yaml
```

Content:

```yaml

deployment:
  admin:
    allow_admin:
      - 0.0.0.0/0

  etcd:
    host:
      - "http://etcd:2379"

apisix:
  node_listen: 9080

plugin_attr:
  key-auth:
    header: apikey


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
docker exec -it airflow-api-server airflow db migrate
```

Create admin user:

```bash
docker exec -it airflow-api-server airflow users create \
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
curl --location --request PUT "http://127.0.0.1:9180/apisix/admin/routes/1" \
--header "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" \
--header "Content-Type: application/json" \
--data-raw '{
  "uri": "/trigger/*",
  "methods": ["POST"],
  "plugins": {
    "key-auth": {},
    "proxy-rewrite": {
      "headers": {
        "set": {
          "X-Client-Id": "$consumer_name"
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
curl --location --request PUT "http://127.0.0.1:9180/apisix/admin/consumers/clientA" \
--header "X-API-KEY: edd1c9f034335f136f87ad84b625c8f1" \
--header "Content-Type: application/json" \
--data-raw '{
  "username": "clientA",
  "plugins": {
    "key-auth": {
      "key": "clientA-secret-keyA"
    }
  }
}'
```


# STEP 12 — Trigger DAG Through APISIX

Run:

```bash
curl -X POST \
http://localhost:9080/trigger/sample_dag \
-H "apikey: clientA-secret-keyA"
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


and call:

```bash
curl -X POST \
http://localhost:9080/trigger/sample_dag \
-H "apikey: clientB-secret-keyB"
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

# If you change fastapi-wrapper alone..

```text
docker compose up -d fastapi-wrapper
```

Sometimes only restart needed:

```text
docker restart fastapi-wrapper
```

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
