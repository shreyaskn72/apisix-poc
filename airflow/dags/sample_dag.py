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