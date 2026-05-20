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

