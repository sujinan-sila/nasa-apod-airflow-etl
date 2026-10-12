from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def preprocess():
    print("preprocessing data...")


def train():
    print("training model...")


def evaluate():
    print("evaluating model...")


with DAG(
    dag_id="hello_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@weekly",
    catchup=False,
    default_args={"retries": 2},
    tags=["learning"],
):
    t1 = PythonOperator(task_id="preprocess", python_callable=preprocess)
    t2 = PythonOperator(task_id="train", python_callable=train)
    t3 = PythonOperator(task_id="evaluate", python_callable=evaluate)

    t1 >> t2 >> t3
