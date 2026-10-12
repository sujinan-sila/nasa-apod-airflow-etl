from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


def start_number(ti):
    ti.xcom_push(key="current_value", value=10)
    print("start: 10")


def add_five(ti):
    value = ti.xcom_pull(key="current_value", task_ids="start")
    ti.xcom_push(key="current_value", value=value + 5)
    print(f"{value} + 5 = {value + 5}")


def multiply_by_two(ti):
    value = ti.xcom_pull(key="current_value", task_ids="add_five")
    ti.xcom_push(key="current_value", value=value * 2)
    print(f"{value} * 2 = {value * 2}")


def subtract_three(ti):
    value = ti.xcom_pull(key="current_value", task_ids="multiply_by_two")
    ti.xcom_push(key="current_value", value=value - 3)
    print(f"{value} - 3 = {value - 3}")


def square(ti):
    value = ti.xcom_pull(key="current_value", task_ids="subtract_three")
    ti.xcom_push(key="current_value", value=value**2)
    print(f"{value} ^ 2 = {value**2}")


with DAG(
    dag_id="math_sequence",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2},
    tags=["learning"],
):
    steps = [
        PythonOperator(task_id="start", python_callable=start_number),
        PythonOperator(task_id="add_five", python_callable=add_five),
        PythonOperator(task_id="multiply_by_two", python_callable=multiply_by_two),
        PythonOperator(task_id="subtract_three", python_callable=subtract_three),
        PythonOperator(task_id="square", python_callable=square),
    ]
    steps[0] >> steps[1] >> steps[2] >> steps[3] >> steps[4]
