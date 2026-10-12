from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="math_sequence_taskflow",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    default_args={"retries": 2},
    tags=["learning"],
)
def math_sequence_taskflow():
    @task
    def start() -> int:
        return 10

    @task
    def add_five(x: int) -> int:
        return x + 5

    @task
    def multiply_by_two(x: int) -> int:
        return x * 2

    @task
    def subtract_three(x: int) -> int:
        return x - 3

    @task
    def square(x: int) -> None:
        print(f"{x} ^ 2 = {x**2}")

    square(subtract_three(multiply_by_two(add_five(start()))))


math_sequence_taskflow()
