from datetime import datetime, timedelta

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

POSTGRES_CONN_ID = "apod_db"


@dag(
    dag_id="nasa_apod_postgres",
    start_date=datetime(2026, 9, 1),
    schedule="0 14 * * *",
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    tags=["etl", "nasa"],
)
def nasa_apod_postgres():
    @task
    def create_table():
        PostgresHook(postgres_conn_id=POSTGRES_CONN_ID).run("""
            CREATE TABLE IF NOT EXISTS apod_data (
                apod_date   DATE PRIMARY KEY,
                title       TEXT NOT NULL,
                explanation TEXT,
                url         TEXT,
                media_type  VARCHAR(20),
                loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
            );
        """)

    create_table()


nasa_apod_postgres()
