"""
DAG: read_postgres_af_data

This DAG connects to a PostgreSQL database (uses Airflow connection id
`postgres_default` by default) and reads rows from a table in schema `af_data`.

Edit `DEFAULT_CONN_ID` and `TABLE` variables below to match your environment.
"""
from datetime import datetime
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Change these values to match your setup
DEFAULT_CONN_ID = "postgres_default"
SCHEMA = "af_data"
TABLE = "date_list"  # <-- set the actual table name here

def read_table(**context):
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    sql = f"select event_date from af_data.date_list where id <= 5"
    logging.info("Executing SQL: %s", sql)
    try:
        records = hook.get_records(sql)
    except Exception as e:
        logging.exception("Failed to read from Postgres: %s", e)
        raise

    logging.info("Fetched %d rows from %s.%s", len(records), SCHEMA, TABLE)
    for i, row in enumerate(records[:10], start=1):
        logging.info("Row %d: %s", i, row)


with DAG(
    dag_id="read_postgres_af_data",
    start_date=datetime(2026, 2, 14),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["example", "postgres"],
) as dag:

    task_read_af_data = PythonOperator(
        task_id="read_af_data_table",
        python_callable=read_table,
    )

    task_read_af_data
