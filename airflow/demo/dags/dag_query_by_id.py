"""
DAG: query_by_id

Accepts an input parameter `id` via `dag_run.conf` (or `params`) and queries
Postgres for the matching row in `af_data.date_list`, then logs the result.
"""
from __future__ import annotations

from datetime import datetime
import logging
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# change to your connection id if needed
DEFAULT_CONN_ID = "postgres_default"


def query_by_id_callable(**context):
    # Prefer dag_run.conf, fall back to params

    time.sleep(2)  # Simulate some processing time before the query

    dag_run = context.get("dag_run")
    run_conf = getattr(dag_run, "conf", None) or {}
    item = run_conf.get("id") or context.get("params", {}).get("id")

    if item is None:
        raise ValueError("No 'id' provided in dag_run.conf or params")

    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    sql = "SELECT id, event_date FROM af_data.date_list WHERE id = %s"
    logging.info("Executing SQL for id %s: %s", item, sql)
    try:
        records = hook.get_records(sql, parameters=(item,))
    except Exception as e:
        logging.exception("Query failed for id %s: %s", item, e)
        raise

    if not records:
        logging.info("No rows returned for id=%s", item)
    else:
        for row in records:
            logging.info("Result for id=%s: %s", item, row)


with DAG(
    dag_id="query_by_id",
    start_date=datetime(2026, 2, 14),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    task_query = PythonOperator(
        task_id="query_by_id",
        python_callable=query_by_id_callable,
        provide_context=True,
    )

    task_query
