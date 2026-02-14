"""
DAG: mapped_query_postgres

Generates a list of item ids and for each item runs a mapped `PythonOperator`
that queries Postgres for `id, event_date` from `af_data.date_list` and logs the result.

Configure `DEFAULT_CONN_ID` to match your Airflow Postgres connection.
"""
from __future__ import annotations

from datetime import datetime
import logging
from random import randint
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# change if needed
DEFAULT_CONN_ID = "postgres_default"


@task
def gen_items() -> list[dict]:
    """Generate list of dicts suitable for `expand(op_kwargs=...)`.

    Returns list like: [{"item": 1}, {"item": 5}, ...]
    """
    values = [randint(1, 50) for _ in range(1, randint(3, 8))]
    items = [{"item": v} for v in values]
    logging.info("Generated items for mapping: %s", items)
    return items


def query_item_callable(item: int, **kwargs):
    """Query Postgres for a specific item and log the result."""
    time.sleep(2)
    hook = PostgresHook(postgres_conn_id=DEFAULT_CONN_ID)
    sql = "SELECT id, event_date FROM af_data.date_list WHERE id = %s"
    logging.info("Executing SQL for item %s: %s", item, sql)
    try:
        records = hook.get_records(sql, parameters=(item,))
    except Exception as e:
        logging.exception("Query failed for item %s: %s", item, e)
        raise

    if not records:
        logging.info("No rows returned for id=%s", item)
    else:
        for row in records:
            logging.info("Result for id=%s: %s", item, row)


with DAG(
    dag_id="mapped_query_postgres",
    start_date=datetime(2026, 2, 14),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    items = gen_items()

    # Use PythonOperator.partial + expand(op_kwargs=items). Each dict in `items`
    # will be passed as op_kwargs to one mapped operator. Set
    # max_active_tis_per_dag=1 to force sequential execution of mapped tasks.
    process_mapped = PythonOperator.partial(
        task_id="query_item",
        python_callable=query_item_callable,
        max_active_tis_per_dag=1,
    ).expand(op_kwargs=items)

    items >> process_mapped
