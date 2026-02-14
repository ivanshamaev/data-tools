"""
DAG: trigger_query_by_id_mapped

Generates a list of ids and in mapped tasks triggers the `query_by_id` DAG
passing each id as `conf={'id': <value>}`.
"""
from __future__ import annotations

from datetime import datetime
import logging
from random import randint
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator


@task
def gen_items_for_trigger() -> list[dict]:
    values = sorted([randint(1, 50) for _ in range(1, randint(3, 10))], reverse=True)
    items = [{"id": v} for v in values]
    logging.info("Generated trigger confs: %s", items)
    return items


with DAG(
    dag_id="trigger_query_by_id_mapped",
    start_date=datetime(2026, 2, 14),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
) as dag:

    items = gen_items_for_trigger()

    # Trigger `query_by_id` for each generated id. `conf` expects a dict per mapped
    # run, so `items` is already in the correct shape: [{"id": 1}, {"id": 2}, ...]
    def log_trigger_callable(id: int, **kwargs):
        logging.info("Triggering DAG 'query_by_id' with id=%s", id)

    # Each mapped task will log and then trigger the target DAG. Using a
    # PythonOperator lets us log first and then create+execute a
    # TriggerDagRunOperator programmatically so the same mapped task both logs
    # and triggers.
    def log_and_trigger_callable(id: int, **context):
        logging.info("Triggering DAG 'query_by_id' with id=%s", id)
        time.sleep(2)  # Simulate some processing time before the trigger
        # Create a TriggerDagRunOperator instance and execute it immediately
        trig = TriggerDagRunOperator(
            task_id="inline_trigger_%s" % id,
            trigger_dag_id="query_by_id",
            conf={"id": id},
            wait_for_completion=False,
        )
        trig.execute(context)

    trigger_mapped = PythonOperator.partial(
        task_id="trigger_query_by_id",
        python_callable=log_and_trigger_callable,
        max_active_tis_per_dag=1,
    ).expand(op_kwargs=items)

    items >> trigger_mapped
