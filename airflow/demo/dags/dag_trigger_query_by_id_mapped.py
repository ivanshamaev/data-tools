"""
DAG: trigger_query_by_id_mapped

Generates a list of ids and in mapped tasks triggers the `query_by_id` DAG
passing each id as `conf={'id': <value>}`.
"""
from __future__ import annotations

from datetime import datetime
import logging
from random import randint

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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
    trigger_mapped = TriggerDagRunOperator.partial(
        task_id="trigger_query_by_id",
        trigger_dag_id="query_by_id",
        max_active_tis_per_dag=1,
    ).expand(conf=items)

    items >> trigger_mapped
