from __future__ import annotations

from datetime import datetime
import time
from random import randint
import logging

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task


@task
def gen_items_taskflow() -> list[dict]:
    # Возвращает список словарей для op_kwargs (по одному словарю на mapped task)
    values = [randint(1, 50) for _ in range(1, randint(2, 10))]
    items = [{"item": v} for v in values]
    logging.info("Generated items for mapping: %s", items)
    return items


def process_item_callable(item: int, **kwargs):
    # Имитируем обработку одного элемента — будет вызвана PythonOperator-ом
    logging.info("Processing %s", item)
    time.sleep(2)


with DAG(dag_id="dag_task_mapping_pythonop", start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:

    # Генератор элементов — TaskFlow API для удобной XCom интеграции
    items = gen_items_taskflow()

    # Маппинг через PythonOperator; указываем max_active_tis_per_dag=1 чтобы
    # задачи выполнялись последовательно (по одному task instance одновременно)
    process_mapped = PythonOperator.partial(
        task_id="process_item",
        python_callable=process_item_callable,
        max_active_tis_per_dag=1,
    ).expand(op_kwargs=items)

    items >> process_mapped
