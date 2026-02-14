from __future__ import annotations

from datetime import datetime
import time
from random import randint

from airflow import DAG
from airflow.decorators import task


with DAG(dag_id='dag_task_mapping', start_date=datetime(2026, 1, 1), schedule=None, catchup=False) as dag:

    @task
    def gen_items() -> list[int]:
        # задача генерирует набор элементов для обработки
        return [randint(1, 50) for _ in range(1, randint(2,10))]

    @task(max_active_tis_per_dag=1)
    def process_item(x: int) -> int:
        # имитируем обработку
        print(f'Processing {x}')
        time.sleep(1)

    items = gen_items()

    # TaskFlow mapping: создаёт отдельный таск для каждого элемента списка
    process_item.expand(x=items)
