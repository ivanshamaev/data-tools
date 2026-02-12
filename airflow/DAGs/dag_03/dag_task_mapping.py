from __future__ import annotations

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator

with DAG(dag_id='dag_task_mapping', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:

    @task
    def gen_items() -> List[int]:
        # задача генерирует набор элементов для обработки
        return [1, 2, 3, 4]

    @task
    def process_item(x: int) -> int:
        # имитируем обработку
        print(f'Processing {x}')
        return x * 2

    items = gen_items()

    # TaskFlow mapping: создаёт отдельный таск для каждого элемента списка
    results = process_item.map(items)

    # Operator mapping: partial + expand (пример с BashOperator)
    # Создаст 4 mapped bash tasks, каждый выполнит echo с разным аргументом
    BashOperator.partial(task_id='mapped_echo').expand(
        bash_command=[f'echo item={i}' for i in [1,2,3,4]]
    )
