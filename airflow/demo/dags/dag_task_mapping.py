from __future__ import annotations

from datetime import datetime
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


with DAG(dag_id='dag_task_mapping', start_date=datetime(2024, 1, 1), schedule=None, catchup=False) as dag:

    @task
    def gen_items() -> list[int]:
        # задача генерирует набор элементов для обработки
        return [1, 2, 3, 4]

    @task(max_active_tis_per_dag=1)
    def process_item(x: int) -> int:
        # имитируем обработку
        print(f'Processing {x}')
        time.sleep(2)
        return x * 2

    items = gen_items()

    # TaskFlow mapping: создаёт отдельный таск для каждого элемента списка
    results = process_item.expand(x=items)

    @task
    def make_echo_commands(values: list[int]) -> list[str]:
        return [f'echo item={i}' for i in values]

    commands = make_echo_commands(results)

    # Operator mapping: partial + expand
    # Создаст mapped bash tasks, каждый выполнит echo с аргументом из items
    mapped_echo = BashOperator.partial(task_id='mapped_echo').expand(
        bash_command=commands
    )

    results >> mapped_echo
