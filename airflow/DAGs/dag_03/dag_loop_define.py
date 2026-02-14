from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dag_loop_define', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id='start')

    # Пример: создаём несколько задач в цикле на этапе парсинга DAG
    # Это статическое динамическое создание: количество тасков фиксируется в момент импорта дага
    def make_callable(i):
        def _run(**context):
            print(f'Running loop task {i}')
        return _run

    for i in range(0, 6):
        t = PythonOperator(task_id=f'loop_task_{i}', python_callable=make_callable(i))
        start >> t
