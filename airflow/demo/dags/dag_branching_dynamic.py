from __future__ import annotations

from datetime import datetime
from typing import List

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dag_branching_dynamic', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:

    def choose_branch(**ctx) -> List[str]:
        # пример: динамически выбираем, какие задачи запускать
        # можно читать из БД/файла; тут — жёстко задаём
        items = ['t1', 't3']
        return items

    branching = BranchPythonOperator(task_id='branching', python_callable=choose_branch)

    t1 = PythonOperator(task_id='t1', python_callable=lambda **c: print('t1'))
    t2 = PythonOperator(task_id='t2', python_callable=lambda **c: print('t2'))
    t3 = PythonOperator(task_id='t3', python_callable=lambda **c: print('t3'))

    final = EmptyOperator(task_id='final', trigger_rule='none_failed_or_skipped')

    branching >> [t1, t2, t3] >> final
