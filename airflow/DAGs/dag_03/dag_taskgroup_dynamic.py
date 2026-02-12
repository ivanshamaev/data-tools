from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dag_taskgroup_dynamic', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id='start')

    # Демонстрация динамики внутри TaskGroup
    items = ['a', 'b', 'c']
    with TaskGroup('dynamic_group') as tg:
        for it in items:
            def make_call(item):
                def _call(**ctx):
                    print(f'Inside group processing {item}')
                return _call

            PythonOperator(task_id=f'proc_{it}', python_callable=make_call(it))

    end = EmptyOperator(task_id='end')
    start >> tg >> end
