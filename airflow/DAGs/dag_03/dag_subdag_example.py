from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.subdag import SubDagOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

def make_subdag(parent_dag_id, child_dag_id, args):
    subdag = DAG(dag_id=f"{parent_dag_id}.{child_dag_id}", start_date=args['start_date'])
    with subdag:
        PythonOperator(task_id='sub_task_1', python_callable=lambda **c: print('sub1'))
        PythonOperator(task_id='sub_task_2', python_callable=lambda **c: print('sub2'))
    return subdag

with DAG(dag_id='dag_subdag_example', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id='start')
    sub = SubDagOperator(task_id='my_subdag', subdag=make_subdag('dag_subdag_example', 'my_subdag', {'start_date': datetime(2024,1,1)}))
    end = EmptyOperator(task_id='end')
    start >> sub >> end

# NOTE: SubDagOperator historically существовал, но часто приводит к сложностям с параллелизмом
# и считается устаревшим подходом — предпочитайте TaskGroup или Task Mapping при возможности.
