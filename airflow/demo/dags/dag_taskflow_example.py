from airflow.decorators import dag, task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import logging

default_args = { 'owner': 'airflow', }

def print_numbs(generated_lst):
    print(f"generated_lst: {generated_lst}")

with DAG(
    dag_id='dag_taskflow_example',
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    params={'n': 3},
) as dag:

    @task(multiple_outputs=True)
    def generate_list(**context):
        logging.info(f"context: {context}")

        n_param = int(context.get("params", {}).get("n"))

        logging.info(f"Received n_param: {n_param} (type: {type(n_param)})")

        return {
            "list_1": list(range(1, n_param + 1)), 
            "list_2": list(range(1, n_param + 3))
        }

    generated_lst = generate_list()

    print_1 = PythonOperator(
        task_id='print_numbs_1',
        python_callable=print_numbs,
        op_kwargs={'generated_lst': generated_lst['list_1']},
    )

    print_2 = PythonOperator(
        task_id='print_numbs_2',
        python_callable=print_numbs,
        op_kwargs={'generated_lst': generated_lst['list_2']},
    )

    generated_lst >> [print_1, print_2]
