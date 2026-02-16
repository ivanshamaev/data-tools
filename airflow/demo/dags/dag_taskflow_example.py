"""Example DAG: generate list from runtime parameter n and print it.

Usage:
 - Trigger the DAG with a JSON conf containing n, for example:
     airflow dags trigger -c '{"n": 5}' dag_taskflow_example
"""
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
}

def generate_list(n=3, **kwargs):
    """Generate list from 1 to n. n is expected to be int-like or string of int."""
    n_param = '{{ dag_run.conf.get("n", params.n) }}'
    try:
        nn = int(n)
    except Exception:
        nn = 0
    return list(range(1, nn + 1))

def print_numbs(**context):
    """Pulls XCom from `generate_numbs` and prints it."""
    generated_lst = context["generated_lst"]
    print(f"generated_lst: {generated_lst}")

with DAG(
    dag_id='dag_taskflow_example',
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval=None,
    catchup=False,
    tags=['example'],
    params={'n': 3},
) as dag:

    generated_lst = generate_list()

    generate_numbs = PythonOperator(
        task_id='print_numbs',
        python_callable=print_numbs,
        op_kwargs={'generated_lst': generated_lst},
    )

