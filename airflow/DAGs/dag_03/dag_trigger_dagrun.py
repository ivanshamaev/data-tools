from __future__ import annotations

from datetime import datetime
from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator

with DAG(dag_id='dag_trigger_dagrun', start_date=datetime(2024,1,1), schedule_interval=None, catchup=False) as dag:
    start = EmptyOperator(task_id='start')

    # Триггер другого дага один раз
    trigger_once = TriggerDagRunOperator(
        task_id='trigger_once',
        trigger_dag_id='some_other_dag',
        wait_for_completion=False,
    )

    # Маппинг триггеров — создаст по одному mapped trigger на каждый conf
    TriggerDagRunOperator.partial(task_id='trigger_mapped', trigger_dag_id='some_other_dag').expand(
        conf=[{'item': i} for i in ['2025-01-01', '2025-01-02']]
    )

    end = EmptyOperator(task_id='end')
    start >> trigger_once >> end
