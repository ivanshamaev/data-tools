from __future__ import annotations

from datetime import datetime, date
from typing import List, Dict

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.microsoft.mssql.operators.mssql import MsSqlOperator

DEFAULT_CONN_ID = 'mssql_default'
TARGET_DAG_ID = 'target_dag'  # <- поменяйте на id дага, который нужно триггерить

with DAG(
    dag_id='orchestrator_dates_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['orchestration'],
) as dag:

    @task
    def get_dates() -> List[str]:
        hook = MsSqlHook(mssql_conn_id=DEFAULT_CONN_ID)
        # Измените SQL под вашу таблицу/колонку
        rows = hook.get_records("""
            SELECT date_col
            FROM your_schema.your_table
            ORDER BY date_col DESC
        """)
        dates: List[str] = []
        for r in rows:
            val = r[0]
            if isinstance(val, (datetime, date)):
                dates.append(val.strftime('%Y-%m-%d'))
            else:
                dates.append(str(val))
        return dates

    @task
    def split_dates(dates: List[str]) -> Dict[str, List[str]]:
        ctx = get_current_context()
        # ожидается, что параметр `n` передаётся через dag_run.conf, например: {"n": 3}
        n = ctx.get('dag_run').conf.get('n') if ctx.get('dag_run') else None
        try:
            n = int(n) if n is not None else 0
        except Exception:
            n = 0
        top = dates[:n]
        rest = dates[n:]
        return {'top': top, 'rest': rest}

    start = EmptyOperator(task_id='start')

    dates = get_dates()
    parts = split_dates(dates)

    # Триггерим целевой даг для первых N дат (каждая дата — отдельный запуск)
    trigger_first_n = TriggerDagRunOperator.partial(
        task_id='trigger_target_dag',
        trigger_dag_id=TARGET_DAG_ID,
        wait_for_completion=False,
    ).expand(
        conf=[{'date': d} for d in parts['top']]
    )

    # Для оставшихся дат выполняем MSSQL процедуру Exec Procedure '<date>'
    # Здесь шаблон SQL: замените имя процедуры при необходимости
    mssql_calls = MsSqlOperator.partial(
        task_id='exec_proc_for_remaining',
        mssql_conn_id=DEFAULT_CONN_ID,
    ).expand(
        sql=[f"EXEC Procedure '{d}'" for d in parts['rest']]
    )

    end = EmptyOperator(task_id='end')

    start >> dates >> parts >> [trigger_first_n, mssql_calls] >> end
