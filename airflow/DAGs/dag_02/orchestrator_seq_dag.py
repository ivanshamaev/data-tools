from __future__ import annotations

from datetime import datetime, date
from typing import List, Dict
import time

from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# NOTE: uses experimental trigger API to create dag runs sequentially
from airflow.api.common.experimental.trigger_dag import trigger_dag

DEFAULT_CONN_ID = 'mssql_default'
TARGET_DAG_ID = 'target_dag'  # change to the DAG id you want to trigger

with DAG(
    dag_id='orchestrator_seq_dag',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['orchestration', 'sequential'],
) as dag:

    @task
    def get_dates() -> List[str]:
        """Query MSSQL and return list of dates as strings in DESC order.

        This task runs at runtime and returns the list as TaskFlow XCom.
        """
        hook = MsSqlHook(mssql_conn_id=DEFAULT_CONN_ID)
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
    def orchestrate(dates: List[str]) -> Dict[str, List[str]]:
        """Trigger target DAG runs sequentially for first N dates and run MSSQL proc for rest.

        Behavior:
        - reads `n` from `dag_run.conf` (e.g. `{"n": 3}`) — if missing or invalid, n=0
        - takes first `n` dates from `dates` (already ordered desc)
        - for each date in `top` triggers `TARGET_DAG_ID` sequentially using `trigger_dag`
        - then for remaining dates runs `EXEC Procedure '<date>'` in MSSQL sequentially

        Notes:
        - `trigger_dag` creates a DagRun entry; this loop calls it one-by-one so triggers are sequential (not parallel).
        - we don't wait for the triggered DAGs to finish here; we only ensure triggers happen in order.
        """
        ctx = get_current_context()
        dag_run = ctx.get('dag_run')
        n = 0
        if dag_run and getattr(dag_run, 'conf', None):
            try:
                n = int(dag_run.conf.get('n', 0) or 0)
            except Exception:
                n = 0

        top = dates[:n]
        rest = dates[n:]

        triggered: List[str] = []
        executed: List[str] = []

        # Trigger target DAGs sequentially (from larger to smaller date)
        for d in top:
            run_id = f"orchestrator__{d}__{int(time.time()*1000)}"
            trigger_dag(dag_id=TARGET_DAG_ID, run_id=run_id, conf={'date': d})
            triggered.append(d)
            # small sleep optional to avoid hammering scheduler
            time.sleep(0.5)

        # For remaining dates execute MSSQL procedure sequentially
        hook = MsSqlHook(mssql_conn_id=DEFAULT_CONN_ID)
        for d in rest:
            sql = f"EXEC Procedure '{d}'"
            hook.run(sql)
            executed.append(d)

        return {'triggered': triggered, 'executed': executed}

    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    dates = get_dates()
    result = orchestrate(dates)

    start >> dates >> result >> end
