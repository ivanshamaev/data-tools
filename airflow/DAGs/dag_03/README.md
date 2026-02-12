# Demo DAGs: динамические подходы (Airflow 2.10 и старше)

Папка содержит несколько небольших демо-дагов, показывающих разные подходы работы с динамическим набором задач в Airflow (версии ~2.0–2.10). Для каждого файла ниже кратко указано, что он демонстрирует и на что обращать внимание.

Файлы
- `dag_loop_define.py` — создание тасков в цикле на этапе парсинга DAG (parse-time dynamic tasks).
- `dag_task_mapping.py` — Task Mapping: `@task.map()` и `Operator.partial().expand()` (Airflow 2.3+).
- `dag_taskgroup_dynamic.py` — динамическое создание задач внутри `TaskGroup` для группировки.
- `dag_branching_dynamic.py` — использование `BranchPythonOperator` для выбора ветки на основе данных.
- `dag_trigger_dagrun.py` — триггер других дагов программно (включая mapped trigger примеры).
- `dag_subdag_example.py` — пример `SubDagOperator` (исторический/дисклеймер: обычно не рекомендуется в новых проектах).

