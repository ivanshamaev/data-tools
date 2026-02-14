**Airflow Trigger DAG Guide**

**Overview**: этот документ объясняет поведение двух дагов, находящихся в `airflow/demo/dags/trigger_example` — `query_by_id` и `trigger_query_by_id_mapped`. Сначала детально описан каждый даг по отдельности, затем их взаимодействие, ограничения и рекомендации по настройке.


**trigger_query_by_id_mapped:**

![trigger_query_by_id_mapped](../images/trigger_query_by_id_mapped.png)

**query_by_id:**

![query_by_id](../images/query_by_id.png)

**postgresql_connection:**

![postgresql_connection](../images/postgresql_connection.png)

**Файлы**
- `query_by_id`: [airflow/demo/dags/trigger_example/dag_query_by_id.py](../dag_query_by_id.py)
- `trigger_query_by_id_mapped`: [airflow/demo/dags/trigger_example/dag_trigger_query_by_id_mapped.py](../dag_trigger_query_by_id_mapped.py)

**1) DAG `query_by_id` — что делает и как устроен**

- Назначение: получить входной параметр `id`, выполнить SQL-запрос в PostgreSQL к таблице `af_data.date_list` и залогировать результат.
- Основные параметры и настройки:
  - `dag_id`: `query_by_id`
  - `start_date`: 2026-02-14
  - `schedule_interval`: `None` (ручной/trigger)
  - `catchup`: `False`
  - `max_active_runs`: `1` — ограничивает параллельные запуски этого дага одним.
  - `params`: содержит `{"id": DEFAULT_ID}` — значение по умолчанию.
- Как берётся `id`:
  - Сначала пытается из `dag_run.conf['id']` (т.е. при запуске через trigger с `conf`).
  - Если нет — берётся `params['id']` из определения дага.
  - Если и это отсутствует — используется `DEFAULT_ID` в коде.
- Операция:
  - `PythonOperator` запускает `query_by_id_callable(**context)`.
  - Внутри выполняется SQL:
    - Пример: `SELECT id, event_date FROM af_data.date_list WHERE id = {item}`
    - Используется `PostgresHook` (подключение через `DEFAULT_CONN_ID`, по умолчанию `postgres_default`).
  - Результаты (строки) логируются, если ничего не найдено — логируется соответствующее сообщение.

**Практические замечания по `query_by_id`**
- Убедитесь, что в Airflow настроено соединение `postgres_default` (или измените `DEFAULT_CONN_ID` в файле). Пример CLI:

```bash
airflow connections add postgres_af --conn-uri 'postgresql://postgres:yourpassword@localhost:6432/postgres'
```

- `max_active_runs=1` предотвращает параллельное выполнение нескольких run-ов этого дага; это полезно, если вы не хотите параллельного доступа/нагрузки.

**2) DAG `trigger_query_by_id_mapped` — что делает и как устроен**

- Назначение: сгенерировать набор id и для каждого id в mapped task триггерить `query_by_id`, передавая id через `conf`.
- Основные параметры и настройки:
  - `dag_id`: `trigger_query_by_id_mapped`
  - `start_date`: 2026-02-14
  - `schedule_interval`: `None`
  - `catchup`: `False`
  - `max_active_runs`: `1`
- Структура и логика:
  1. `@task gen_items_for_trigger()` (TaskFlow) генерирует список словарей вида `[{"id": 42}, {"id": 7}, ...]`. Это XComArg, используемый для маппинга.
  2. `trigger_mapped` — mapped `PythonOperator` (создан через `PythonOperator.partial(...).expand(op_kwargs=items)`). Для каждого словаря `op_kwargs` оператор получает аргумент `id`.
  3. Внутри mapped `PythonOperator` (функция `log_and_trigger_callable`) происходит:
     - Логирование: `Triggering DAG 'query_by_id' with id=<id>`
     - Создание экземпляра `TriggerDagRunOperator` и вызов `trig.execute(context)` для немедленного запуска целевого дага с `conf={'id': id}`.
  4. `max_active_tis_per_dag=1` указан в `.partial(...)` — это ограничение на количество одновременно активных Task Instances в родительском даге для этого операторного шаблона. В результате mapped tasks будут выполнены последовательно (по одному активному mapped TI одновременно).

**Почему выбран такой подход**
- Использование `TaskFlow` для генерации `items` даёт удобную XCom-интеграцию: `items` может быть прямо использован в `expand(op_kwargs=items)`.
- Объединение логирования и вызова `TriggerDagRunOperator.execute()` в одном mapped `PythonOperator` даёт гарантию, что лог и фактический триггер будут происходить в одном TI — удобно для трассировки.

**3) Взаимодействие двух дагов — пошагово**

1. Запуск `trigger_query_by_id_mapped`:
   - Создаётся task instance `gen_items_for_trigger` — генерирует список конфов `items` и пушит их в XCom.
2. Scheduler создаёт mapped Task Instances для `trigger_query_by_id` — один TI на словарь `{"id": X}`.
3. По очереди (из‑за `max_active_tis_per_dag=1`) запускается mapped TI:
   - TI вызывает `log_and_trigger_callable(id=X)`.
   - Функция логирует сообщение о триггере и создает `TriggerDagRunOperator` инлайн.
   - `TriggerDagRunOperator.execute(context)` создает новый DAG run для `query_by_id` и передаёт `conf={'id': X}`.
4. Когда `query_by_id` получает запуск с `conf`, его `query_by_id_callable` извлекает `id` из `dag_run.conf` и выполняет SQL-запрос к Postgres, логируя результат.

Параллельность и ограничения:
- `trigger_query_by_id_mapped` ограничен `max_active_runs=1` и mapped оператор имеет `max_active_tis_per_dag=1`, значит mapped TIs выполняются последовательно.
- `query_by_id` имеет `max_active_runs=1` — это дополнительно гарантирует, что не будет параллельных run-ов целевого дага. Если требуется, чтобы родитель ждал завершения дочернего дага, можно установить `wait_for_completion=True` у `TriggerDagRunOperator`.

Поведение при `wait_for_completion`:
- `wait_for_completion=False` (текущая реализация) — parent TI логирует и инициирует запуск дочернего дага, не ожидая его завершения. Parent продолжает и завершается, в то время как child DAG runs выполняется независимо.
- `wait_for_completion=True` — parent TI будет блокироваться до завершения триггернутого DAG run (опционально с `poke_interval`). Это увеличит время выполнения parent TI и поведение должно учитываться при расчёте `timeout` и `retry` политик.

Рекомендации и лучшие практики
- Настройте корректное соединение в Airflow (`Connections`) для Postgres и проверьте `DEFAULT_CONN_ID`.
- Если важно, чтобы каждый триггер ждал результата дочернего дага — используйте `wait_for_completion=True` и контролируйте `allowed_states`/`poke_interval`.
- Если хотите, чтобы parent создавал только один дочерний запуск одновременно и ждал результата — `max_active_runs=1` в child и `wait_for_completion=True` в TriggerDagRunOperator дадут нужный эффект.
- Логируйте уникальные идентификаторы (run_id, conf) — упрощает трассировку.
- При большом числе `items` подумайте о дедупликации/ограничении диапазона, чтобы не перегрузить базу.

Отладка и проверка
- В web UI можно смотреть слои Execution Date / Run Ids для parent и child DAGs.
- Используйте `airflow dags trigger query_by_id --conf '{"id": 5}'` для ручного триггера и проверки поведения целевого дага.

Заключение

Эта связка покрывает распространённую задачу: сгенерировать набор значений и последовательно инициировать отдельные DAG run-ы для каждого значения, при этом сохраняя простоту трассировки (лог в mapped TI) и контроль параллелизма через `max_active_tis_per_dag` и `max_active_runs`.

Если хотите, могу добавить раздел «варианты» с примерами `wait_for_completion=True`, или показать пример конфигурации Airflow Connection и пример запуска в CLI. Также могу переместить этот файл в `docs/` или корень репозитория — скажите, куда сохранить.
