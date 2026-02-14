# Airflow Mapped Tasks Tutorial

## 1. Что вы строите в этом примере

В папке `airflow/demo/dags/trigger_example` у вас есть DAG `trigger_query_by_id_mapped`, который:

1. генерирует список `id`,
2. создает mapped task instances,
3. для каждого `id` выполняет вызов Python-функции.

Ключевая строка:

```python
trigger_mapped = PythonOperator.partial(
    task_id="trigger_query_by_id",
    python_callable=log_and_trigger_callable,
    max_active_tis_per_dag=1,
).expand(op_kwargs=items)
```

## 2. Как работает Dynamic Task Mapping

По официальной документации Airflow Dynamic Task Mapping:

- `partial(...)` задает аргументы, которые **не мапятся** (статические для всех копий task).
- `expand(...)` задает аргументы, которые **мапятся** (по ним Airflow создаст N task instances во время выполнения).

Важно:

- `partial()` принимает только keyword-аргументы.
- для classic operators (включая `PythonOperator`) часть полей нельзя мапить и их нужно передавать именно в `partial()` (`task_id`, `pool`, `queue` и другие поля `BaseOperator`).

## 3. Что значит `.partial(...)` в вашем DAG

В вашем коде `PythonOperator.partial(...)` фиксирует общие параметры для всех mapped копий:

- `task_id="trigger_query_by_id"`
- `python_callable=log_and_trigger_callable`
- `max_active_tis_per_dag=1`

То есть это "шаблон" оператора до разворачивания.

## 4. Что значит `.expand(op_kwargs=items)`

В вашем DAG `items` приходит из TaskFlow-задачи:

```python
@task
def gen_items_for_trigger() -> list[dict]:
    values = sorted([randint(1, 50) for _ in range(1, randint(3, 10))], reverse=True)
    items = [{"id": v} for v in values]
    return items
```

`items` имеет вид:

```python
[{"id": 48}, {"id": 33}, {"id": 7}]
```

Когда вы пишете:

```python
.expand(op_kwargs=items)
```

Airflow делает по сути следующее:

1. создает 3 mapped task instances,
2. в каждой копии подставляет свой `op_kwargs`,
3. вызывает `python_callable` как `log_and_trigger_callable(id=<value>, **context)`.

Эквивалент в терминах обычного цикла:

```python
for kw in [{"id": 48}, {"id": 33}, {"id": 7}]:
    log_and_trigger_callable(**kw)
```

Только в Airflow это отдельные task instances с собственным `map_index`, логами и retry-поведением.

## 5. Зачем здесь `max_active_tis_per_dag=1`

Ваша mapped-задача ограничена:

```python
max_active_tis_per_dag=1
```

Это значит, что одновременно будет исполняться только 1 mapped instance этой задачи. Остальные будут ждать.

Практический эффект:

- меньше параллельной нагрузки на БД и scheduler,
- более предсказуемый порядок триггеров (хотя общий порядок dynamic mapping не гарантируется документацией как жесткий контракт).

## 6. `expand(op_kwargs=items)` vs `expand_kwargs(...)`

Оба подхода рабочие, но применяются чуть по-разному:

- `expand(op_kwargs=items)`:
  - удобно, когда хотите мапить именно `op_kwargs` целиком.
- `expand_kwargs(...)`:
  - удобно, когда нужно мапить сразу набор параметров самого оператора (`op_kwargs`, `show_return_value_in_logs`, и т.д.).

В документации есть пример для `PythonOperator` через `expand_kwargs` с полем `op_kwargs`.

## 7. Как это связано с вашим DAG-ом

Связка из ваших файлов:

- `dag_trigger_query_by_id_mapped.py` генерирует `items` и выполняет mapped запуск.
- `dag_query_by_id.py` принимает `id` (через `dag_run.conf` или `params`) и делает SQL-запрос.

То есть mapped-часть отвечает за fan-out, а `query_by_id` отвечает за бизнес-логику по конкретному `id`.

## 8. Проверка в UI

Для `trigger_query_by_id_mapped` в Grid View:

1. запустите DAG,
2. откройте таску `trigger_query_by_id`,
3. увидите набор mapped instances c индексами (`0`, `1`, `2`, ...),
4. в каждом instance проверьте лог с конкретным `id`.

## 9. Типичные ошибки

1. В `expand(...)` передан не list/dict/XComArg с list/dict.
2. Пытаетесь мапить поля, которые должны быть статичными (`task_id` и т.д.).
3. Пустой список на входе. В этом случае Airflow автоматически скипнет mapped task.

## 10. Рекомендуемые улучшения для этого примера

1. Убрать неиспользуемую функцию `log_trigger_callable`.
2. Для SQL-запроса в `dag_query_by_id.py` использовать параметризацию, а не f-string.
3. Если нужна высокая прозрачность, триггерить дочерний DAG отдельным `TriggerDagRunOperator` mapped task (см. второй гайд).

## 11. Источники

- Dynamic Task Mapping (Airflow stable): https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
- Dynamic Task Mapping (Airflow 2.10): https://airflow.apache.org/docs/apache-airflow/2.10.0/authoring-and-scheduling/dynamic-task-mapping.html
- Ваш пример DAG: `airflow/demo/dags/trigger_example/dag_trigger_query_by_id_mapped.py`
