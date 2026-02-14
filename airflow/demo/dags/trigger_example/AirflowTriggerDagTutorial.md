# Airflow Trigger Dag Tutorial

## 1. Цель и контекст

Этот гайд разбирает связку ваших DAG-ов:

- `airflow/demo/dags/trigger_example/dag_trigger_query_by_id_mapped.py`
- `airflow/demo/dags/trigger_example/dag_query_by_id.py`

И отвечает на вопросы:

1. Что делает `TriggerDagRunOperator`.
2. Зачем (и когда) вызывать `execute(context)` вручную.
3. Чем это отличается от отдельной task на `TriggerDagRunOperator`.

## 2. Что делает `TriggerDagRunOperator`

`TriggerDagRunOperator` создает новый DAG Run для целевого DAG (`trigger_dag_id`) и может передать `conf` в этот запуск.

Типичный сценарий:

- родительский DAG вычисляет параметры,
- `TriggerDagRunOperator` запускает дочерний DAG с `conf`,
- дочерний DAG читает эти параметры через `dag_run.conf`.

В вашем коде:

```python
trig = TriggerDagRunOperator(
    task_id="inline_trigger_%s" % id,
    trigger_dag_id="query_by_id",
    conf={"id": id},
    wait_for_completion=False,
)
trig.execute(context)
```

`conf={"id": id}` потом читается в `dag_query_by_id.py`:

```python
run_conf = getattr(dag_run, "conf", None) or {}
item = run_conf.get("id") or context.get("params", {}).get("id") or DEFAULT_ID
```

## 3. Что значит `execute(context)`

В Airflow `execute(context)` это основной метод оператора, который выполняется TaskInstance'ом во время запуска задачи.

Почему передается `context`:

- многие операторы используют runtime-контекст (`task_instance`, `dag_run`, `logical_date`, templating),
- `TriggerDagRunOperator` внутри `execute` использует контекст и пушит служебные значения в XCom.

Поэтому при ручном вызове внутри `PythonOperator` вы должны передать текущий `context`, иначе оператор не получит нужное окружение выполнения.

## 4. Ваш текущий паттерн: плюсы и минусы

Плюсы:

1. Логирование и trigger происходят в одном mapped task instance.
2. Проще "собрать" логику в одном Python callable.

Минусы:

1. В UI нет отдельной task-ноды для trigger-операции, observability ниже.
2. Retry/retry_delay относятся к обертке (`PythonOperator`), а не к отдельной trigger-задаче.
3. Код менее декларативный, чем обычное определение задач в DAG.

## 5. Рекомендуемый airflow-native вариант

Обычно лучше объявить `TriggerDagRunOperator` как самостоятельную mapped task.

Пример для вашего кейса:

```python
items = gen_items_for_trigger()  # [{"id": 10}, {"id": 5}, ...]

trigger_mapped = TriggerDagRunOperator.partial(
    task_id="trigger_query_by_id",
    trigger_dag_id="query_by_id",
    wait_for_completion=False,
    max_active_tis_per_dag=1,
).expand(conf=items)

items >> trigger_mapped
```

Что это дает:

1. Чистая декларативная модель DAG.
2. Отдельные mapped instances именно у `TriggerDagRunOperator`.
3. Более прозрачные логи и статусы в UI.

## 6. Важные параметры `TriggerDagRunOperator`

1. `trigger_dag_id`: какой DAG запускать.
2. `conf`: payload для дочернего DAG (`dag_run.conf`).
3. `wait_for_completion`:
   - `False`: триггернул и пошел дальше.
   - `True`: ждать завершения дочернего DAG run.
4. `reset_dag_run`:
   - при `True` может очистить существующий run с тем же `run_id` и перезапустить.

## 7. Когда все же допустим `trig.execute(context)` вручную

Это допустимо, если вам нужно:

1. строго inline-выполнение операторной логики в сложном Python-сценарии,
2. быстро прототипировать без выделения отдельной task.

Но для production-пайплайнов чаще лучше выделять оператор отдельной задачей.

## 8. Отладка вашей связки DAG-ов

1. Запустите `trigger_query_by_id_mapped`.
2. Убедитесь, что в его логах видны id для каждого mapped instance.
3. Откройте запуски `query_by_id` и проверьте, что `dag_run.conf` содержит ожидаемый `id`.
4. Если нужно блокирующее поведение, переключите `wait_for_completion=True`.

## 9. Короткий итог

1. `TriggerDagRunOperator` нужен для запуска другого DAG с параметрами.
2. `execute(context)` вручную работает, потому что оператор ожидает runtime context Airflow.
3. Для прозрачности и сопровождения обычно лучше отдельная mapped task на `TriggerDagRunOperator`.

## 10. Источники

- TriggerDagRunOperator (Airflow stable): https://airflow.apache.org/docs/apache-airflow-providers-standard/stable/operators/trigger_dag_run.html
- TriggerDagRunOperator source docs (execute implementation): https://airflow.apache.org/docs/apache-airflow/2.10.2/_modules/airflow/operators/trigger_dagrun.html
- Dynamic Task Mapping: https://airflow.apache.org/docs/apache-airflow/stable/authoring-and-scheduling/dynamic-task-mapping.html
- PythonOperator mapping example (`partial`/`expand_kwargs`): https://airflow.apache.org/docs/apache-airflow/2.10.0/authoring-and-scheduling/dynamic-task-mapping.html
- Ваши DAG-файлы: `airflow/demo/dags/trigger_example/dag_trigger_query_by_id_mapped.py`, `airflow/demo/dags/trigger_example/dag_query_by_id.py`
