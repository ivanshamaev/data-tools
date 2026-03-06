# Task Flow: get_dict_mapped — теория и практика

Два варианта DAG (`dag_get_dict_mapped.py` и `dag_get_dict_mapped_v2.py`) демонстрируют цепочку задач с **mapped tasks** в Airflow TaskFlow API. В этом документе — теория и детальное описание решений.

---

## 1. Обзор архитектуры

Оба DAG реализуют одну и ту же логику:

```
task_get_dict
      │
      ├──► extract_for_mapping_1 ──► task_map_processing_1 (mapped × 3)
      │
      └──► extract_for_mapping_2 ──► task_map_processing_2 (mapped × 2)
```

- **task_get_dict** — возвращает словарь с двумя ключами: `dict_1` и `dict_2`, каждый содержит список словарей (наборы `op_kwargs` для mapped tasks).
- **extract_for_mapping_1 / 2** — промежуточные задачи, извлекающие нужный список из результата `task_get_dict`.
- **task_map_processing_1 / 2** — mapped tasks, создающие по одному экземпляру на каждый элемент списка.

---

## 2. Теория: TaskFlow API и декоратор @dag

### Зачем всё обёрнуто в функцию `def dag_get_dict_mapped()`?

Декоратор `@dag` ожидает **вызываемый объект** (функцию). При загрузке DAG Airflow **вызывает** эту функцию, и её тело выполняется в момент парсинга.

**Причины такого подхода:**

1. **Фабрика DAG** — функция выступает как «конструктор»: при каждом парсинге создаются свежие экземпляры задач и связей.

2. **Область видимости** — все `@task` и вызовы задач локальны для DAG. Это снижает риск конфликтов имён при загрузке нескольких DAG-файлов.

3. **Создание задач** — `@task` превращает функцию в фабрику задач. Сама задача создаётся при **вызове** (например, `task_get_dict()`). Эти вызовы должны выполняться при парсинге, поэтому их помещают в функцию, которую вызывает `@dag`.

**Альтернатива** — контекстный менеджер `with DAG(...) as dag:`. Декоратор `@dag` — это синтаксически удобный способ задать DAG через функцию.

---

## 3. Теория: Mapped tasks и expand()

### Что такое mapped task?

**Mapped task** — это задача, которая создаёт несколько экземпляров (Task Instances) на основе набора входных данных. Вместо одной задачи выполняется N задач, каждая с собственным набором аргументов.

### expand(op_kwargs=...)

Метод `.expand(op_kwargs=items)` принимает **список словарей**. Каждый словарь в списке передаётся как `**kwargs` в один экземпляр mapped task.

**Пример:**
```python
items = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
task.expand(op_kwargs=items)  # 2 экземпляра: один с id=1, другой с id=2
```

### Формат данных для expand

Для `expand(op_kwargs=...)` нужен **список словарей**:
```python
[
    {"key1": value1, "key2": value2},  # kwargs для экземпляра 1
    {"key1": value3, "key2": value4},  # kwargs для экземпляра 2
    ...
]
```

Поэтому `task_get_dict` возвращает не `{"dict_1": {}, "dict_2": {}}`, а `{"dict_1": [{}, ...], "dict_2": [{}, ...]}` — списки словарей для `expand`.

---

## 4. Теория: XCom и ограничение «custom key»

### Проблема

При вызове `task_get_dict()` с `multiple_outputs=True` результат — это XComArg, представляющий словарь. Доступ по ключу: `data["dict_1"]` даёт **XComArg с custom key**.

**Airflow не поддерживает `expand()` над таким XComArg:**

```
ValueError: cannot map over XCom with custom key 'dict_1' from <Task: task_get_dict>
```

`expand()` ожидает **прямой** XComArg — результат задачи без доступа по ключу.

### Решение: промежуточные задачи extract

Добавляем задачи, которые принимают полный вывод `task_get_dict` и возвращают **только нужный список**. Их результат — прямой XComArg, подходящий для `expand()`:

```python
@task
def extract(data):
    return data["dict_1"]  # возвращает список, не XComArg с ключом

items = extract(data)  # items — прямой XComArg
mapped = task.expand(op_kwargs=items)  # ✓ работает
```

---

## 5. Теория: фабрика make_extract

### Зачем одна функция вместо двух?

Вместо двух почти одинаковых задач:
```python
@task
def extract_for_mapping_1(data):
    return data["dict_1"]

@task
def extract_for_mapping_2(data):
    return data["dict_2"]
```

используется **фабрика** — функция, возвращающая задачу с нужным `task_id`:

```python
def make_extract(key: str, task_id_suffix: str):
    @task(task_id=f"extract_for_mapping_{task_id_suffix}")
    def extract(data):
        return data[key]
    return extract

extract_for_mapping_1 = make_extract("dict_1", "1")
extract_for_mapping_2 = make_extract("dict_2", "2")
```

**Почему нужен `task_id`?** При повторном вызове одной и той же функции Airflow создаёт задачи с одинаковым `task_id`, что приводит к конфликту. Фабрика генерирует уникальные `task_id` для каждой ветки.

---

## 6. Сравнение версий DAG

### dag_get_dict_mapped.py (v1)

**Подход:** явные переменные для extract и зависимостей.

```python
extract_for_mapping_1 = make_extract("dict_1", "1")
extract_for_mapping_2 = make_extract("dict_2", "2")

data = task_get_dict()
items_1 = extract_for_mapping_1(data)
items_2 = extract_for_mapping_2(data)

mapped_1 = task_map_processing_1.expand(op_kwargs=items_1)
mapped_2 = task_map_processing_2.expand(op_kwargs=items_2)

data >> [items_1, items_2]
items_1 >> mapped_1
items_2 >> mapped_2
```

**Плюсы:**
- Явные зависимости
- Легко читать и отлаживать
- Понятно, какие задачи от чего зависят

### dag_get_dict_mapped_v2.py (v2)

**Подход:** вызов `make_extract(...)(data)` передаётся сразу в `expand()`.

```python
data = task_get_dict()
mapped_1 = task_map_processing_1.expand(op_kwargs=make_extract("dict_1", "1")(data))
mapped_2 = task_map_processing_2.expand(op_kwargs=make_extract("dict_2", "2")(data))
```

**Плюсы:**
- Меньше кода
- Нет промежуточных переменных
- Зависимости задаются автоматически: `data` передаётся в extract, `expand()` связывает extract с mapped

**Как задаются зависимости в v2:**
1. `make_extract("dict_1", "1")(data)` — extract получает `data` как вход → `data >> extract`.
2. `expand(op_kwargs=...)` — mapped task зависит от вывода extract → `extract >> mapped`.

---

## 7. max_active_tis_per_dag=1

Оба DAG используют `@task(max_active_tis_per_dag=1)` для mapped tasks. Это ограничивает выполнение **одним** активным экземпляром mapped task на DAG run. Экземпляры выполняются последовательно, а не параллельно — полезно при ограниченных ресурсах или при необходимости строгого порядка.

---

## 8. Структура данных в примере

```python
{
    "dict_1": [
        {"id": 1, "name": "item_a", "value": 10},
        {"id": 2, "name": "item_b", "value": 20},
        {"id": 3, "name": "item_c", "value": 30},
    ],
    "dict_2": [
        {"key": "x", "count": 100},
        {"key": "y", "count": 200},
    ],
}
```

- **task_map_processing_1** — 3 экземпляра (по одному на каждый элемент `dict_1`).
- **task_map_processing_2** — 2 экземпляра (по одному на каждый элемент `dict_2`).

В `kwargs` каждой mapped task попадает соответствующий словарь (через `kwargs["op_kwargs"]` в текущей реализации).

---

## 9. Резюме

| Аспект | Описание |
|--------|----------|
| **@dag + функция** | Функция — фабрика DAG, вызывается при парсинге |
| **expand(op_kwargs=...)** | Требует список словарей; каждый словарь → один экземпляр mapped task |
| **XCom custom key** | `data["key"]` нельзя использовать в `expand()` — нужны промежуточные задачи |
| **make_extract** | Фабрика для одной логики извлечения с разными `task_id` |
| **v1 vs v2** | v1 — явные переменные и зависимости; v2 — inline-вызов в `expand()` |
