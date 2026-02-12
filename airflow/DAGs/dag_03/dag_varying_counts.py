from __future__ import annotations

from datetime import datetime
import json
from typing import List

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable

"""Demo DAG: количество задач меняется от запуска к запуску.

Механика:
- Список чисел (например `[3,10,7]`) хранится в Airflow Variable `dynamic_counts`.
- Текущий индекс хранится в Airflow Variable `dynamic_counts_idx`.
- При каждом запуске читаем `counts[idx]`, увеличиваем `idx` и сохраняем.
- Создаём `count` элементов (0..count-1) и применяем `process.map(items)` — Task Mapping создаёт нужное число задач.

Инициализация переменных (пример):
```bash
airflow variables set dynamic_counts '[3,10,7]'
airflow variables set dynamic_counts_idx 0
```
"""

with DAG(dag_id='dag_varying_counts', start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False, tags=['demo','dynamic']) as dag:

    @task
    def get_items() -> List[int]:
        raw = Variable.get('dynamic_counts', default_var='[3,10,7]')
        try:
            counts = json.loads(raw)
            if not isinstance(counts, list) or not counts:
                counts = [3, 10, 7]
        except Exception:
            counts = [3, 10, 7]

        idx_raw = Variable.get('dynamic_counts_idx', default_var='0')
        try:
            idx = int(idx_raw)
        except Exception:
            idx = 0

        count = int(counts[idx % len(counts)])

        # Обновляем индекс для следующего запуска
        next_idx = (idx + 1) % (10 ** 9)
        Variable.set('dynamic_counts_idx', str(next_idx))

        # Возвращаем список элементов для mapping
        return list(range(count))

    @task
    def process(i: int) -> str:
        # Простая обработка — в реале сюда идёт бизнес-логика
        msg = f'processed {i}'
        print(msg)
        return msg

    items = get_items()
    results = process.map(items)
