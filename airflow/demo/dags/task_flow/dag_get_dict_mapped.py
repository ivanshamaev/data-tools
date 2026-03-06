"""
DAG: get_dict_mapped

Цепочка: task_get_dict -> [task_map_processing_1, task_map_processing_2].
task_get_dict возвращает словарь с dict_1 и dict_2; каждый используется
для запуска mapped tasks в соответствующей задаче.
"""
from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task


@dag(
    dag_id="get_dict_mapped",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["task_flow", "mapped"],
)
def dag_get_dict_mapped():
    @task(multiple_outputs=True)
    def task_get_dict():
        """Возвращает словарь {'dict_1': {}, 'dict_2': {}}; значения — списки для expand."""
        return {
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

    def make_extract(key: str, task_id_suffix: str):
        """Фабрика: одна логика извлечения, разный task_id для каждой ветки."""
        @task(task_id=f"extract_for_mapping_{task_id_suffix}")
        def extract(data):
            return data[key]
        return extract

    extract_for_mapping_1 = make_extract("dict_1", "1")
    extract_for_mapping_2 = make_extract("dict_2", "2")

    @task(max_active_tis_per_dag=1)
    def task_map_processing_1(**kwargs):
        """Mapped task: обрабатывает один набор kwargs из dict_1."""
        print("task_map_processing_1:", kwargs["op_kwargs"])

    @task(max_active_tis_per_dag=1)
    def task_map_processing_2(**kwargs):
        """Mapped task: обрабатывает один набор kwargs из dict_2."""
        print("task_map_processing_2:", kwargs["op_kwargs"])

    data = task_get_dict()
    items_1 = extract_for_mapping_1(data)
    items_2 = extract_for_mapping_2(data)

    # Подставляем значения dict_1 и dict_2 через промежуточные задачи (прямой XCom для expand)
    mapped_1 = task_map_processing_1.expand(op_kwargs=items_1)
    mapped_2 = task_map_processing_2.expand(op_kwargs=items_2)

    data >> [items_1, items_2]
    items_1 >> mapped_1
    items_2 >> mapped_2


dag_get_dict_mapped()
