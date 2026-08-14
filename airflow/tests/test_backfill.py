"""Collector Backfill DAG 구성을 검증한다."""

from airflow.models.mappedoperator import MappedOperator
from airflow.timetables.simple import NullTimetable

from dags.backfill import BACKFILL_SOURCE_ID, dag


def test_backfill_dag_configuration() -> None:
    """Backfill DAG의 실행 정책을 검증한다."""
    assert dag.dag_id == "collector_backfill"
    assert isinstance(dag.timetable, NullTimetable)
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_backfill_tasks_exist() -> None:
    """백필 대상 조회와 동적 백필 실행 Task가 생성되는지 검증한다."""
    assert set(dag.task_ids) == {
        "list_backfill_targets",
        "run_backfill",
    }


def test_backfill_task_mapping_contract() -> None:
    """백필 실행 Task가 대상 목록을 기반으로 동적 매핑되는지 검증한다."""
    task = dag.get_task("run_backfill")

    assert isinstance(task, MappedOperator)
    assert task.upstream_task_ids == {"list_backfill_targets"}


def test_backfill_source_contract() -> None:
    """현재 Backfill 대상 source가 Collector 계약과 일치하는지 검증한다."""
    assert BACKFILL_SOURCE_ID == "bike_station_realtime"