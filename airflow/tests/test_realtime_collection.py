"""실시간 수집 DAG의 구성과 Collector 실행 계약을 검증한다."""

from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import REALTIME_CRON, TIMEZONE
from config.sources import REALTIME_SOURCES
from dags.realtime_collection import dag


def test_realtime_collection_schedule() -> None:
    """실시간 DAG의 스케줄과 실행 정책을 검증한다."""
    assert dag.dag_id == "realtime_collection"
    assert isinstance(dag.timetable, CronTriggerTimetable)

    assert REALTIME_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"

    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_realtime_collection_tasks() -> None:
    """실시간 source마다 Collector Task가 생성되는지 검증한다."""
    expected_task_ids = {
        f"collect_{source_id}"
        for source_id in REALTIME_SOURCES
    }

    assert set(dag.task_ids) == expected_task_ids


def test_collector_task_execution_contract() -> None:
    """Collector CLI 계약과 Airflow 재시도 정책을 검증한다."""
    task = dag.get_task("collect_bike_station_realtime")

    assert isinstance(task, BashOperator)

    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(minutes=4)

    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "logical_date" in task.bash_command

    assert "--run-id" not in task.bash_command