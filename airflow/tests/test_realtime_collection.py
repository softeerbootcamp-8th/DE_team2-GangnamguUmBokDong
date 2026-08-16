"""실시간 수집 DAG의 구성과 Collector 실행 계약을 검증한다."""

from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from callbacks.task_callbacks import log_task_failure, log_task_retry
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
    """실시간 source Task + 정규화 Task 2개가 모두 생성되는지 검증한다."""
    expected_task_ids = {f"collect_{source_id}" for source_id in REALTIME_SOURCES} | {
        "normalize_pop_grid",
        "normalize_pop_grid_fallback",
    }

    assert set(dag.task_ids) == expected_task_ids


def test_normalize_pop_grid_depends_on_population_realtime() -> None:
    """normalize_pop_grid는 collect_population_realtime의 다운스트림이어야 한다."""
    collect_population = dag.get_task("collect_population_realtime")
    normalize = dag.get_task("normalize_pop_grid")

    assert normalize.task_id in {t.task_id for t in collect_population.downstream_list}


def test_normalize_pop_grid_fallback_only_runs_after_all_failed() -> None:
    """fallback은 normalize_pop_grid 재시도가 모두 실패했을 때만 실행돼야 한다(strict 실패 시 latest로 재시도)."""
    normalize = dag.get_task("normalize_pop_grid")
    fallback = dag.get_task("normalize_pop_grid_fallback")

    assert fallback.trigger_rule == "all_failed"
    assert fallback.task_id in {t.task_id for t in normalize.downstream_list}
    assert "--baseline-date-mode latest" in fallback.bash_command
    assert "--baseline-date-mode" not in normalize.bash_command


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

    assert task.on_retry_callback == log_task_retry
    assert task.on_failure_callback == log_task_failure