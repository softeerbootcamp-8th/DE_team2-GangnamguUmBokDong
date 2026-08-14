"""Collector Backfill DAG 구성을 검증한다."""

from datetime import timedelta

from airflow.operators.bash import BashOperator

from dags.backfill import dag

from airflow.timetables.simple import NullTimetable

def test_backfill_dag_configuration() -> None:
    """Backfill DAG의 실행 정책을 검증한다."""
    assert dag.dag_id == "collector_backfill"
    assert isinstance(dag.timetable, NullTimetable)
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_backfill_task_contract() -> None:
    """Backfill Collector CLI 계약을 검증한다."""
    assert len(dag.tasks) == 1

    task = dag.tasks[0]

    assert isinstance(task, BashOperator)
    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(minutes=4)

    assert "--source" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "--backfill" in task.bash_command
    assert "--force" not in task.bash_command

    assert "dag_run.conf['source_id']" in task.bash_command
    assert "dag_run.conf['window_start']" in task.bash_command