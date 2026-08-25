"""POI Master 일일 갱신 DAG의 스케줄과 단일 책임을 검증한다."""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import POI_MASTER_REFRESH_CRON, TIMEZONE
from dags.poi_master_refresh import dag


def test_schedule_is_daily_kst_without_catchup() -> None:
    """매일 02:04 KST에 자동 활성 상태로 중첩 없이 변경 확인을 실행한다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert POI_MASTER_REFRESH_CRON == "4 2 * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is False


def test_dag_contains_only_the_refresh_cli_task() -> None:
    """파일 판정 로직을 Airflow에 중복하지 않고 POI Master CLI 하나만 호출한다."""
    assert dag.task_ids == ["refresh_poi_master"]
    task = dag.get_task("refresh_poi_master")

    assert isinstance(task, BashOperator)
    assert "python main.py refresh" in task.bash_command
    assert callable(task.output_processor)
    assert task.upstream_task_ids == set()
    assert task.downstream_task_ids == set()
