"""시간별 데이터 수집 이상 감지 DAG의 스케줄과 태스크 구성을 검증한다."""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import HOURLY_COLLECTION_ALERT_CRON, TIMEZONE
from config.sources import COLD_BRONZE_SOURCES, HOURLY_MONITORED_SOURCES
from dags.hourly_collection_alert import dag


def test_schedule_is_hourly_kst_without_catchup() -> None:
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert HOURLY_COLLECTION_ALERT_CRON == "0 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is False


def test_only_hourly_monitored_sources_have_stats_tasks() -> None:
    assert set(HOURLY_MONITORED_SOURCES) < set(COLD_BRONZE_SOURCES)

    alert = dag.get_task("send_hourly_alert")
    assert isinstance(alert, PythonOperator)

    for source_id in HOURLY_MONITORED_SOURCES:
        task = dag.get_task(f"collection_stats_{source_id}")
        assert isinstance(task, BashOperator)
        assert f"--source {source_id}" in task.bash_command
        assert "--window-hour" in task.bash_command
        assert task.downstream_task_ids == {"send_hourly_alert"}

    assert alert.upstream_task_ids == {
        f"collection_stats_{source_id}" for source_id in HOURLY_MONITORED_SOURCES
    }
