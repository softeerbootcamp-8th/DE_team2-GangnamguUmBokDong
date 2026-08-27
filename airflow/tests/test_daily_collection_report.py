"""일별 데이터 수집 리포트 DAG의 스케줄과 태스크 구성을 검증한다."""

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import DAILY_COLLECTION_REPORT_CRON, TIMEZONE
from config.sources import COLD_BRONZE_SOURCES
from dags.daily_collection_report import dag


def test_schedule_is_daily_kst_at_seven_without_catchup() -> None:
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert DAILY_COLLECTION_REPORT_CRON == "0 7 * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.is_paused_upon_creation is False


def test_every_source_has_a_stats_task_feeding_the_report() -> None:
    report = dag.get_task("send_daily_report")
    assert isinstance(report, PythonOperator)

    for source_id in COLD_BRONZE_SOURCES:
        task = dag.get_task(f"collection_stats_{source_id}")
        assert isinstance(task, BashOperator)
        assert f"--source {source_id}" in task.bash_command
        assert "--window-hour" not in task.bash_command
        assert task.downstream_task_ids == {"send_daily_report"}

    assert report.upstream_task_ids == {
        f"collection_stats_{source_id}" for source_id in COLD_BRONZE_SOURCES
    }
