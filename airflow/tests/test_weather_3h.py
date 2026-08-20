"""3시간 날씨예보 DAG의 구성을 검증한다."""

from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import WEATHER_3H_CRON
from dags.weather_3h import dag


def test_schedule() -> None:
    """3시간 cron과 single-run policy를 유지한다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert WEATHER_3H_CRON == "0 */3 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_task_is_collector_only() -> None:
    """Short-term source만 수집하고 standalone Gold publication은 하지 않는다."""
    assert set(dag.task_ids) == {"collect_weather_short_term_forecast"}
    collect = dag.get_task("collect_weather_short_term_forecast")

    assert collect.downstream_task_ids == set()
    assert "--source weather_short_term_forecast" in collect.bash_command
