"""10분 날씨 DAG의 구성을 검증한다."""

from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import WEATHER_10MIN_CRON
from dags.weather_10min import dag


def test_schedule() -> None:
    """10분 cron과 single-run policy를 유지한다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert WEATHER_10MIN_CRON == "*/10 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_are_collector_only() -> None:
    """Live·forecast source를 독립 수집하고 standalone Gold publication은 하지 않는다."""
    expected_tasks = {
        "collect_weather_ultra_short_live",
        "collect_weather_ultra_short_forecast",
    }
    assert set(dag.task_ids) == expected_tasks
    collect_live = dag.get_task("collect_weather_ultra_short_live")
    collect_forecast = dag.get_task("collect_weather_ultra_short_forecast")

    assert collect_live.downstream_task_ids == set()
    assert collect_forecast.downstream_task_ids == set()
    assert "--source weather_ultra_short_live" in collect_live.bash_command
    assert "--source weather_ultra_short_forecast" in collect_forecast.bash_command
