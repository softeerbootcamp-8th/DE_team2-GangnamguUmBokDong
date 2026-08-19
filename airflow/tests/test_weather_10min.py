"""10분 날씨 DAG의 구성을 검증한다."""

from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import WEATHER_10MIN_CRON
from dags.weather_10min import dag


def test_schedule():
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert WEATHER_10MIN_CRON == "*/10 * * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_and_dependency():
    expected_tasks = {
        "collect_weather_ultra_short_live",
        "collect_weather_ultra_short_forecast",
        "publish_weather_forecast",
    }
    assert set(dag.task_ids) == expected_tasks
    collect_live = dag.get_task("collect_weather_ultra_short_live")
    collect_forecast = dag.get_task("collect_weather_ultra_short_forecast")
    publish = dag.get_task("publish_weather_forecast")

    assert collect_live.downstream_task_ids == set()
    assert publish.upstream_task_ids == {collect_forecast.task_id}
    assert "--publication weather-forecast" in publish.bash_command
    assert "--table weather_current" not in publish.bash_command
