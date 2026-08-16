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
        "collect_weather_ultra_short_live", "load_weather_current",
        "collect_weather_ultra_short_forecast", "load_weather_forecast_ultra",
    }
    assert set(dag.task_ids) == expected_tasks
    collect = dag.get_task("collect_weather_ultra_short_live")
    load = dag.get_task("load_weather_current")
    assert load.task_id in {t.task_id for t in collect.downstream_list}
    assert "--table weather_current" in load.bash_command
