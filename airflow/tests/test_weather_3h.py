"""3시간 날씨예보 DAG의 구성을 검증한다."""

from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import WEATHER_3H_CRON
from dags.weather_3h import dag


def test_schedule():
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert WEATHER_3H_CRON == "0 */3 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_tasks_and_dependency():
    assert set(dag.task_ids) == {"collect_weather_short_term_forecast", "load_weather_forecast"}
    collect = dag.get_task("collect_weather_short_term_forecast")
    load = dag.get_task("load_weather_forecast")
    assert load.task_id in {t.task_id for t in collect.downstream_list}
    assert "--table weather_forecast" in load.bash_command
