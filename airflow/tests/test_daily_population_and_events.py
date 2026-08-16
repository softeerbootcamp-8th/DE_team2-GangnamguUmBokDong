"""1일 주기 DAG(생활인구+nowcasting / 문화행사)의 구성을 검증한다."""

from datetime import timedelta

from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import (
    DAILY_CRON,
    EXECUTION_TIMEOUT_OVERRIDES,
    NOWCASTING_EXECUTION_TIMEOUT,
)
from dags.daily_population_and_events import dag


def test_schedule():
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert DAILY_CRON == "0 3 * * *"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_expected_tasks():
    assert set(dag.task_ids) == {
        "collect_living_population_grid",
        "run_nowcasting_estimate",
        "collect_cultural_event",
        "load_cultural_events",
    }


def test_population_branch_independent_of_event_branch():
    collect_population = dag.get_task("collect_living_population_grid")
    collect_events = dag.get_task("collect_cultural_event")
    assert collect_events.task_id not in {t.task_id for t in collect_population.downstream_list}
    assert collect_population.task_id not in {t.task_id for t in collect_events.downstream_list}


def test_population_then_nowcasting():
    collect_population = dag.get_task("collect_living_population_grid")
    nowcasting = dag.get_task("run_nowcasting_estimate")
    assert nowcasting.task_id in {t.task_id for t in collect_population.downstream_list}
    assert nowcasting.execution_timeout == NOWCASTING_EXECUTION_TIMEOUT


def test_events_then_load_cultural_events():
    collect_events = dag.get_task("collect_cultural_event")
    load_events = dag.get_task("load_cultural_events")
    assert load_events.task_id in {t.task_id for t in collect_events.downstream_list}
    assert "--table cultural_events" in load_events.bash_command


def test_living_population_grid_uses_long_timeout():
    collect_population = dag.get_task("collect_living_population_grid")
    assert collect_population.execution_timeout == EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"]
    assert collect_population.execution_timeout == timedelta(seconds=1200)
