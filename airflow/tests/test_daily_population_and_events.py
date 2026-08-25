"""1일 주기 DAG(생활인구+nowcasting / 문화행사)의 구성을 검증한다."""

from datetime import timedelta

from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    DAILY_CRON,
    DAILY_POPULATION_RETRIES,
    DAILY_POPULATION_RETRY_DELAY,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
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
        "publish_event_cultural_event",
        "collect_performance_event",
        "publish_event_performance_event",
    }


def test_population_branch_independent_of_event_branch():
    collect_population = dag.get_task("collect_living_population_grid")
    collect_events = dag.get_task("collect_cultural_event")
    assert collect_events.task_id not in {
        t.task_id for t in collect_population.downstream_list
    }
    assert collect_population.task_id not in {
        t.task_id for t in collect_events.downstream_list
    }


def test_population_then_nowcasting():
    collect_population = dag.get_task("collect_living_population_grid")
    nowcasting = dag.get_task("run_nowcasting_estimate")
    assert nowcasting.task_id in {t.task_id for t in collect_population.downstream_list}
    assert nowcasting.execution_timeout == NOWCASTING_EXECUTION_TIMEOUT


def test_events_then_publish_source_scoped_projection():
    collect_events = dag.get_task("collect_cultural_event")
    publish_events = dag.get_task("publish_event_cultural_event")
    collect_performance = dag.get_task("collect_performance_event")
    publish_performance = dag.get_task("publish_event_performance_event")

    assert publish_events.upstream_task_ids == {collect_events.task_id}
    assert "--publication event:cultural_event" in publish_events.bash_command
    assert publish_performance.upstream_task_ids == {collect_performance.task_id}
    assert "--publication event:performance_event" in publish_performance.bash_command


def test_living_population_grid_uses_long_timeout():
    collect_population = dag.get_task("collect_living_population_grid")
    assert (
        collect_population.execution_timeout
        == EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"]
    )
    assert collect_population.execution_timeout == timedelta(seconds=1200)


def test_nowcaster_receives_same_exact_window_as_collector():
    """생활인구 actual 승격은 날짜 prefix가 아니라 같은 logical window를 사용한다."""
    task = dag.get_task("run_nowcasting_estimate")

    assert "--target-date" in task.bash_command
    assert "--source-window-start" in task.bash_command


def test_living_population_grid_uses_same_day_retry_budget():
    """과거 재조회가 불가능한 생활인구는 당일 재시도 시간을 충분히 확보한다."""
    collect_population = dag.get_task("collect_living_population_grid")

    assert collect_population.retries == DAILY_POPULATION_RETRIES == 4
    assert (
        collect_population.retry_delay
        == DAILY_POPULATION_RETRY_DELAY
        == timedelta(minutes=10)
    )
    assert "--force" not in collect_population.bash_command
    assert "--backfill" not in collect_population.bash_command


def test_event_collectors_keep_default_retry_policy():
    """행사 collector는 공용 retry와 기존 일반 수집 명령을 유지한다."""
    for source_id in ("cultural_event", "performance_event"):
        task = dag.get_task(f"collect_{source_id}")

        assert task.retries == DEFAULT_RETRIES
        assert task.retry_delay == DEFAULT_RETRY_DELAY
        assert "--force" not in task.bash_command
        assert "--backfill" not in task.bash_command
