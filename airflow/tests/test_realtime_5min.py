"""5분 coordinated serving DAG의 task 집합과 의존성을 검증한다."""

from datetime import timedelta
from itertools import pairwise

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    EXECUTION_TIMEOUT_OVERRIDES,
    REALTIME_5MIN_CRON,
    TIMEZONE,
    WEATHER_MANIFEST_POKE_INTERVAL_SECONDS,
    WEATHER_MANIFEST_WAIT_TIMEOUT_SECONDS,
)
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from dags.realtime_5min import dag


def test_schedule_and_run_policy() -> None:
    """5분 cron과 single active run을 유지한다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert REALTIME_5MIN_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_only_coordinated_serving_tasks_exist() -> None:
    """Legacy compute/loader/station standalone task가 DAG에 남지 않는다."""
    expected = (
        {f"collect_{source}" for source in REALTIME_5MIN_SOURCES}
        | {
            "run_normalizer",
            "wait_for_weather_manifests",
            "prepare_serving_plan",
            "run_inference",
            "finalize_serving_release",
            "publish_station_urgency",
            "publish_rebalance_route",
        }
        | {
            f"collect_bike_rental_history_replay_{hour}h"
            for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
        }
    )
    assert set(dag.task_ids) == expected
    assert not {
        "publish_station_release",
        "load_forecast_points",
        "compute_urgency",
        "load_station_urgency",
        "compute_routes",
        "load_rebalance_routes",
        "load_rebalance_route_stops",
    }.intersection(dag.task_ids)


def test_prepare_waits_for_station_and_bounded_weather_sensor() -> None:
    """Plan은 station 성공과 날씨 bounded wait 종료 후 준비된다."""
    prepare = dag.get_task("prepare_serving_plan")
    assert prepare.upstream_task_ids == {
        "collect_bike_station_realtime",
        "wait_for_weather_manifests",
    }
    assert prepare.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    sensor = dag.get_task("wait_for_weather_manifests")
    assert isinstance(sensor, BashSensor)
    assert sensor.poke_interval == WEATHER_MANIFEST_POKE_INTERVAL_SECONDS == 2
    assert sensor.timeout == WEATHER_MANIFEST_WAIT_TIMEOUT_SECONDS == 30
    assert sensor.soft_fail is True
    assert sensor.mode == "poke"
    assert sensor.upstream_task_ids == set()
    assert "serving_cli.py weather-ready" in sensor.bash_command
    assert "--project" in sensor.bash_command
    assert "astimezone" in sensor.bash_command


def test_inference_waits_for_plan_normalizer_and_rental_input() -> None:
    """Plan 준비와 실제 inference 계산 input이 inference 합류점에서 만난다."""
    inference = dag.get_task("run_inference")
    assert inference.upstream_task_ids == {
        "prepare_serving_plan",
        "collect_bike_rental_history",
        "run_normalizer",
    }
    assert dag.get_task("run_normalizer").upstream_task_ids == {
        "collect_population_realtime"
    }
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS
    assert "collect_population_realtime" not in inference.upstream_task_ids
    assert "collect_weather_ultra_short_live" not in dag.task_ids


def test_derived_publication_is_one_strict_chain() -> None:
    """Inference 뒤 4-key final·urgency·route가 정확한 ref 순서로 실행된다."""
    assert dag.get_task("finalize_serving_release").upstream_task_ids == {
        "run_inference"
    }
    assert dag.get_task("publish_station_urgency").upstream_task_ids == {
        "finalize_serving_release"
    }
    assert dag.get_task("publish_rebalance_route").upstream_task_ids == {
        "publish_station_urgency"
    }


def test_json_tasks_use_bash_env_contract() -> None:
    """Production task는 append_env와 JSON output processor를 사용한다."""
    for task_id in (
        "prepare_serving_plan",
        "run_inference",
        "finalize_serving_release",
        "publish_station_urgency",
        "publish_rebalance_route",
    ):
        task = dag.get_task(task_id)
        assert isinstance(task, BashOperator)
        assert task.append_env is True
        assert callable(task.output_processor)
    assert dag.get_task("finalize_serving_release").retries == 0
    assert dag.get_task("publish_station_urgency").retries == 0
    assert dag.get_task("publish_rebalance_route").retries == 0


def test_collector_task_execution_contract() -> None:
    """현재 station collector의 retry·timeout·window 계약을 유지한다."""
    task = dag.get_task("collect_bike_station_realtime")

    assert isinstance(task, BashOperator)
    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(seconds=240)
    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert task.bash_command.startswith(
        "env -u VIRTUAL_ENV UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector "
    )


def test_living_population_grid_timeout_override_not_used() -> None:
    """Daily 전용 grid source와 timeout override가 realtime DAG에 섞이지 않는다."""
    assert "collect_living_population_grid" not in dag.task_ids
    assert EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"] == timedelta(
        seconds=1200
    )


def _replay_tasks() -> list[BashOperator]:
    """설정된 lookback 수만큼 rental replay task를 반환한다."""
    return [
        dag.get_task(f"collect_bike_rental_history_replay_{hour}h")
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_lookback_is_one_hour() -> None:
    """현행 API 비용과 late-return 회수율에 맞춘 1시간 lookback을 고정한다."""
    assert RENTAL_HISTORY_LOOKBACK_HOURS == 1


def test_replay_tasks_exist_for_each_lookback_hour() -> None:
    """Lookback 설정 변경 시 DAG replay task 수가 함께 변한다."""
    assert [task.task_id for task in _replay_tasks()] == [
        f"collect_bike_rental_history_replay_{hour}h"
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_replay_uses_force_for_late_returns() -> None:
    """늦은 반납은 실패 backfill이 아니라 whole-window force replay로 회수한다."""
    for task in _replay_tasks():
        assert "--force" in task.bash_command
        assert "--backfill" not in task.bash_command
        assert "--source bike_rental_history" in task.bash_command


def test_replay_window_start_is_shifted_by_whole_hours() -> None:
    """각 replay가 원래 Silver hour partition을 다시 채우도록 시간을 이동한다."""
    for hours, task in enumerate(_replay_tasks(), start=1):
        assert f"timedelta(hours={hours})" in task.bash_command
        assert "astimezone" in task.bash_command


def test_replay_runs_sequentially_to_cap_api_concurrency() -> None:
    """현재 수집과 replay를 직렬화해 API page 동시 요청 수를 제한한다."""
    chain = [dag.get_task("collect_bike_rental_history"), *_replay_tasks()]

    for upstream, downstream in pairwise(chain):
        assert downstream.task_id in upstream.downstream_task_ids


def test_replay_failure_does_not_block_inference() -> None:
    """과거 window 보강 실패는 current serving inference를 막지 않는다."""
    for task in _replay_tasks():
        assert task.trigger_rule == TriggerRule.ALL_DONE
        assert "run_inference" not in task.downstream_task_ids


def test_inference_waits_only_for_current_rental_collection() -> None:
    """Inference 입력에는 current rental collector만 직접 연결한다."""
    upstream = dag.get_task("run_inference").upstream_task_ids

    assert "collect_bike_rental_history" in upstream
    for task in _replay_tasks():
        assert task.task_id not in upstream
