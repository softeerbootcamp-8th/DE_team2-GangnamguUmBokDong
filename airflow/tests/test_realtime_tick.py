"""realtime tick 4-DAG 분할(구 realtime_5min + weather_10min/weather_3h 흡수)을 검증한다."""

from datetime import datetime, timedelta
from itertools import pairwise

import pytest
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    EXECUTION_TIMEOUT_OVERRIDES,
    REALTIME_TICK_CRON,
    REALTIME_TICK_FULL_WEATHER_CRON,
    REALTIME_TICK_ULTRA_WEATHER_CRON,
    REALTIME_TICK_ULTRA_WEATHER_ON_HOUR_CRON,
    TIMEZONE,
)
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from croniter import croniter
from dags.realtime_tick import (
    dag,
    dag_full_weather,
    dag_ultra_weather,
    dag_ultra_weather_on_hour,
)

_ALL_DAGS = (dag, dag_ultra_weather, dag_ultra_weather_on_hour, dag_full_weather)
_ALL_CRONS = (
    REALTIME_TICK_CRON,
    REALTIME_TICK_ULTRA_WEATHER_CRON,
    REALTIME_TICK_ULTRA_WEATHER_ON_HOUR_CRON,
    REALTIME_TICK_FULL_WEATHER_CRON,
)


def _old_boundary_group(minute: int, hour: int) -> int:
    """구 `weather_sources_ready`의 분·시 나머지 연산 경계를 그대로 재현한다.

    0=날씨 없음, 1=초단기만(매시 10~50분), 2=초단기만(정시, 3시간 경계 아님),
    3=초단기+단기(정시, 3시간 경계).
    """
    if minute % 10 != 0:
        return 0
    if minute == 0 and hour % 3 == 0:
        return 3
    if minute == 0:
        return 2
    return 1


def test_cron_partition_matches_old_five_minute_grid_exactly() -> None:
    """4개 cron의 합집합이 구 `*/5 * * * *`의 모든 tick과 정확히 같고, 서로 겹치지 않는다."""
    start = datetime(2026, 8, 17, 0, 0)
    end = start + timedelta(days=3)

    fires_by_cron = []
    for cron in _ALL_CRONS:
        it = croniter(cron, start)
        fires = []
        while True:
            nxt = it.get_next(datetime)
            if nxt >= end:
                break
            fires.append(nxt)
        fires_by_cron.append(set(fires))

    union = set().union(*fires_by_cron)
    for a, b in pairwise(fires_by_cron):
        assert a.isdisjoint(b)

    expected = set()
    it = croniter("*/5 * * * *", start)
    while True:
        nxt = it.get_next(datetime)
        if nxt >= end:
            break
        expected.add(nxt)
    assert union == expected

    # 그룹 배정이 실제로 옛 boundary 공식(분%10, 시%3)과 일치하는지도 확인한다 —
    # 그냥 "안 겹친다"만으론 날씨가 필요 없는 시각에 엉뚱하게 배정될 수 있다.
    for group_index, fires in enumerate(fires_by_cron):
        for ts in fires:
            assert _old_boundary_group(ts.minute, ts.hour) == group_index


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_schedule_and_run_policy(target_dag) -> None:
    """4개 DAG 모두 catchup 없이 single active run으로 돈다."""
    assert isinstance(target_dag.timetable, CronTriggerTimetable)
    assert TIMEZONE == "Asia/Seoul"
    assert target_dag.catchup is False
    assert target_dag.max_active_runs == 1


def _core_task_ids() -> set[str]:
    return (
        {f"collect_{source}" for source in REALTIME_5MIN_SOURCES}
        | {
            "run_normalizer",
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


def test_base_dag_has_no_weather_tasks() -> None:
    """분%10 != 0 tick(`dag`)에는 날씨 collector·gate가 전혀 없다."""
    assert set(dag.task_ids) == _core_task_ids()

    prepare = dag.get_task("prepare_serving_plan")
    assert prepare.upstream_task_ids == {"collect_bike_station_realtime"}
    assert prepare.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


@pytest.mark.parametrize(
    ("target_dag", "expected_weather_sources"),
    [
        (dag_ultra_weather, {"weather_ultra_short_live", "weather_ultra_short_forecast"}),
        (
            dag_ultra_weather_on_hour,
            {"weather_ultra_short_live", "weather_ultra_short_forecast"},
        ),
        (
            dag_full_weather,
            {
                "weather_ultra_short_live",
                "weather_ultra_short_forecast",
                "weather_short_term_forecast",
            },
        ),
    ],
)
def test_weather_variant_folds_collectors_and_gate_before_prepare(
    target_dag, expected_weather_sources
) -> None:
    """날씨가 필요한 tick은 collector들을 ALL_DONE 게이트로 묶어 prepare 앞에 둔다."""
    expected_weather_task_ids = {f"collect_{source}" for source in expected_weather_sources}
    assert expected_weather_task_ids | {"weather_ready_gate"} <= set(target_dag.task_ids)
    assert set(target_dag.task_ids) == _core_task_ids() | expected_weather_task_ids | {
        "weather_ready_gate"
    }

    gate = target_dag.get_task("weather_ready_gate")
    assert isinstance(gate, EmptyOperator)
    assert gate.trigger_rule == TriggerRule.ALL_DONE
    assert gate.upstream_task_ids == expected_weather_task_ids

    prepare = target_dag.get_task("prepare_serving_plan")
    assert prepare.upstream_task_ids == {"collect_bike_station_realtime", "weather_ready_gate"}
    assert prepare.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS

    for source in expected_weather_sources:
        task = target_dag.get_task(f"collect_{source}")
        assert f"--source {source}" in task.bash_command
        timeout = EXECUTION_TIMEOUT_OVERRIDES.get(source)
        if timeout is not None:
            assert task.execution_timeout == timeout


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_inference_waits_for_plan_normalizer_and_rental_input(target_dag) -> None:
    """Plan 준비와 실제 inference 계산 input이 inference 합류점에서 만난다(4개 DAG 공통)."""
    inference = target_dag.get_task("run_inference")
    assert inference.upstream_task_ids == {
        "prepare_serving_plan",
        "collect_bike_rental_history",
        "run_normalizer",
    }
    assert target_dag.get_task("run_normalizer").upstream_task_ids == {
        "collect_population_realtime"
    }
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_derived_publication_is_one_strict_chain(target_dag) -> None:
    """Inference 뒤 4-key final·urgency·route가 정확한 ref 순서로 실행된다(4개 DAG 공통)."""
    assert target_dag.get_task("finalize_serving_release").upstream_task_ids == {
        "run_inference"
    }
    assert target_dag.get_task("publish_station_urgency").upstream_task_ids == {
        "finalize_serving_release"
    }
    assert target_dag.get_task("publish_rebalance_route").upstream_task_ids == {
        "publish_station_urgency"
    }


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_json_tasks_use_bash_env_contract(target_dag) -> None:
    """Production task는 append_env와 JSON output processor를 사용한다(4개 DAG 공통)."""
    for task_id in (
        "prepare_serving_plan",
        "run_inference",
        "finalize_serving_release",
        "publish_station_urgency",
        "publish_rebalance_route",
    ):
        task = target_dag.get_task(task_id)
        assert isinstance(task, BashOperator)
        assert task.append_env is True
        assert callable(task.output_processor)
    assert target_dag.get_task("finalize_serving_release").retries == 0
    assert target_dag.get_task("publish_station_urgency").retries == 0
    assert target_dag.get_task("publish_rebalance_route").retries == 0


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_collector_task_execution_contract(target_dag) -> None:
    """현재 station collector의 retry·timeout·window 계약을 유지한다(4개 DAG 공통)."""
    task = target_dag.get_task("collect_bike_station_realtime")

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


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_living_population_grid_timeout_override_not_used(target_dag) -> None:
    """Daily 전용 grid source와 timeout override가 realtime tick DAG에 섞이지 않는다."""
    assert "collect_living_population_grid" not in target_dag.task_ids
    assert EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"] == timedelta(
        seconds=1200
    )


def _replay_tasks(target_dag) -> list[BashOperator]:
    """설정된 lookback 수만큼 rental replay task를 반환한다."""
    return [
        target_dag.get_task(f"collect_bike_rental_history_replay_{hour}h")
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_lookback_is_one_hour() -> None:
    """현행 API 비용과 late-return 회수율에 맞춘 1시간 lookback을 고정한다."""
    assert RENTAL_HISTORY_LOOKBACK_HOURS == 1


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_replay_tasks_exist_for_each_lookback_hour(target_dag) -> None:
    """Lookback 설정 변경 시 DAG replay task 수가 함께 변한다(4개 DAG 공통)."""
    assert [task.task_id for task in _replay_tasks(target_dag)] == [
        f"collect_bike_rental_history_replay_{hour}h"
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_replay_uses_force_for_late_returns(target_dag) -> None:
    """늦은 반납은 실패 backfill이 아니라 whole-window force replay로 회수한다."""
    for task in _replay_tasks(target_dag):
        assert "--force" in task.bash_command
        assert "--backfill" not in task.bash_command
        assert "--source bike_rental_history" in task.bash_command


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_replay_runs_sequentially_to_cap_api_concurrency(target_dag) -> None:
    """현재 수집과 replay를 직렬화해 API page 동시 요청 수를 제한한다."""
    chain = [target_dag.get_task("collect_bike_rental_history"), *_replay_tasks(target_dag)]

    for upstream, downstream in pairwise(chain):
        assert downstream.task_id in upstream.downstream_task_ids


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_replay_failure_does_not_block_inference(target_dag) -> None:
    """과거 window 보강 실패는 current serving inference를 막지 않는다."""
    for task in _replay_tasks(target_dag):
        assert task.trigger_rule == TriggerRule.ALL_DONE
        assert "run_inference" not in task.downstream_task_ids


@pytest.mark.parametrize("target_dag", _ALL_DAGS)
def test_inference_waits_only_for_current_rental_collection(target_dag) -> None:
    """Inference 입력에는 current rental collector만 직접 연결한다."""
    upstream = target_dag.get_task("run_inference").upstream_task_ids

    assert "collect_bike_rental_history" in upstream
    for task in _replay_tasks(target_dag):
        assert task.task_id not in upstream
