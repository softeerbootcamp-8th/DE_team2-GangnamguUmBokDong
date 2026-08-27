"""단일 realtime tick DAG(5분 격자 + 날씨 freshness gate)를 검증한다."""

from datetime import timedelta
from itertools import pairwise

import pytest
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.providers.standard.operators.python import ShortCircuitOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import REALTIME_TICK_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from dags.realtime_tick import dag

_WEATHER_SOURCES = (
    "weather_ultra_short_live",
    "weather_ultra_short_forecast",
    "weather_short_term_forecast",
)


def test_schedule_is_five_minute_grid_without_catchup() -> None:
    """5분 격자로 매번 돌고, single active run에 catchup은 없다."""
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert REALTIME_TICK_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1
    assert dag.dagrun_timeout == timedelta(minutes=5)


def _core_task_ids() -> set[str]:
    return (
        {f"collect_{source}" for source in REALTIME_5MIN_SOURCES}
        | {
            "run_normalizer",
            "resolve_poi_master",
            "prepare_serving_plan",
            "run_inference",
            "finalize_serving_release",
            "publish_station_urgency",
            "publish_rebalance_route",
            "weather_ready_gate",
        }
        | {f"collect_{source}" for source in _WEATHER_SOURCES}
        | {f"freshness_gate_{source}" for source in _WEATHER_SOURCES}
        | {
            f"collect_bike_rental_history_replay_{hour}h"
            for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
        }
    )


def test_dag_has_exactly_the_expected_tasks() -> None:
    assert set(dag.task_ids) == _core_task_ids()


@pytest.mark.parametrize("source_id", _WEATHER_SOURCES)
def test_weather_collector_sits_behind_its_own_freshness_gate(source_id: str) -> None:
    """날씨 collector마다 freshness gate 하나가 직접 상위에 있다."""
    gate = dag.get_task(f"freshness_gate_{source_id}")
    collect = dag.get_task(f"collect_{source_id}")

    assert isinstance(gate, ShortCircuitOperator)
    assert gate.retries == 0
    # 다운스트림 전체를 강제로 스킵시키면(기본값 True) weather_ready_gate 이후
    # 체인까지 통째로 멈춘다 — 직접 하위(collect)만 스킵돼야 한다.
    assert gate.ignore_downstream_trigger_rules is False
    assert collect.upstream_task_ids == {f"freshness_gate_{source_id}"}

    # 재시도 없이 60초 안에 실패시킨다(세 소스 다 동일값, 소스별 dict라 필요하면
    # 하나만 바꿀 수 있다 — realtime_tick.py의 _WEATHER_COLLECTOR_TIMEOUTS 참고).
    assert f"--source {source_id}" in collect.bash_command
    assert collect.retries == 0
    assert collect.execution_timeout == timedelta(seconds=60)


def test_weather_ready_gate_tolerates_skipped_or_failed_collectors() -> None:
    """freshness gate로 건너뛰어졌거나 실패한 날씨 collector가 있어도 게이트는 진행한다."""
    gate = dag.get_task("weather_ready_gate")
    assert isinstance(gate, EmptyOperator)
    assert gate.trigger_rule == TriggerRule.ALL_DONE
    assert gate.upstream_task_ids == {f"collect_{source}" for source in _WEATHER_SOURCES}


def test_prepare_plan_waits_for_station_and_weather_gate() -> None:
    prepare = dag.get_task("prepare_serving_plan")
    assert prepare.upstream_task_ids == {"collect_bike_station_realtime", "weather_ready_gate"}
    assert prepare.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS


def test_inference_waits_for_plan_normalizer_and_rental_input() -> None:
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


def test_population_and_normalizer_share_one_pinned_poi_master() -> None:
    """실시간 인구 수집과 공간 보정이 같은 resolver XCom만 사용한다."""
    resolver = dag.get_task("resolve_poi_master")
    population = dag.get_task("collect_population_realtime")
    normalizer = dag.get_task("run_normalizer")

    assert population.upstream_task_ids == {resolver.task_id}
    assert normalizer.upstream_task_ids == {population.task_id}
    assert population.env == normalizer.env == {
        "POI_MASTER_MODE": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')['mode'] }}"
        ),
        "POI_MASTER_MANIFEST_URI": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')"
            ".get('manifest_uri') or '' }}"
        ),
        "POI_MASTER_MANIFEST_SHA256": (
            "{{ ti.xcom_pull(task_ids='resolve_poi_master')"
            ".get('manifest_sha256') or '' }}"
        ),
    }
    for task in (population, normalizer):
        assert '--poi-master-mode "$POI_MASTER_MODE"' in task.bash_command
        assert (
            '--poi-master-manifest-uri "$POI_MASTER_MANIFEST_URI"'
            in task.bash_command
        )
        assert (
            '--poi-master-manifest-sha256 "$POI_MASTER_MANIFEST_SHA256"'
            in task.bash_command
        )


def test_other_collectors_do_not_receive_poi_master_arguments() -> None:
    """POI Master 계약이 population 외 Collector의 CLI를 바꾸지 않는다."""
    for source_id in REALTIME_5MIN_SOURCES:
        if source_id == "population_realtime":
            continue
        task = dag.get_task(f"collect_{source_id}")
        assert "--poi-master-" not in task.bash_command
        assert task.env is None


def test_derived_publication_is_one_strict_chain() -> None:
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
    assert "collect_living_population_grid" not in dag.task_ids


def _replay_tasks() -> list[BashOperator]:
    return [
        dag.get_task(f"collect_bike_rental_history_replay_{hour}h")
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_lookback_is_one_hour() -> None:
    assert RENTAL_HISTORY_LOOKBACK_HOURS == 1


def test_replay_tasks_exist_for_each_lookback_hour() -> None:
    assert [task.task_id for task in _replay_tasks()] == [
        f"collect_bike_rental_history_replay_{hour}h"
        for hour in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1)
    ]


def test_replay_uses_force_for_late_returns() -> None:
    for task in _replay_tasks():
        assert "--force" in task.bash_command
        assert "--backfill" not in task.bash_command
        assert "--source bike_rental_history" in task.bash_command


def test_replay_runs_sequentially_to_cap_api_concurrency() -> None:
    chain = [dag.get_task("collect_bike_rental_history"), *_replay_tasks()]

    for upstream, downstream in pairwise(chain):
        assert downstream.task_id in upstream.downstream_task_ids


def test_replay_failure_does_not_block_inference() -> None:
    for task in _replay_tasks():
        assert task.trigger_rule == TriggerRule.ALL_DONE
        assert "run_inference" not in task.downstream_task_ids


def test_inference_waits_only_for_current_rental_collection() -> None:
    upstream = dag.get_task("run_inference").upstream_task_ids

    assert "collect_bike_rental_history" in upstream
    for task in _replay_tasks():
        assert task.task_id not in upstream
