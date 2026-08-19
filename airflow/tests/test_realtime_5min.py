"""5분 핵심 파이프라인 DAG의 구성과 의존성을 검증한다."""

from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import EXECUTION_TIMEOUT_OVERRIDES, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES
from dags.realtime_5min import dag


def test_schedule_and_run_policy():
    assert isinstance(dag.timetable, CronTriggerTimetable)
    assert REALTIME_5MIN_CRON == "*/5 * * * *"
    assert TIMEZONE == "Asia/Seoul"
    assert dag.catchup is False
    assert dag.max_active_runs == 1


def test_expected_tasks_exist():
    expected = {f"collect_{s}" for s in REALTIME_5MIN_SOURCES} | {
        "load_stations",
        "load_station_stock",
        "run_normalizer_strict",
        "run_normalizer_fallback",
        "run_inference",
        "load_forecast_points",
    }
    assert set(dag.task_ids) == expected


def test_stations_loads_before_station_stock():
    """station_stock.sta_id가 stations.sta_id를 FK 참조하므로 순차 실행이어야 한다."""
    load_stations = dag.get_task("load_stations")
    load_station_stock = dag.get_task("load_station_stock")
    assert load_station_stock.task_id in {t.task_id for t in load_stations.downstream_list}


def test_load_stations_depends_on_bike_station_realtime():
    collect = dag.get_task("collect_bike_station_realtime")
    load_stations = dag.get_task("load_stations")
    assert load_stations.task_id in {t.task_id for t in collect.downstream_list}


def test_normalizer_strict_then_fallback():
    collect_population = dag.get_task("collect_population_realtime")
    strict = dag.get_task("run_normalizer_strict")
    fallback = dag.get_task("run_normalizer_fallback")

    assert strict.task_id in {t.task_id for t in collect_population.downstream_list}
    assert fallback.task_id in {t.task_id for t in strict.downstream_list}
    assert fallback.trigger_rule == "all_failed"
    assert "--baseline-date-mode latest" in fallback.bash_command
    assert "--baseline-date-mode strict" in strict.bash_command


def test_inference_depends_on_all_three_collectors_and_normalizer_fallback():
    """ml/inference/predict_single.py의 _get_recent_population()은 실제로 normalizer의
    출력(living_population_normalized)을 읽는다(2026-08 정정 — 예전엔 안 읽는다고
    잘못 판단했었다) — normalizer가 그 tick을 다 쓰기 전에 run_inference가 먼저
    실행되는 race condition을 막으려면 normalizer 브랜치 뒤에 붙어야 한다."""
    run_inference = dag.get_task("run_inference")
    upstream_ids = {t.task_id for t in run_inference.upstream_list}

    assert upstream_ids == {
        "collect_bike_rental_history",
        "collect_bike_station_realtime",
        "collect_population_realtime",
        "run_normalizer_fallback",
    }
    # strict는 fallback을 거쳐 간접적으로만 upstream이다(직접 엣지는 없음).
    assert "run_normalizer_strict" not in upstream_ids


def test_inference_trigger_rule_does_not_skip_when_normalizer_fallback_is_skipped():
    """run_normalizer_fallback은 strict가 성공하면(정상 경로) trigger_rule=all_failed 때문에
    SKIPPED로 끝난다 — run_inference가 기본 규칙(ALL_SUCCESS)이면 SKIPPED가 전파돼
    정상 경로에서도 추론이 거의 항상 안 도는 사고가 난다. NONE_FAILED_MIN_ONE_SUCCESS로
    fallback이 SKIPPED든 SUCCESS든 다른 collector가 성공하면 진행되게 했는지 확인한다."""
    run_inference = dag.get_task("run_inference")
    assert run_inference.trigger_rule == "none_failed_min_one_success"


def test_inference_then_load_forecast_points():
    run_inference = dag.get_task("run_inference")
    load_forecast_points = dag.get_task("load_forecast_points")
    assert load_forecast_points.task_id in {t.task_id for t in run_inference.downstream_list}


def test_collector_task_execution_contract():
    task = dag.get_task("collect_bike_station_realtime")
    assert isinstance(task, BashOperator)
    assert task.retries == 2
    assert task.retry_delay == timedelta(seconds=30)
    assert task.execution_timeout == timedelta(seconds=240)
    assert "--source bike_station_realtime" in task.bash_command
    assert "--window-start" in task.bash_command
    assert "astimezone" in task.bash_command
    assert "env -u VIRTUAL_ENV" in task.bash_command


def test_living_population_grid_timeout_override_not_used_in_this_dag():
    """living_population_grid는 daily DAG 소관이라 이 DAG에는 없다."""
    assert "collect_living_population_grid" not in dag.task_ids
    assert EXECUTION_TIMEOUT_OVERRIDES["living_population_grid"] == timedelta(seconds=1200)
