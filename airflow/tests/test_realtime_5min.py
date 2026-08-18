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
        "population_normalized",
        "run_inference",
        "load_forecast_points",
        "compute_urgency",
        "load_station_urgency",
        "compute_routes",
        "load_rebalance_routes",
        "load_rebalance_route_stops",
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


def test_inference_waits_for_realtime_bikes_and_normalized_population():
    """날씨는 별도 DAG의 최신 Silver를 읽고, 이 DAG의 실시간 입력은 직접 기다린다."""
    run_inference = dag.get_task("run_inference")
    upstream_ids = {t.task_id for t in run_inference.upstream_list}

    assert upstream_ids == {
        "collect_bike_rental_history",
        "collect_bike_station_realtime",
        "population_normalized",
    }
    assert "collect_population_realtime" not in upstream_ids
    assert "collect_weather_ultra_short_live" not in dag.task_ids


def test_inference_then_load_forecast_points():
    run_inference = dag.get_task("run_inference")
    load_forecast_points = dag.get_task("load_forecast_points")
    assert load_forecast_points.task_id in {t.task_id for t in run_inference.downstream_list}


def test_inference_then_compute_urgency_then_load_station_urgency():
    """urgency_score 계산(rebalance)은 S3(재고 이력·예측 결과)만 읽어서 RDS 적재
    (load_station_stock/load_forecast_points)를 기다릴 필요가 없다 — run_inference에만
    의존한다(이유: #107)."""
    run_inference = dag.get_task("run_inference")
    compute_urgency = dag.get_task("compute_urgency")
    load_station_urgency = dag.get_task("load_station_urgency")

    assert compute_urgency.task_id in {t.task_id for t in run_inference.downstream_list}
    assert {t.task_id for t in compute_urgency.upstream_list} == {"run_inference"}
    assert load_station_urgency.task_id in {t.task_id for t in compute_urgency.downstream_list}
    assert {t.task_id for t in load_station_urgency.upstream_list} == {"compute_urgency"}


def test_compute_urgency_then_compute_routes_then_load_rebalance_tables():
    """라우트 생성(rebalance/routes.py)도 urgency와 마찬가지로 S3만 읽는다(dispatched
    넷팅을 위한 좁은 RDS 조회 하나 제외) — compute_urgency에만 의존하고
    load_station_urgency와는 독립적으로 병렬 실행된다(이유: #109)."""
    compute_urgency = dag.get_task("compute_urgency")
    compute_routes = dag.get_task("compute_routes")
    load_rebalance_routes = dag.get_task("load_rebalance_routes")
    load_rebalance_route_stops = dag.get_task("load_rebalance_route_stops")

    assert compute_routes.task_id in {t.task_id for t in compute_urgency.downstream_list}
    assert {t.task_id for t in compute_routes.upstream_list} == {"compute_urgency"}

    downstream_ids = {t.task_id for t in compute_routes.downstream_list}
    assert downstream_ids == {"load_rebalance_routes", "load_rebalance_route_stops"}
    assert {t.task_id for t in load_rebalance_routes.upstream_list} == {"compute_routes"}
    assert {t.task_id for t in load_rebalance_route_stops.upstream_list} == {"compute_routes"}


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
