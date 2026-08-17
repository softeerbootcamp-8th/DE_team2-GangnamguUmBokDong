"""Airflow DAG 모듈이 문법/의존성 에러 없이 로드되고 핵심 E2E 의존성을 유지하는지 확인한다."""

import dags.daily_population_and_events as daily_dag
import dags.e2e_realtime as e2e_realtime_dag
import dags.realtime_5min as realtime_5min_dag
import dags.weather_3h as weather_3h_dag
import dags.weather_10min as weather_10min_dag


def test_realtime_5min_dag_id():
    assert realtime_5min_dag.dag.dag_id == "realtime_5min"


def test_e2e_realtime_dag_id():
    assert e2e_realtime_dag.dag.dag_id == "e2e_realtime"


def test_realtime_population_is_normalized_before_inference():
    normalized = realtime_5min_dag.dag.get_task("population_normalized")
    assert normalized.upstream_task_ids == {"run_normalizer_strict", "run_normalizer_fallback"}
    assert "population_normalized" in realtime_5min_dag.dag.get_task("run_inference").upstream_task_ids
    assert "collect_population_realtime" not in realtime_5min_dag.dag.get_task("run_inference").upstream_task_ids


def test_e2e_population_is_normalized_before_inference():
    normalized = e2e_realtime_dag.dag.get_task("population_normalized")
    assert normalized.upstream_task_ids == {"run_normalizer_strict", "run_normalizer_fallback"}
    assert "population_normalized" in e2e_realtime_dag.dag.get_task("run_inference").upstream_task_ids
    assert "collect_population_realtime" not in e2e_realtime_dag.dag.get_task("run_inference").upstream_task_ids


def test_realtime_gold_waits_for_inference_and_station_stock():
    upstream = realtime_5min_dag.dag.get_task("load_forecast_points").upstream_task_ids
    assert upstream == {"run_inference", "load_station_stock"}


def test_e2e_gold_waits_for_inference_and_station_stock():
    upstream = e2e_realtime_dag.dag.get_task("load_forecast_points").upstream_task_ids
    assert upstream == {"run_inference", "load_station_stock"}


def test_weather_10min_dag_id():
    assert weather_10min_dag.dag.dag_id == "weather_10min"


def test_weather_3h_dag_id():
    assert weather_3h_dag.dag.dag_id == "weather_3h"


def test_daily_population_and_events_dag_id():
    assert daily_dag.dag.dag_id == "daily_population_and_events"
