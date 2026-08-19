"""Airflow DAG 모듈이 문법/의존성 에러 없이 로드되고 핵심 E2E 의존성을 유지하는지 확인한다."""

import dags.daily_compaction as daily_compaction_dag
import dags.daily_population_and_events as daily_dag
import dags.e2e_realtime as e2e_realtime_dag
import dags.realtime_5min as realtime_5min_dag
import dags.station_master as station_master_dag
import dags.weather_3h as weather_3h_dag
import dags.weather_10min as weather_10min_dag
from airflow.task.trigger_rule import TriggerRule


def test_realtime_5min_dag_id():
    assert realtime_5min_dag.dag.dag_id == "realtime_5min"


def test_e2e_realtime_dag_id():
    assert e2e_realtime_dag.dag.dag_id == "e2e_realtime"


def test_realtime_population_is_normalized_before_inference():
    normalizer = realtime_5min_dag.dag.get_task("run_normalizer")
    normalized = realtime_5min_dag.dag.get_task("population_normalized")
    inference = realtime_5min_dag.dag.get_task("run_inference")
    assert normalizer.upstream_task_ids == {"collect_population_realtime"}
    assert normalized.upstream_task_ids == {"run_normalizer"}
    assert normalized.trigger_rule == TriggerRule.ONE_SUCCESS
    assert "population_normalized" in inference.upstream_task_ids
    assert "collect_population_realtime" not in inference.upstream_task_ids
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS
    # 날씨는 weather_10min/weather_3h DAG가 쓴 최신 Silver를 inference가 직접 읽는다.
    assert "collect_weather_ultra_short_live" not in realtime_5min_dag.dag.task_ids


def test_e2e_population_is_normalized_before_inference():
    normalizer = e2e_realtime_dag.dag.get_task("run_normalizer")
    normalized = e2e_realtime_dag.dag.get_task("population_normalized")
    inference = e2e_realtime_dag.dag.get_task("run_inference")
    assert normalizer.upstream_task_ids == {"collect_population_realtime"}
    assert normalized.upstream_task_ids == {"run_normalizer"}
    assert normalized.trigger_rule == TriggerRule.ONE_SUCCESS
    assert "population_normalized" in inference.upstream_task_ids
    assert "collect_population_realtime" not in inference.upstream_task_ids
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS
    assert "collect_weather_ultra_short_live" in inference.upstream_task_ids
    assert "enrich_station_master" in inference.upstream_task_ids


def test_realtime_gold_waits_for_inference_and_station_stock():
    upstream = realtime_5min_dag.dag.get_task("load_forecast_points").upstream_task_ids
    assert upstream == {"run_inference", "load_station_stock"}


def test_e2e_gold_waits_for_inference_and_station_stock():
    upstream = e2e_realtime_dag.dag.get_task("load_forecast_points").upstream_task_ids
    assert upstream == {"run_inference", "load_station_stock"}


def test_urgency_loaders_wait_for_stations_fk():
    assert realtime_5min_dag.dag.get_task("load_station_urgency").upstream_task_ids == {
        "compute_urgency",
        "load_stations",
    }
    assert e2e_realtime_dag.dag.get_task("load_station_urgency").upstream_task_ids == {
        "compute_urgency",
        "load_stations",
    }


def test_weather_10min_dag_id():
    assert weather_10min_dag.dag.dag_id == "weather_10min"


def test_weather_3h_dag_id():
    assert weather_3h_dag.dag.dag_id == "weather_3h"


def test_daily_population_and_events_dag_id():
    assert daily_dag.dag.dag_id == "daily_population_and_events"


def test_daily_compaction_dag_id():
    assert daily_compaction_dag.dag.dag_id == "daily_compaction"


def test_station_master_daily_collector_contract():
    assert station_master_dag.dag.dag_id == "station_master"
    task = station_master_dag.dag.get_task("collect_bike_station_master")
    assert "--source bike_station_master" in task.bash_command
    enrich = station_master_dag.dag.get_task("enrich_station_master")
    assert enrich.upstream_task_ids == {"collect_bike_station_master"}
    assert "station_master.py" in enrich.bash_command
