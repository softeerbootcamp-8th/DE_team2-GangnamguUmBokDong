"""Airflow DAG import와 coordinated production topology를 검증한다."""

import dags.daily_compaction as daily_compaction_dag
import dags.daily_population_and_events as daily_dag
import dags.monthly_retrain_rental as monthly_rental_dag
import dags.monthly_retrain_return as monthly_return_dag
import dags.realtime_5min as realtime_5min_dag
import dags.station_master as station_master_dag
import dags.weather_3h as weather_3h_dag
import dags.weather_10min as weather_10min_dag
from airflow.task.trigger_rule import TriggerRule


def test_all_dag_ids_import() -> None:
    """모든 production/manual DAG module이 의존성 오류 없이 import된다."""
    assert {
        realtime_5min_dag.dag.dag_id,
        station_master_dag.dag.dag_id,
        weather_10min_dag.dag.dag_id,
        weather_3h_dag.dag.dag_id,
        daily_dag.dag.dag_id,
        daily_compaction_dag.dag.dag_id,
        monthly_rental_dag.dag.dag_id,
        monthly_return_dag.dag.dag_id,
    } == {
        "realtime_5min",
        "station_master",
        "weather_10min",
        "weather_3h",
        "daily_population_and_events",
        "daily_compaction",
        "monthly_retrain_rental",
        "monthly_retrain_return",
    }


def test_realtime_population_is_normalized_before_inference() -> None:
    """Realtime inference가 raw population collector를 우회하지 않는다."""
    normalizer = realtime_5min_dag.dag.get_task("run_normalizer")
    inference = realtime_5min_dag.dag.get_task("run_inference")

    assert normalizer.upstream_task_ids == {"collect_population_realtime"}
    assert "run_normalizer" in inference.upstream_task_ids
    assert "collect_population_realtime" not in inference.upstream_task_ids
    assert inference.trigger_rule == TriggerRule.ALL_SUCCESS
    assert "collect_weather_ultra_short_live" not in realtime_5min_dag.dag.task_ids


def test_realtime_uses_single_coordinated_publication_chain() -> None:
    """단일 serving DAG가 inference 이후 exact-ref chain을 소유한다."""
    assert realtime_5min_dag.dag.get_task(
        "finalize_serving_release"
    ).upstream_task_ids == {"run_inference"}
    assert realtime_5min_dag.dag.get_task(
        "publish_station_urgency"
    ).upstream_task_ids == {"finalize_serving_release"}
    assert realtime_5min_dag.dag.get_task(
        "publish_rebalance_route"
    ).upstream_task_ids == {"publish_station_urgency"}


def test_station_and_weather_schedules_are_collector_only() -> None:
    """Topology/weather standalone Gold authority가 scheduled DAG를 우회하지 못한다."""
    # enrich_station_master는 Silver(station_master_enriched)를 만드는 normalizer라
    # 이 계약(Gold 우회 금지)에 걸리지 않는다. 태스크 수를 세는 대신 아래에서
    # gold_cli 호출이 없음을 직접 확인한다.
    assert set(station_master_dag.dag.task_ids) == {
        "collect_bike_station_master",
        "enrich_station_master",
    }
    for task in station_master_dag.dag.tasks:
        assert "gold_cli.py" not in task.bash_command
    assert set(weather_10min_dag.dag.task_ids) == {
        "collect_weather_ultra_short_live",
        "collect_weather_ultra_short_forecast",
    }
    assert set(weather_3h_dag.dag.task_ids) == {"collect_weather_short_term_forecast"}


def test_station_master_daily_collector_contract() -> None:
    """Station master schedule가 source snapshot만 수집하고 Gold를 게시하지 않는다."""
    assert station_master_dag.dag.dag_id == "station_master"
    task = station_master_dag.dag.get_task("collect_bike_station_master")

    assert "--source bike_station_master" in task.bash_command
    assert task.upstream_task_ids == set()
    # 수집 직후 같은 스냅샷을 Silver로 보강한다. realtime_5min의
    # prepare_serving_plan이 silver/station_master_enriched를 필수로 읽으므로,
    # 이 태스크가 어느 DAG에도 없으면 그 경로가 영구히 비어 파이프라인이 멈춘다.
    assert task.downstream_task_ids == {"enrich_station_master"}
    enrichment = station_master_dag.dag.get_task("enrich_station_master")
    assert "station_master.py" in enrichment.bash_command
    assert enrichment.downstream_task_ids == set()
