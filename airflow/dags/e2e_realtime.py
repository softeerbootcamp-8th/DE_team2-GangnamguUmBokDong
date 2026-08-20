"""실제 컴포넌트 CLI 연결을 수동 검증하는 실시간 E2E DAG.

운영 DAG인 ``realtime_5min``과 동일한 핵심 컴포넌트 계약을 사용하지만
schedule=None으로 두어 개발/통합 검증 시에만 수동 실행한다.

핵심 경로:

    Collector -> population normalizer -----> Inference -> Gold Loader
              -> weather --------------------^
              -> Operational DB Loader ------^

population_realtime Silver는 inference 전에 normalizer를 반드시 통과한다.
weather_ultra_short_live Silver도 같은 window에 수집된 뒤 inference로 진입한다.
normalizer는 한 번 실행으로 현재 시각과 향후 12시간 예측 시각을 모두 보정한다 —
baseline이 항상 nowcaster 추정치라 예전의 strict/fallback 두 갈래는 없앴다.
"""

import pendulum
from airflow import DAG

from config.schedules import TIMEZONE
from config.sources import (
    REALTIME_5MIN_SOURCES,
    STATION_MASTER_SOURCE,
    WEATHER_10MIN_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import (
    build_normalizer_task,
    build_station_master_enrichment_task,
)
from orchestration.routes_task import build_routes_task
from orchestration.urgency_task import build_urgency_task

with DAG(
    dag_id="e2e_realtime",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 17, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["e2e", "realtime", "manual"],
) as dag:
    collector_tasks = {source_id: build_collector_task(dag, source_id) for source_id in REALTIME_5MIN_SOURCES}
    collect_weather = build_collector_task(dag, WEATHER_10MIN_SOURCE)
    collect_station_master = build_collector_task(dag, STATION_MASTER_SOURCE)
    enrich_station_master = build_station_master_enrichment_task(dag)
    [collect_station_master, collector_tasks["bike_station_realtime"]] >> enrich_station_master

    load_stations = build_db_loader_task(dag, "stations")
    load_station_stock = build_db_loader_task(dag, "station_stock")
    collector_tasks["bike_station_realtime"] >> load_stations >> load_station_stock

    run_normalizer = build_normalizer_task(dag)
    collector_tasks["population_realtime"] >> run_normalizer

    run_inference = build_inference_task(dag)
    inference_inputs = [
        task
        for source_id, task in collector_tasks.items()
        if source_id != "population_realtime"
    ]
    [*inference_inputs, collect_weather, run_normalizer, enrich_station_master] >> run_inference

    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    [run_inference, load_station_stock] >> load_forecast_points

    compute_urgency = build_urgency_task(dag)
    load_station_urgency = build_db_loader_task(dag, "station_urgency")
    run_inference >> compute_urgency
    [compute_urgency, load_stations] >> load_station_urgency

    compute_routes = build_routes_task(dag)
    load_rebalance_routes = build_db_loader_task(dag, "rebalance_routes")
    load_rebalance_route_stops = build_db_loader_task(dag, "rebalance_route_stops")
    compute_urgency >> compute_routes >> load_rebalance_routes >> load_rebalance_route_stops
    load_stations >> load_rebalance_route_stops
