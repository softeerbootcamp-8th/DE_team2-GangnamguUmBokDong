"""실제 컴포넌트 CLI 연결을 수동 검증하는 실시간 E2E DAG.

운영 DAG인 ``realtime_5min``과 동일한 핵심 컴포넌트 계약을 사용하지만
schedule=None으로 두어 개발/통합 검증 시에만 수동 실행한다.

핵심 경로:

    Collector -> population normalizer -----> Inference -> Gold Loader
              -> weather --------------------^
              -> Operational DB Loader ------^

population_realtime Silver는 inference 전에 normalizer를 반드시 통과한다.
weather_ultra_short_live Silver도 같은 window에 수집된 뒤 inference로 진입한다.
strict가 성공하면 fallback은 skipped되고, strict가 실패하면 fallback(latest)이 실행된다.
둘 중 하나가 성공한 경우 ``population_normalized`` 합류 task가 성공해 inference로 진행한다.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from config.schedules import TIMEZONE
from config.sources import (
    NORMALIZER_BASELINE_MODE_FALLBACK,
    NORMALIZER_BASELINE_MODE_PRIMARY,
    REALTIME_5MIN_SOURCES,
    WEATHER_10MIN_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task

from airflow import DAG

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

    load_stations = build_db_loader_task(dag, "stations")
    load_station_stock = build_db_loader_task(dag, "station_stock")
    collector_tasks["bike_station_realtime"] >> load_stations >> load_station_stock

    run_normalizer_strict = build_normalizer_task(dag, "run_normalizer_strict", NORMALIZER_BASELINE_MODE_PRIMARY)
    run_normalizer_fallback = build_normalizer_task(
        dag,
        "run_normalizer_fallback",
        NORMALIZER_BASELINE_MODE_FALLBACK,
        trigger_rule="all_failed",
    )
    population_normalized = EmptyOperator(
        task_id="population_normalized",
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    collector_tasks["population_realtime"] >> run_normalizer_strict >> run_normalizer_fallback
    [run_normalizer_strict, run_normalizer_fallback] >> population_normalized

    run_inference = build_inference_task(dag)
    inference_inputs = [
        task
        for source_id, task in collector_tasks.items()
        if source_id != "population_realtime"
    ]
    [*inference_inputs, collect_weather, population_normalized] >> run_inference

    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    [run_inference, load_station_stock] >> load_forecast_points
