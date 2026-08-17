"""5분 주기 핵심 파이프라인: bike*3 collector -> stations/station_stock 적재,
normalizer, ml/inference -> forecast_points 적재.

## Task 구조

    collect_bike_rental_history                              (leaf)

    collect_bike_station_realtime -> load_stations -> load_station_stock
        (station_stock.sta_id가 stations.sta_id를 FK 참조하므로 순차 실행)

    collect_population_realtime -> run_normalizer_strict -(all_failed)-> run_normalizer_fallback
        (normalizer 자신의 docstring이 "latest는 Airflow fallback용"이라고 명시)

    [collect_bike_rental_history, collect_bike_station_realtime, collect_population_realtime]
        -> run_inference -> load_forecast_points

## run_inference는 normalizer에 의존하지 않는다

ml/inference/predict_single.py를 직접 읽어 확인한 결과, 인구 피처는
`_get_recent_population()`이 living_population_grid/population_realtime Silver를
직접 읽어오며 normalizer의 출력(write_normalized_silver)을 전혀 소비하지 않는다.
정규화 결과물은 현재 어떤 다운스트림도 없는 leaf 브랜치다.

## 금지 사항

DAG 안에서 API 호출, 페이지네이션, S3 저장, 데이터 검증, 모델 추론 로직을
직접 구현하지 않는다 — 전부 각 모듈 CLI의 책임이다.
"""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import (
    NORMALIZER_BASELINE_MODE_FALLBACK,
    NORMALIZER_BASELINE_MODE_PRIMARY,
    REALTIME_5MIN_SOURCES,
)
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task

with DAG(
    dag_id="realtime_5min",
    schedule=CronTriggerTimetable(REALTIME_5MIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["realtime", "5min"],
) as dag:
    collector_tasks = {source_id: build_collector_task(dag, source_id) for source_id in REALTIME_5MIN_SOURCES}

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
    collector_tasks["population_realtime"] >> run_normalizer_strict >> run_normalizer_fallback

    run_inference = build_inference_task(dag)
    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    list(collector_tasks.values()) >> run_inference >> load_forecast_points
