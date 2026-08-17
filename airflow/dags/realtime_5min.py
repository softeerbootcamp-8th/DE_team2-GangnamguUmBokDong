"""5분 주기 핵심 파이프라인: 실시간 수집 -> 운영 DB 적재/보조 처리 -> 추론 -> Gold 적재.

## Task 구조

    collect_bike_rental_history                              (leaf)

    collect_bike_station_realtime -> load_stations -> load_station_stock
        (station_stock.sta_id가 stations.sta_id를 FK 참조하므로 순차 실행)

    collect_population_realtime -> run_normalizer_strict -(all_failed)-> run_normalizer_fallback
        (normalizer 자신의 docstring이 "latest는 Airflow fallback용"이라고 명시)

    [collect_bike_rental_history, collect_bike_station_realtime, collect_population_realtime]
        -> run_inference

    [run_inference, load_station_stock]
        -> load_forecast_points

## 의존성 원칙

Airflow dependency는 실제 데이터 계약을 기준으로 둔다.
현재 ml/inference/predict_single.py는 population_realtime Silver를 직접 읽기 때문에
normalizer 출력에 의존하지 않는다. 따라서 normalizer 브랜치는 inference의 선행 조건으로
강제하지 않는다.

반면 forecast_points는 서비스용 Gold 결과이므로, 같은 run의 예측 결과뿐 아니라
stations/station_stock 적재까지 완료된 뒤 적재하도록 한다. 이 구조는 E2E 경계에서
Gold가 참조하는 운영 데이터와 예측 결과가 함께 준비됐음을 보장한다.

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
    list(collector_tasks.values()) >> run_inference

    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    [run_inference, load_station_stock] >> load_forecast_points
