"""5분 주기 핵심 파이프라인: bike*3 collector -> stations/station_stock 적재,
normalizer, ml/inference -> forecast_points 적재.

## Task 구조

    collect_bike_rental_history                              (leaf)

    collect_bike_station_realtime -> load_stations -> load_station_stock
        (station_stock.sta_id가 stations.sta_id를 FK 참조하므로 순차 실행)

    collect_population_realtime -> run_normalizer_strict -(all_failed)-> run_normalizer_fallback
        (normalizer 자신의 docstring이 "latest는 Airflow fallback용"이라고 명시)

    [collect_bike_rental_history, collect_bike_station_realtime, collect_population_realtime,
     run_normalizer_fallback] -> run_inference -> load_forecast_points

## run_inference는 사실 normalizer의 출력에 의존한다 (2026-08 정정)

예전 버전의 이 문서는 "인구 피처가 normalizer 출력을 전혀 안 쓴다"고 적어뒀는데
`ml/inference/predict_single.py`를 다시 확인해보니 틀린 내용이었다 —
`_get_recent_population()`은 실제로 `living_population_normalized`
(normalizer가 5분마다 쓰는 정규화된 생활인구)를 읽는다. 즉 이 DAG처럼
`run_normalizer_*`와 `run_inference`가 아무 의존관계 없이 병렬로 뜨면,
normalizer가 그 tick의 파일을 S3에 쓰기 전에 run_inference가 먼저 실행돼서
최신 인구 대신 이전 tick 값이나 프로필 fallback을 읽는 race condition이 생긴다.

그래서 `run_normalizer_fallback`을 `run_inference`의 upstream으로 추가했다.
**주의**: `run_normalizer_fallback`은 strict가 성공하면 트리거 규칙(`all_failed`)
때문에 보통 SKIPPED로 끝난다 — `run_inference`가 기본 트리거 규칙(ALL_SUCCESS)을
그대로 쓰면 upstream이 SKIPPED일 때 이 태스크도 그대로 SKIPPED로 전파되어
정상 경로(strict 성공)에서 추론이 거의 항상 안 도는 사고가 난다. 그래서
`run_inference`는 `NONE_FAILED_MIN_ONE_SUCCESS`(아무도 실패 안 했고 최소 하나는
성공)로 바꿔서, fallback이 SKIPPED든 SUCCESS든 다른 collector들이 성공하면
정상 진행되게 했다 — normalizer 브랜치가 strict/fallback 둘 다 실패했을 때만
추론을 막는다(그 정도면 인구 데이터 자체가 통째로 의심스러운 상황).

## 금지 사항

DAG 안에서 API 호출, 페이지네이션, S3 저장, 데이터 검증, 모델 추론 로직을
직접 구현하지 않는다 — 전부 각 모듈 CLI의 책임이다.
"""

import pendulum
from airflow import DAG
from airflow.task.trigger_rule import TriggerRule
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

    run_inference = build_inference_task(dag, trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    list(collector_tasks.values()) >> run_inference
    run_normalizer_fallback >> run_inference
    run_inference >> load_forecast_points
