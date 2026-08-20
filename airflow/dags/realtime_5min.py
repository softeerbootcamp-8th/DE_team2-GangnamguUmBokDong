"""5분 주기 핵심 파이프라인: 실시간 수집 -> 정규화/운영 DB 적재 -> 추론 -> Gold 적재.

## Task 구조

    collect_bike_rental_history -----------------------------┐
                                                             |
    collect_bike_station_realtime ---------------------------+-> run_inference -> compute_urgency -> load_station_urgency
        |                                                    |       |
        -> publish_station_release --------------------------|-------+-> load_forecast_points
                                                             |
    collect_population_realtime -> run_normalizer_strict ----|
                                      |                      |
                                      -(실패)-> run_normalizer_fallback
                                                   |         |
                         [strict 또는 fallback 성공]          |
                                                   v         |
                                      population_normalized -┘

    weather_10min / weather_3h가 쓴 최신 Silver ------------┘ (추론기가 시점 기준 조회)

    run_inference -> compute_urgency -+-> load_station_urgency
                                       └-> compute_routes -+-> load_rebalance_routes
                                                            └-> load_rebalance_route_stops

population_realtime Silver는 inference 전에 반드시 normalizer를 거쳐 보정된 상태여야 한다.
strict가 성공하면 fallback은 skipped되고, strict가 실패하면 fallback(latest)이 실행된다.
``population_normalized``는 둘 중 하나가 성공한 경우에만 통과하는 합류 지점이다.

## 의존성 원칙

Airflow dependency는 실제 데이터 계약을 기준으로 둔다.
- population_realtime -> normalizer -> inference
- weather_10min/weather_3h의 최신 Silver -> inference (cross-DAG snapshot 계약)
- bike_station_realtime -> station/stock 원자 publication
- inference + station/stock publication -> forecast_points
- inference -> compute_urgency(rebalance, S3만 읽음) -> load_station_urgency
  (compute는 RDS load와 병렬이지만, load_station_urgency는 stations FK를 위해
  station publication도 기다린다. forecast_points와는 독립적이다.
  이유: #107)
- compute_urgency -> compute_routes(rebalance, compute_urgency가 S3에 써둔
  결과를 그대로 읽음 + dispatched 넷팅을 위한 좁은 RDS 조회 하나) ->
  load_rebalance_routes -> load_rebalance_route_stops(순차 — route_id FK).
  load_rebalance_route_stops는 sta_id FK를 위해 station publication도 기다린다.
  이유: #109

## 금지 사항

DAG 안에서 API 호출, 페이지네이션, S3 저장, 데이터 검증, 모델 추론 로직을
직접 구현하지 않는다 — 전부 각 모듈 CLI의 책임이다.
"""

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import (
    NORMALIZER_BASELINE_MODE_FALLBACK,
    NORMALIZER_BASELINE_MODE_PRIMARY,
    REALTIME_5MIN_SOURCES,
    RENTAL_HISTORY_LOOKBACK_HOURS,
)
from orchestration.collector_task import (
    build_collector_replay_task,
    build_collector_task,
)
from orchestration.db_loader_task import build_db_loader_task
from orchestration.gold_publisher_task import build_gold_publisher_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task
from orchestration.routes_task import build_routes_task
from orchestration.urgency_task import build_urgency_task

from airflow import DAG

with DAG(
    dag_id="realtime_5min",
    schedule=CronTriggerTimetable(REALTIME_5MIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["realtime", "5min"],
) as dag:
    collector_tasks = {
        source_id: build_collector_task(dag, source_id)
        for source_id in REALTIME_5MIN_SOURCES
    }
    publish_station_release = build_gold_publisher_task(dag, "station-release")
    collector_tasks["bike_station_realtime"] >> publish_station_release

    run_normalizer_strict = build_normalizer_task(
        dag, "run_normalizer_strict", NORMALIZER_BASELINE_MODE_PRIMARY
    )
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

    (
        collector_tasks["population_realtime"]
        >> run_normalizer_strict
        >> run_normalizer_fallback
    )
    [run_normalizer_strict, run_normalizer_fallback] >> population_normalized

    run_inference = build_inference_task(dag)
    inference_inputs = [
        task
        for source_id, task in collector_tasks.items()
        if source_id != "population_realtime"
    ]
    [*inference_inputs, population_normalized] >> run_inference

    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    [run_inference, publish_station_release] >> load_forecast_points

    # 대여이력 과거 시간대 재조회. 현재 tick 수집 뒤에 사슬로 이어 붙인다 — 동시에
    # 띄우면 각 호출이 페이지를 4개씩 병렬로 받으므로 같은 API에 대한 동시 요청이
    # 배수로 늘어난다. 사슬로 묶으면 4로 고정되고, 실측 14.4초/호출이라 5분 tick에
    # 충분히 들어간다. run_inference의 상위에는 두지 않는다(과거 보강이므로).
    replay_chain = collector_tasks["bike_rental_history"]
    for hours_back in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1):
        replay = build_collector_replay_task(dag, "bike_rental_history", hours_back)
        replay_chain >> replay
        replay_chain = replay

    compute_urgency = build_urgency_task(dag)
    load_station_urgency = build_db_loader_task(dag, "station_urgency")
    run_inference >> compute_urgency
    [compute_urgency, publish_station_release] >> load_station_urgency

    compute_routes = build_routes_task(dag)
    load_rebalance_routes = build_db_loader_task(dag, "rebalance_routes")
    load_rebalance_route_stops = build_db_loader_task(dag, "rebalance_route_stops")
    (
        compute_urgency
        >> compute_routes
        >> load_rebalance_routes
        >> load_rebalance_route_stops
    )
    publish_station_release >> load_rebalance_route_stops
