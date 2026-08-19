"""5분 주기 핵심 파이프라인: 실시간 수집 -> 정규화/운영 DB 적재 -> 추론 -> Gold 적재.

## Task 구조

    collect_bike_rental_history -----------------------------┐
                                                             |
    collect_bike_station_realtime ---------------------------+-> run_inference -> compute_urgency -> load_station_urgency
        |                                                    |       |
        -> load_stations -> load_station_stock --------------|-------+-> load_forecast_points
                                                             |
    collect_population_realtime -> run_normalizer -----------┘

    weather_10min / weather_3h가 쓴 최신 Silver ------------┘ (추론기가 시점 기준 조회)

population_realtime Silver는 inference 전에 반드시 normalizer를 거쳐 보정된 상태여야 한다.
normalizer는 한 번 실행으로 현재 시각과 향후 12시간(실시간 도시데이터의 `FCST_PPLTN`)
예측 시각을 각각 보정해 그 시각의 tick 키에 쓴다. baseline이 항상 nowcaster 추정치라
예전의 strict/fallback 두 갈래는 없앴다(`orchestration/normalizer_task.py` 참고).

## 의존성 원칙

Airflow dependency는 실제 데이터 계약을 기준으로 둔다.
- population_realtime -> normalizer -> inference
- weather_10min/weather_3h의 최신 Silver -> inference (cross-DAG snapshot 계약)
- bike_station_realtime -> stations -> station_stock
- inference + station_stock -> forecast_points
- inference -> compute_urgency(rebalance, S3만 읽음) -> load_station_urgency
  (compute는 RDS load와 병렬이지만, load_station_urgency는 stations FK를 위해
  load_stations도 기다린다. load_station_stock/load_forecast_points와는 독립적이다.)

## 금지 사항

DAG 안에서 API 호출, 페이지네이션, S3 저장, 데이터 검증, 모델 추론 로직을
직접 구현하지 않는다 — 전부 각 모듈 CLI의 책임이다.
"""

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
from orchestration.collector_task import build_collector_replay_task, build_collector_task
from orchestration.db_loader_task import build_db_loader_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task
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
    collector_tasks = {source_id: build_collector_task(dag, source_id) for source_id in REALTIME_5MIN_SOURCES}
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
    [*inference_inputs, run_normalizer] >> run_inference

    load_forecast_points = build_db_loader_task(dag, "forecast_points")
    [run_inference, load_station_stock] >> load_forecast_points

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
    [compute_urgency, load_stations] >> load_station_urgency
