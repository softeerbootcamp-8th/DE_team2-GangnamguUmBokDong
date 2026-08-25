"""5분마다 도는 단일 realtime 체인. 날씨는 freshness gate로 필요 시에만 수집한다.

한때는 이 체인을 날씨 필요 여부에 따라 4개 cron으로 나눠 실행했다(분·시 나머지
연산으로 "언제 초단기/단기 날씨가 필요한지"가 고정돼 있다는 점을 이용해, 필요한
시각에만 날씨 collector를 그 cron의 DAG에 직접 의존성으로 묶는 방식). 그런데
서로 다른 DAG는 `max_active_runs`를 각자 따로 관리해서, 한 DAG의 tick이 늦어지면
다른 DAG의 tick과 실행 시각이 겹칠 수 있었다 — 약한 인스턴스에서 CPU 경합이 나면
60초 타임아웃(retries=0)인 날씨 collector가 죽을 위험이 있다(`config.schedules.
REALTIME_TICK_CRON` 주석의 2026-08-22 실측 참고). 게다가 3시간짜리(단기예보)
수집이 한 번 실패하면 다음 cron 경계까지 최대 3시간을 기다려야 했다.

지금은 DAG 하나로 합치고, 대신 날씨 collector마다
`orchestration.collector_task.build_weather_freshness_gate_task()`로 "마지막
성공 수집 이후 충분히 지났는지"를 실제 시각(`datetime.now()`, DAG의 논리 시각이
아니라) 기준으로 매 tick마다 직접 물어서 스킵 여부를 정한다. 이러면 동시 실행
경합이 구조적으로 사라지고(같은 DAG 안에서는 태스크 의존성이 순서를 강제한다),
실패한 수집은 다음 5분 tick에서 곧바로 재시도된다.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    REALTIME_TICK_CRON,
    TIMEZONE,
)
from config.sources import (
    REALTIME_5MIN_SOURCES,
    RENTAL_HISTORY_LOOKBACK_HOURS,
    WEATHER_3H_SOURCE,
    WEATHER_10MIN_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
)
from orchestration.collector_task import (
    build_collector_replay_task,
    build_collector_task,
    build_population_collector_task,
    build_weather_freshness_gate_task,
)
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task
from orchestration.poi_master_task import build_poi_master_resolve_task
from orchestration.routes_task import build_routes_task
from orchestration.serving_task import (
    build_finalize_serving_task,
    build_prepare_serving_task,
)
from orchestration.urgency_task import build_urgency_task

# 초단기(실황+예보)는 10분, 단기예보는 3시간 — 기상청이 그보다 자주 새 값을 내지
# 않으므로, 마지막 성공 수집이 이보다 최근이면 이번 tick은 건너뛴다.
_ULTRA_WEATHER_MIN_INTERVAL = timedelta(minutes=10)
_SHORT_TERM_WEATHER_MIN_INTERVAL = timedelta(hours=3)
_WEATHER_MIN_INTERVAL_BY_SOURCE = {
    WEATHER_10MIN_SOURCE: _ULTRA_WEATHER_MIN_INTERVAL,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE: _ULTRA_WEATHER_MIN_INTERVAL,
    WEATHER_3H_SOURCE: _SHORT_TERM_WEATHER_MIN_INTERVAL,
}
_WEATHER_SOURCES = tuple(_WEATHER_MIN_INTERVAL_BY_SOURCE)

# 재시도 없이(retries=0) 이 시간 안에 못 끝나면 바로 실패시켜서, 날씨 collector가
# 자기 재시도 정책(기본 240초 x 3회)대로 붙잡고 있느라 뒤의
# prepare_serving_plan~publish_rebalance_route 체인 전체를 최대 13분까지 묶어두는
# 일을 막는다.
#
# 소스별 dict로 둔다 — 지금은 셋 다 60초로 같지만, 세 API의 실측 소요가 원래
# 꽤 달라서(단기예보만 격자당 페이지 2장 필요, concurrency:8로 줄인 뒤에도
# 14~16초대, 나머지 둘은 7~11초대 — collector/sources/weather_*.yaml 참고) 나중에
# 하나만 튜닝해야 할 상황이 다시 생길 수 있다. 그때 이 dict만 고치면 된다
# (2026-08-23 실측 근거로 30초에서 60초로 올림 — 3개 동시 실행 시 단기예보가
# 16.36초까지 늘어나는 걸 봐서 여유를 더 뒀다).
_WEATHER_COLLECTOR_TIMEOUTS = {
    WEATHER_10MIN_SOURCE: timedelta(seconds=60),
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE: timedelta(seconds=60),
    WEATHER_3H_SOURCE: timedelta(seconds=60),
}


def _build_realtime_tick_dag() -> DAG:
    """realtime 체인 하나를 만든다. 날씨는 freshness gate로 매 tick 필요 여부를 정한다."""
    with DAG(
        dag_id="realtime_tick",
        schedule=CronTriggerTimetable(REALTIME_TICK_CRON, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        tags=["realtime"],
    ) as dag:
        resolve_poi_master = build_poi_master_resolve_task(dag)
        collector_tasks = {
            source_id: build_collector_task(dag, source_id)
            for source_id in REALTIME_5MIN_SOURCES
            if source_id != "population_realtime"
        }
        collector_tasks["population_realtime"] = build_population_collector_task(
            dag,
            poi_master_task_id=resolve_poi_master.task_id,
        )
        run_normalizer = build_normalizer_task(
            dag,
            poi_master_task_id=resolve_poi_master.task_id,
        )
        resolve_poi_master >> collector_tasks["population_realtime"]
        collector_tasks["population_realtime"] >> run_normalizer

        weather_collect_tasks = []
        for source_id in _WEATHER_SOURCES:
            freshness_gate = build_weather_freshness_gate_task(
                dag, source_id, min_interval=_WEATHER_MIN_INTERVAL_BY_SOURCE[source_id]
            )
            collect = build_collector_task(
                dag,
                source_id,
                retries=0,
                execution_timeout=_WEATHER_COLLECTOR_TIMEOUTS[source_id],
            )
            freshness_gate >> collect
            weather_collect_tasks.append(collect)

        # ALL_DONE 게이트: 날씨 수집이 실패하거나(FAILED) freshness gate로
        # 건너뛰어졌어도(SKIPPED) 이 게이트는 항상 성공으로 끝나므로, prepare의
        # NONE_FAILED_MIN_ONE_SUCCESS를 위반하지 않는다 — "날씨가 없으면 이전
        # 스냅샷으로 계속 진행"을 폴링 없이 재현한다.
        weather_gate = EmptyOperator(
            task_id="weather_ready_gate",
            trigger_rule=TriggerRule.ALL_DONE,
            on_success_callback=on_success_callback,
            on_failure_callback=on_failure_callback,
            dag=dag,
        )
        weather_collect_tasks >> weather_gate

        prepare_plan = build_prepare_serving_task(
            dag,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        [collector_tasks["bike_station_realtime"], weather_gate] >> prepare_plan

        run_inference = build_inference_task(dag, plan_task_id=prepare_plan.task_id)
        [
            prepare_plan,
            collector_tasks["bike_rental_history"],
            run_normalizer,
        ] >> run_inference
        finalize_release = build_finalize_serving_task(
            dag,
            plan_task_id=prepare_plan.task_id,
            inference_task_id=run_inference.task_id,
        )
        publish_urgency = build_urgency_task(
            dag,
            final_task_id=finalize_release.task_id,
        )
        publish_routes = build_routes_task(
            dag,
            urgency_task_id=publish_urgency.task_id,
        )
        run_inference >> finalize_release >> publish_urgency >> publish_routes

        # 과거 반납 완료 기록 보강은 serving current tick과 독립인 비차단 side chain이다.
        replay_chain = collector_tasks["bike_rental_history"]
        for hours_back in range(1, RENTAL_HISTORY_LOOKBACK_HOURS + 1):
            replay = build_collector_replay_task(dag, "bike_rental_history", hours_back)
            replay_chain >> replay
            replay_chain = replay
    return dag


dag = _build_realtime_tick_dag()
