"""5분 realtime 체인을 날씨 필요 여부에 따라 4개 cron으로 나눠 실행한다.

이전에는 DAG 하나(`realtime_5min`)가 매 5분 돌면서, `prepare_serving_plan` 앞에
`wait_for_weather_manifests` 센서를 둬서 날씨 authority가 준비됐는지 폴링했다.
그런데 어떤 시각에 어떤 날씨가 필요한지는 애초에 분·시 나머지 연산으로 고정돼
있어서(`config.schedules`의 `REALTIME_TICK_*_CRON` 주석 참고) 런타임에 물어볼 필요가
없다 — cron 자체를 그 경계로 나누고, 필요한 시각에만 날씨 collector를 같은 DAG 안
직접 의존성으로 묶으면 센서도, 폴링도, 워커 슬롯 점유도 전부 사라진다.

그래서 하나였던 `weather_10min`/`weather_3h` DAG의 날씨 수집도 여기로 흡수했다 —
같은 소스를 두 DAG이 같은 시각에 중복 수집하면 안 되기 때문이다. 4개 DAG 모두
`_build_realtime_tick_dag`라는 같은 빌더를 호출해서 만들어지므로 core 로직(수집·
정규화·serving 체인)은 한 곳에만 있다.
"""

from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    CATCHUP,
    MAX_ACTIVE_RUNS,
    REALTIME_TICK_CRON,
    REALTIME_TICK_FULL_WEATHER_CRON,
    REALTIME_TICK_ULTRA_WEATHER_CRON,
    REALTIME_TICK_ULTRA_WEATHER_ON_HOUR_CRON,
    TIMEZONE,
)
from config.sources import (
    REALTIME_5MIN_SOURCES,
    RENTAL_HISTORY_LOOKBACK_HOURS,
    WEATHER_10MIN_SOURCE,
    WEATHER_3H_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
)
from orchestration.collector_task import (
    build_collector_replay_task,
    build_collector_task,
)
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task
from orchestration.routes_task import build_routes_task
from orchestration.serving_task import (
    build_finalize_serving_task,
    build_prepare_serving_task,
)
from orchestration.urgency_task import build_urgency_task

from airflow import DAG

_ULTRA_WEATHER_SOURCES = (WEATHER_10MIN_SOURCE, WEATHER_ULTRA_SHORT_FORECAST_SOURCE)
_FULL_WEATHER_SOURCES = _ULTRA_WEATHER_SOURCES + (WEATHER_3H_SOURCE,)

# 구 wait_for_weather_manifests 센서가 최대 30초만 기다리고 soft_fail로 넘어가던 것과
# 같은 상한이다. 재시도 없이(retries=0) 30초 안에 못 끝나면 바로 실패시켜서, 날씨
# collector가 자기 재시도 정책(기본 240초 x 3회)대로 붙잡고 있느라 뒤의
# prepare_serving_plan~publish_rebalance_route 체인 전체를 최대 13분까지 묶어두는
# 일을 막는다. 실측 KMA 호출은 12~13초대라 30초면 넉넉하다.
_WEATHER_COLLECTOR_TIMEOUT = timedelta(seconds=30)


def _build_realtime_tick_dag(dag_id: str, cron: str, weather_source_ids: tuple[str, ...]) -> DAG:
    """공통 realtime 체인을 만들고, 주어지면 날씨 collector를 prepare 앞에 직접 묶는다."""
    with DAG(
        dag_id=dag_id,
        schedule=CronTriggerTimetable(cron, timezone=TIMEZONE),
        start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
        catchup=CATCHUP,
        max_active_runs=MAX_ACTIVE_RUNS,
        tags=["realtime"],
    ) as dag:
        collector_tasks = {
            source_id: build_collector_task(dag, source_id)
            for source_id in REALTIME_5MIN_SOURCES
        }
        run_normalizer = build_normalizer_task(dag)
        collector_tasks["population_realtime"] >> run_normalizer

        prepare_upstream = [collector_tasks["bike_station_realtime"]]
        if weather_source_ids:
            weather_tasks = [
                build_collector_task(
                    dag,
                    source_id,
                    retries=0,
                    execution_timeout=_WEATHER_COLLECTOR_TIMEOUT,
                )
                for source_id in weather_source_ids
            ]
            # ALL_DONE 게이트: 날씨 수집이 실패해도(FAILED) 이 게이트는 항상 성공으로
            # 끝나므로, prepare의 NONE_FAILED_MIN_ONE_SUCCESS를 위반하지 않는다 — 이전
            # 센서의 soft_fail이 하던 "날씨가 없으면 이전 스냅샷으로 계속 진행" 역할을
            # 폴링 없이 그대로 재현한다.
            weather_gate = EmptyOperator(
                task_id="weather_ready_gate",
                trigger_rule=TriggerRule.ALL_DONE,
                on_success_callback=on_success_callback,
                on_failure_callback=on_failure_callback,
                dag=dag,
            )
            weather_tasks >> weather_gate
            prepare_upstream.append(weather_gate)

        prepare_plan = build_prepare_serving_task(
            dag,
            trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        )
        prepare_upstream >> prepare_plan

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


dag = _build_realtime_tick_dag("realtime_tick", REALTIME_TICK_CRON, ())
dag_ultra_weather = _build_realtime_tick_dag(
    "realtime_tick_ultra_weather", REALTIME_TICK_ULTRA_WEATHER_CRON, _ULTRA_WEATHER_SOURCES
)
dag_ultra_weather_on_hour = _build_realtime_tick_dag(
    "realtime_tick_ultra_weather_on_hour",
    REALTIME_TICK_ULTRA_WEATHER_ON_HOUR_CRON,
    _ULTRA_WEATHER_SOURCES,
)
dag_full_weather = _build_realtime_tick_dag(
    "realtime_tick_full_weather", REALTIME_TICK_FULL_WEATHER_CRON, _FULL_WEATHER_SOURCES
)
