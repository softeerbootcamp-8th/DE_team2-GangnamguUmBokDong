"""5분 운영 체인을 immutable plan과 coordinated Gold release로 실행한다."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, REALTIME_5MIN_CRON, TIMEZONE
from config.sources import REALTIME_5MIN_SOURCES, RENTAL_HISTORY_LOOKBACK_HOURS
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
    run_normalizer = build_normalizer_task(dag)
    collector_tasks["population_realtime"] >> run_normalizer

    prepare_plan = build_prepare_serving_task(dag)
    collector_tasks["bike_station_realtime"] >> prepare_plan

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
