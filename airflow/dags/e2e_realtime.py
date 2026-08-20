"""모든 source collector를 포함한 coordinated realtime chain 수동 E2E DAG다."""

import pendulum
from airflow import DAG

from config.schedules import TIMEZONE
from config.sources import (
    REALTIME_5MIN_SOURCES,
    STATION_MASTER_SOURCE,
    WEATHER_3H_SOURCE,
    WEATHER_10MIN_SOURCE,
    WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.inference_task import build_inference_task
from orchestration.normalizer_task import build_normalizer_task
from orchestration.routes_task import build_routes_task
from orchestration.serving_task import (
    build_finalize_serving_task,
    build_prepare_serving_task,
)
from orchestration.urgency_task import build_urgency_task

with DAG(
    dag_id="e2e_realtime",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 17, tz=TIMEZONE),
    catchup=False,
    max_active_runs=1,
    tags=["e2e", "realtime", "manual"],
) as dag:
    collector_tasks = {
        source_id: build_collector_task(dag, source_id)
        for source_id in REALTIME_5MIN_SOURCES
    }
    collect_station_master = build_collector_task(dag, STATION_MASTER_SOURCE)
    collect_weather_live = build_collector_task(dag, WEATHER_10MIN_SOURCE)
    collect_weather_short = build_collector_task(dag, WEATHER_3H_SOURCE)
    collect_weather_ultra = build_collector_task(
        dag,
        WEATHER_ULTRA_SHORT_FORECAST_SOURCE,
    )

    run_normalizer = build_normalizer_task(dag)
    collector_tasks["population_realtime"] >> run_normalizer

    prepare_plan = build_prepare_serving_task(dag)
    [
        collector_tasks["bike_station_realtime"],
        collect_station_master,
        collect_weather_short,
        collect_weather_ultra,
    ] >> prepare_plan

    run_inference = build_inference_task(dag, plan_task_id=prepare_plan.task_id)
    [
        prepare_plan,
        collector_tasks["bike_rental_history"],
        collect_weather_live,
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
