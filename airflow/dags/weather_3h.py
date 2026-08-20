"""3시간 주기 단기예보 수집과 통합 weather forecast publication DAG."""

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_3H_CRON
from config.sources import WEATHER_3H_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.gold_publisher_task import build_gold_publisher_task

from airflow import DAG

with DAG(
    dag_id="weather_3h",
    schedule=CronTriggerTimetable(WEATHER_3H_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["weather", "3h"],
) as dag:
    collect = build_collector_task(dag, WEATHER_3H_SOURCE)
    publish = build_gold_publisher_task(dag, "weather-forecast")
    collect >> publish
