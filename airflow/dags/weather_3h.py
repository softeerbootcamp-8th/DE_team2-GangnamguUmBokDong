"""3시간 주기: weather_short_term_forecast collector -> weather_forecast 적재."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_3H_CRON
from config.sources import WEATHER_3H_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task

with DAG(
    dag_id="weather_3h",
    schedule=CronTriggerTimetable(WEATHER_3H_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["weather", "3h"],
) as dag:
    collect = build_collector_task(dag, WEATHER_3H_SOURCE)
    load = build_db_loader_task(dag, "weather_forecast")
    collect >> load
