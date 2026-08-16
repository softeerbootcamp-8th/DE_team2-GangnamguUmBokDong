"""10분 주기: weather_ultra_short_term collector -> weather_current 적재."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_10MIN_CRON
from config.sources import WEATHER_10MIN_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task

with DAG(
    dag_id="weather_10min",
    schedule=CronTriggerTimetable(WEATHER_10MIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["weather", "10min"],
) as dag:
    collect = build_collector_task(dag, WEATHER_10MIN_SOURCE)
    load = build_db_loader_task(dag, "weather_current")
    collect >> load
