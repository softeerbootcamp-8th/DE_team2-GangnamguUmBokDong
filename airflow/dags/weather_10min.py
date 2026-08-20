"""10분 주기 초단기 실황과 초단기예보 collector-only DAG."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_10MIN_CRON
from config.sources import WEATHER_10MIN_SOURCE, WEATHER_ULTRA_SHORT_FORECAST_SOURCE
from orchestration.collector_task import build_collector_task

with DAG(
    dag_id="weather_10min",
    schedule=CronTriggerTimetable(WEATHER_10MIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["weather", "10min"],
) as dag:
    collect_current = build_collector_task(dag, WEATHER_10MIN_SOURCE)
    collect_ultra_fcst = build_collector_task(dag, WEATHER_ULTRA_SHORT_FORECAST_SOURCE)
