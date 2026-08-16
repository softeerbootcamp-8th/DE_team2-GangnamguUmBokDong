"""10분 주기: 초단기 실황 → weather_current 적재, 초단기예보 → weather_forecast 적재."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_10MIN_CRON
from config.sources import WEATHER_10MIN_SOURCE, WEATHER_ULTRA_SHORT_FORECAST_SOURCE
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
    # 초단기 실황 (현재 날씨)
    collect_current = build_collector_task(dag, WEATHER_10MIN_SOURCE)
    load_current = build_db_loader_task(dag, "weather_current")
    collect_current >> load_current

    # 초단기예보 (향후 6시간 예보, 매 30분 발표)
    collect_ultra_fcst = build_collector_task(dag, WEATHER_ULTRA_SHORT_FORECAST_SOURCE)
    load_ultra_fcst = build_db_loader_task(dag, "weather_forecast_ultra")
    collect_ultra_fcst >> load_ultra_fcst
