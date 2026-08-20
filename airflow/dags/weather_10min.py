"""10분 주기 초단기 실황 수집과 초단기예보 Gold publication DAG."""

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, TIMEZONE, WEATHER_10MIN_CRON
from config.sources import WEATHER_10MIN_SOURCE, WEATHER_ULTRA_SHORT_FORECAST_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.gold_publisher_task import build_gold_publisher_task

from airflow import DAG

with DAG(
    dag_id="weather_10min",
    schedule=CronTriggerTimetable(WEATHER_10MIN_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["weather", "10min"],
) as dag:
    # 초단기 실황은 Silver/ML 전용이며 Gold target을 만들지 않는다.
    collect_current = build_collector_task(dag, WEATHER_10MIN_SOURCE)

    # resolver가 단기·초단기 최신 완전 snapshot을 함께 읽어 한 projection을 게시한다.
    collect_ultra_fcst = build_collector_task(dag, WEATHER_ULTRA_SHORT_FORECAST_SOURCE)
    publish_forecast = build_gold_publisher_task(dag, "weather-forecast")
    collect_ultra_fcst >> publish_forecast
