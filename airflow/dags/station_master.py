"""따릉이 대여소 마스터를 하루 한 번 수집하는 DAG."""

import pendulum
from config.schedules import CATCHUP, DAILY_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import STATION_MASTER_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.normalizer_task import build_station_master_enrichment_task

from airflow import DAG

with DAG(
    dag_id="station_master",
    schedule=DAILY_CRON,
    start_date=pendulum.datetime(2026, 8, 17, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["collector", "daily", "station"],
) as dag:
    collect_station_master = build_collector_task(dag, STATION_MASTER_SOURCE)
    enrich_station_master = build_station_master_enrichment_task(dag)

    collect_station_master >> enrich_station_master
