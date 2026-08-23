"""따릉이 대여소 마스터를 하루 한 번 수집하는 DAG."""

import pendulum
from airflow import DAG

from config.schedules import CATCHUP, MAX_ACTIVE_RUNS, STATION_MASTER_CRON, TIMEZONE
from config.sources import STATION_MASTER_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.normalizer_task import build_station_master_enrichment_task

with DAG(
    dag_id="station_master",
    schedule=STATION_MASTER_CRON,
    start_date=pendulum.datetime(2026, 8, 17, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["collector", "daily", "station"],
) as dag:
    collect_station_master = build_collector_task(dag, STATION_MASTER_SOURCE)
    # realtime tick DAG들의 prepare_serving_plan/run_inference가 silver/station_master_enriched를
    # 필수로 읽는다 — 이 태스크가 없으면 그 경로가 매번 "S3에 없음"으로 실패한다
    # (2026-08-21 AWS 최초 배포에서 실제로 발견됨. enrich_station_master_task 빌더는
    # 있었는데 어느 DAG에도 연결돼 있지 않았다).
    enrich_station_master = build_station_master_enrichment_task(dag)
    collect_station_master >> enrich_station_master
