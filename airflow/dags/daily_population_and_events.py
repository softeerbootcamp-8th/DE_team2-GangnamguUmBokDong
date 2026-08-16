"""1일 주기: living_population_grid collector -> nowcasting 추정,
cultural_event collector -> cultural_events 적재. 두 브랜치는 서로 독립.
"""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, DAILY_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import DAILY_EVENT_SOURCE, DAILY_POPULATION_SOURCE
from orchestration.collector_task import build_collector_task
from orchestration.db_loader_task import build_db_loader_task
from orchestration.nowcasting_task import build_nowcasting_task

with DAG(
    dag_id="daily_population_and_events",
    schedule=CronTriggerTimetable(DAILY_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily"],
) as dag:
    collect_population = build_collector_task(dag, DAILY_POPULATION_SOURCE)
    run_nowcasting = build_nowcasting_task(dag)
    collect_population >> run_nowcasting

    collect_events = build_collector_task(dag, DAILY_EVENT_SOURCE)
    load_events = build_db_loader_task(dag, "cultural_events")
    collect_events >> load_events
