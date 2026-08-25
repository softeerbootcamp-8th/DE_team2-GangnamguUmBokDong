"""1일 주기 생활인구 추정과 source-scoped event publication DAG.

생활인구와 두 행사 source 브랜치는 서로 독립이다.
"""

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import (
    CATCHUP,
    DAILY_CRON,
    DAILY_POPULATION_RETRIES,
    DAILY_POPULATION_RETRY_DELAY,
    MAX_ACTIVE_RUNS,
    TIMEZONE,
)
from config.sources import (
    DAILY_EVENT_SOURCE,
    DAILY_POPULATION_SOURCE,
    PERFORMANCE_EVENT_SOURCE,
)
from orchestration.collector_task import build_collector_task
from orchestration.gold_publisher_task import build_gold_publisher_task
from orchestration.nowcasting_task import build_nowcasting_task

from airflow import DAG

with DAG(
    dag_id="daily_population_and_events",
    schedule=CronTriggerTimetable(DAILY_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily"],
) as dag:
    collect_population = build_collector_task(
        dag,
        DAILY_POPULATION_SOURCE,
        retries=DAILY_POPULATION_RETRIES,
        retry_delay=DAILY_POPULATION_RETRY_DELAY,
    )
    run_nowcasting = build_nowcasting_task(dag)
    collect_population >> run_nowcasting

    collect_events = build_collector_task(dag, DAILY_EVENT_SOURCE)
    publish_events = build_gold_publisher_task(dag, "event:cultural_event")
    collect_events >> publish_events

    collect_performance = build_collector_task(dag, PERFORMANCE_EVENT_SOURCE)
    publish_performance = build_gold_publisher_task(dag, "event:performance_event")
    collect_performance >> publish_performance
