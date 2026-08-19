"""1일 주기: 소스별로 하루치 silver를 archive parquet 하나로 묶는다.

소스 간 의존이 없어 태스크를 병렬로 둔다 — 한 소스의 압축 실패가 다른 소스를 막지
않는다. collector 내부에서도 날짜 단위로 격리되어 있어, 한 날짜가 실패해도 나머지
날짜는 압축된다.

일 단위 수집(`daily_population_and_events`, 03:00)이 끝난 뒤에 돌도록 04:30에 둔다.
"""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import CATCHUP, COMPACTION_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import COMPACTION_SOURCES
from orchestration.compaction_task import build_compaction_task

with DAG(
    dag_id="daily_compaction",
    schedule=CronTriggerTimetable(COMPACTION_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily", "archive"],
) as dag:
    for source_id in COMPACTION_SOURCES:
        build_compaction_task(dag, source_id)
