"""1일 주기: D-6 대여이력을 재수집한 뒤 하루치 Silver를 Archive로 묶는다.

대여이력 API는 반납 완료 건만 보여 최초 수집 때 장기 대여가 빠질 수 있다. D-6의
24개 시간대를 `--force`로 순차 재조회하고, 모든 시간대의 시도가 끝난 뒤 대여이력
compaction을 실행한다. 한 시간대 실패가 다음 시간대를 막지 않으며, 대여소 상태와
초단기 실황 compaction은 replay와 독립적으로 실행한다.

모든 compaction은 명시적 날짜 대신 Collector의 recovery sweep을 사용해 과거 DAG
중단이나 압축 실패로 누락된 Archive도 다음 실행에서 복구한다.
"""

import pendulum
from airflow.task.trigger_rule import TriggerRule
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, COMPACTION_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import (
    COLD_BRONZE_SOURCES,
    COMPACTION_SOURCES,
    DAILY_ARCHIVE_DELAY_DAYS,
)
from orchestration.collector_task import build_daily_history_replay_task
from orchestration.compaction_task import (
    build_cold_bronze_compaction_task,
    build_compaction_task,
    build_hot_bronze_gc_task,
    build_silver_gc_task,
)
from orchestration.templates import KST_DATE, kst_date_days_ago

from airflow import DAG

SILVER_GC_RETENTION_DAYS = 30
HOT_BRONZE_RETENTION_DAYS = 30

with DAG(
    dag_id="daily_compaction",
    schedule=CronTriggerTimetable(COMPACTION_CRON, timezone=TIMEZONE),
    start_date=pendulum.datetime(2026, 8, 16, tz=TIMEZONE),
    catchup=CATCHUP,
    max_active_runs=MAX_ACTIVE_RUNS,
    tags=["daily", "archive"],
) as dag:
    replay_chain = None
    for hour in range(24):
        replay = build_daily_history_replay_task(
            dag, hour, DAILY_ARCHIVE_DELAY_DAYS
        )
        if replay_chain is not None:
            replay_chain >> replay
        replay_chain = replay

    compaction_tasks = {}
    for source_id in COMPACTION_SOURCES:
        if source_id == "bike_rental_history":
            compact = build_compaction_task(
                dag, source_id, trigger_rule=TriggerRule.ALL_DONE
            )
            replay_chain >> compact
        else:
            compact = build_compaction_task(dag, source_id)
        compaction_tasks[source_id] = compact

    cold_tasks = {}
    for source_id in COLD_BRONZE_SOURCES:
        cold = build_cold_bronze_compaction_task(
            dag,
            source_id,
            today=KST_DATE,
            delay_days=DAILY_ARCHIVE_DELAY_DAYS,
            trigger_rule=(
                TriggerRule.ALL_DONE
                if source_id == "bike_rental_history"
                else TriggerRule.ALL_SUCCESS
            ),
        )
        cold_tasks[source_id] = cold
        if source_id == "bike_rental_history":
            replay_chain >> cold

        hot_gc = build_hot_bronze_gc_task(
            dag,
            source_id,
            today=KST_DATE,
            retention_days=HOT_BRONZE_RETENTION_DAYS,
        )
        cold >> hot_gc

    for source_id in COLD_BRONZE_SOURCES:
        gc_target_date = kst_date_days_ago(
            DAILY_ARCHIVE_DELAY_DAYS + SILVER_GC_RETENTION_DAYS
        )
        gc = build_silver_gc_task(
            dag,
            source_id,
            gc_target_date,
            require_archive=source_id in COMPACTION_SOURCES,
        )
        dag.get_task(f"gc_hot_bronze_{source_id}") >> gc
