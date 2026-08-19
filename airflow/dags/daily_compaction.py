"""1일 주기: D-6 대여이력을 재수집한 뒤 하루치 Silver를 Archive로 묶는다.

대여이력 API는 반납 완료 건만 보여 최초 수집 때 장기 대여가 빠질 수 있다. D-6의
24개 시간대를 `--force`로 순차 재조회하고, 모두 성공한 뒤 같은 날짜의 대여이력·
대여소 상태·초단기 실황을 병렬 압축한다.

시간별 재조회는 API 동시 요청을 제한하기 위해 사슬로 연결한다.
"""

import pendulum
from airflow.timetables.trigger import CronTriggerTimetable
from config.schedules import CATCHUP, COMPACTION_CRON, MAX_ACTIVE_RUNS, TIMEZONE
from config.sources import COMPACTION_SOURCES, DAILY_ARCHIVE_DELAY_DAYS
from orchestration.collector_task import build_daily_history_replay_task
from orchestration.compaction_task import build_compaction_task
from orchestration.templates import kst_date_days_ago

from airflow import DAG

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

    target_date = kst_date_days_ago(DAILY_ARCHIVE_DELAY_DAYS)
    for source_id in COMPACTION_SOURCES:
        compact = build_compaction_task(dag, source_id, target_date=target_date)
        replay_chain >> compact
