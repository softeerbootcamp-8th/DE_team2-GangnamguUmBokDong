"""5분 주기 실시간 Collector 오케스트레이션 DAG.

## 목적

정해진 5분 시각마다 실시간 Collector들을 병렬 실행한다.

Airflow는 실제 API 수집이나 저장을 수행하지 않는다.
각 source의 Collector CLI를 실행하고 성공/실패 상태를 관리한다.

## 스케줄

CronTriggerTimetable을 사용한다.

    */5 * * * *
    timezone = Asia/Seoul

예:

    14:00 -> run_id=20260813T140000
    14:05 -> run_id=20260813T140500

## Task 구조

각 source는 독립적인 Task다.

                 realtime_collection
                         |
           +-------------+-------------+
           |             |             |
          bike       population      weather
           |             |             |
           +------ dependency 없음 ----+

source 사이에 선행 관계를 만들지 않는다.

## run_id

같은 DAG Run의 모든 Collector는 동일한 logical_date를 run_id로 사용한다.

Airflow Task retry가 발생해도 run_id는 변경하지 않는다.

## 실패 처리

Collector:

    SUCCESS         -> exit 0
    PARTIAL_SUCCESS -> exit 0
    FAILED          -> exit != 0

Airflow는 non-zero exit에 대해서만 Task retry를 수행한다.

Collector 내부 API 조각 retry는 Airflow가 관여하지 않는다.

## DAG Run 중첩

초기 정책은 max_active_runs=1이다.

한 5분 Run이 끝나지 않은 상태에서 다음 Run을 동시에 실행시켜
동일 Collector의 자원 사용이 무제한 증가하는 것을 방지한다.

향후 처리시간과 EC2 자원을 측정한 뒤 변경할 수 있다.

## 구현 방법

- config.sources에서 실시간 source 목록을 읽는다.
- config.schedules에서 스케줄을 읽는다.
- orchestration.collector_task의 공통 builder로 Task를 생성한다.
- source별 Task 사이 dependency는 만들지 않는다.

## 금지 사항

DAG 내부에서 다음을 하지 않는다.

- requests/httpx 호출
- 페이지네이션
- API 응답 파싱
- S3 저장
- 데이터 validation
- failed_pages 계산

## 검증

Airflow Graph에서 source Task가 같은 레벨에 존재해야 한다.
동시에 실행 가능한 상태인지 확인한다.
"""



"""5분 주기 실시간 Collector 실행 DAG."""

import pendulum
from airflow import DAG
from airflow.timetables.trigger import CronTriggerTimetable

from config.schedules import REALTIME_CRON, TIMEZONE
from config.sources import REALTIME_SOURCES
from orchestration.collector_task import build_collector_task
from orchestration.normalizer_task import build_normalizer_task

with DAG(
    dag_id="realtime_collection",
    schedule=CronTriggerTimetable(
        REALTIME_CRON,
        timezone=TIMEZONE,
    ),
    start_date=pendulum.datetime(
        2026,
        8,
        13,
        tz=TIMEZONE,
    ),
    catchup=False,
    max_active_runs=1,
    tags=["collection", "realtime"],
) as dag:
    collector_tasks = {
        source_id: build_collector_task(source_id) for source_id in REALTIME_SOURCES
    }

    normalize_task = build_normalizer_task("normalize_pop_grid")
    normalize_fallback_task = build_normalizer_task(
        "normalize_pop_grid_fallback",
        baseline_date_mode="latest",
        trigger_rule="all_failed",
    )

    collector_tasks["population_realtime"] >> normalize_task >> normalize_fallback_task