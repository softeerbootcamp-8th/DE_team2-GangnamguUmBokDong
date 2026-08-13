"""Airflow DAG의 시간 스케줄 정책을 정의하는 모듈.

## 이 모듈의 역할

DAG 파일에 cron 문자열과 timezone을 흩어놓지 않고
Airflow 실행 주기를 한 곳에서 관리한다.

## Timezone

모든 운영 기준 시간은 Asia/Seoul을 사용한다.

    TIMEZONE = "Asia/Seoul"

## 실시간 수집

실시간 API 수집은 시간 구간 처리보다 특정 시점의 Polling이 목적이다.

따라서 CronTriggerTimetable을 사용한다.

운영 주기:

    */5 * * * *

의미:

    14:00 -> Collector 실행
    14:05 -> Collector 실행
    14:10 -> Collector 실행

Trigger 시각과 Collector run_id가 동일한 논리 시각을 가져야 한다.

## Backfill

Backfill은 하루 1회 새벽 시간에 실행한다.

실시간 5분 수집과 독립된 DAG로 실행하며,
Backfill 실행 중에도 실시간 Collector는 계속 동작할 수 있어야 한다.

정확한 실행 시각은 구현 시 확정한다.

## Bronze Compaction

Bronze Compaction은 Polling 작업과 성격이 다르다.

특정 하루 동안 생성된 Bronze를 대상으로 하므로
Data Interval 기반 일 단위 DAG로 구현한다.

예:

    data_interval_start = 2026-08-13 00:00
    data_interval_end   = 2026-08-14 00:00

8월 14일에 실행되더라도 처리 대상은 8월 13일 데이터다.

## 금지 사항

schedule을 기준으로 Collector 내부 API 조회 범위를 계산하지 않는다.

수집 API의 실제 기준시간이나 데이터 지연 정책은 Collector가 담당한다.

## 테스트 기준

- timezone이 Asia/Seoul인지 확인
- 실시간 DAG가 CronTriggerTimetable인지 확인
- 실시간 cron이 5분 주기인지 확인
- Backfill이 일 단위인지 확인
- Compaction이 Data Interval 기반인지 확인
"""