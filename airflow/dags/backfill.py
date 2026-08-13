"""누락 데이터 복구를 위한 일일 Backfill DAG.

## 목적

실시간 수집 경로에서 완전히 확보되지 못한 데이터를
정기적으로 다시 수집하여 데이터 완전성을 보완한다.

## Backfill의 두 가지 목적

### 1. 기술적 실패 복구

API 호출 실패 또는 재시도 소진으로 확보하지 못한 조각을 다시 수집한다.

실패 조각 정보의 저장 및 조회 방법은 Collector 계약을 따른다.

Airflow는 실패 페이지를 직접 계산하지 않는다.

### 2. 지연 도착 데이터 보완

대여이력 API는 대여 시작 시 즉시 최종 기록이 생성되지 않는다.

반납이 완료된 뒤 기록이 생성되며,
대여 후 약 3시간이 지나야 약 95%가 반납되어 조회 가능한 상태가 된다.

따라서 API 호출이 성공했더라도 최근 대여이력은 완전하지 않을 수 있다.

매일 최근 7일 범위를 다시 조회하여 이후 새롭게 생성된 기록을 보완한다.

## 실행 정책

하루 1회 새벽 시간에 실행한다.

Backfill DAG와 5분 realtime_collection DAG는 독립적이다.

따라서:

    실시간 Collector
    Backfill Collector

가 동시에 실행될 수 있다.

실시간 작업 하나를 놓치더라도 이후 Backfill에서 복구할 수 있는
구조를 목표로 한다.

## Collector 계약

Airflow는 Collector의 --backfill 인터페이스를 호출한다.

구체적인 CLI 계약은 Collector 구현 확정 후 반영한다.

## 금지 사항

Airflow에서 다음을 직접 구현하지 않는다.

- 실패 페이지 조회 알고리즘
- API 페이지 재호출
- 대여이력 중복 제거
- Bronze/Silver 보완
- 데이터 merge

모두 Collector 책임이다.

## 검증

- 하루 한 번 실행되는지
- realtime_collection과 dependency가 없는지
- Collector backfill CLI를 올바르게 호출하는지
- 실패 시 Airflow retry가 동작하는지
"""