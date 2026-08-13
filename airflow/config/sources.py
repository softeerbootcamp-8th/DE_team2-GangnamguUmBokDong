"""Airflow가 실행할 Collector source 목록을 정의하는 모듈.

구현 예정:
    docs/airflow/implementation-issues.md

설계 근거:
    docs/airflow/implementation-plan.md

## 이 모듈의 역할

Airflow가 어떤 Collector source를 어떤 종류의 DAG에서 실행할지 정의한다.

Collector 내부의 API URL, 페이지 크기, validation 정책 등은 관리하지 않는다.
Airflow는 Collector CLI에 전달할 source 식별자만 알아야 한다.

## 실시간 수집 Source

5분 Polling DAG에서 병렬 실행할 source를 정의한다.

예:

    REALTIME_SOURCES = (
        "bike",
        "population",
        "weather",
    )

같은 DAG Run의 source들은 서로 dependency를 가지지 않으며
Executor 자원이 허용하면 병렬 실행한다.

## 주기가 다른 Source

모든 데이터가 5분마다 수집되는 것은 아니다.

일 단위 또는 다른 주기를 가진 source는 REALTIME_SOURCES에 억지로 포함하지 않고
별도의 DAG 또는 schedule 그룹으로 분리한다.

구체적인 최종 source 이름은 Collector CLI 계약과 맞춘다.

## Backfill Source

Backfill은 두 목적을 가진다.

1. 기술적 API 호출 실패 조각 복구
2. 지연 도착 데이터 정기 보완

특히 대여이력은 반납이 완료된 뒤 API 기록이 생성되므로
최근 7일 범위를 매일 다시 조회하여 지연 데이터를 보완한다.

단, 실제 재수집 범위와 API 조회 로직은 Collector가 담당한다.
Airflow는 --backfill 실행만 오케스트레이션한다.

## 금지 사항

이 모듈에는 다음을 넣지 않는다.

- API URL
- API Key
- 페이지 크기
- HTTP retry 설정
- 실패 비율
- Bronze/Silver 경로

위 설정은 Collector 책임이다.

## 테스트 기준

- 등록된 source 이름 중복 여부
- DAG가 해당 source를 모두 Task로 생성하는지
- source 추가 시 DAG 본체 수정이 필요 없는지
"""