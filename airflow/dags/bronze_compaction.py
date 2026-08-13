"""일 단위 Bronze Compaction 오케스트레이션 DAG.

## 목적

하루 동안 Collector가 생성한 Bronze 조각들을
장기 보관 단위로 Compaction하는 작업을 실행한다.

Airflow가 데이터를 직접 병합하는 것은 아니다.
실제 S3 조회와 파일 병합은 실행 대상 프로그램이 담당한다.

## Data Interval을 사용하는 이유

실시간 Collector는 특정 시각에 현재 API를 호출하므로 Trigger 기반이다.

반면 Compaction은 특정 기간에 이미 저장된 데이터를 처리한다.

예:

    data_interval_start = 2026-08-13 00:00
    data_interval_end   = 2026-08-14 00:00

이 DAG Run의 의미는:

    "8월 13일에 생성된 Bronze를 처리한다."

이다.

실제 실행이 8월 14일에 이루어져도 처리 대상 기간은 변하지 않는다.

## 재처리

8월 13일 Compaction을 8월 20일에 다시 실행하더라도
동일한 Data Interval을 사용해 8월 13일 데이터를 다시 처리해야 한다.

현재 날짜에서 yesterday를 계산하는 방식에 의존하지 않는다.

## 실행 결과

실행 프로그램이 성공하면 exit 0,
실패하면 non-zero exit로 Airflow에 결과를 전달한다.

## 금지 사항

DAG 안에서 다음을 직접 구현하지 않는다.

- S3 LIST
- JSON 읽기
- NDJSON 변환
- 파일 병합
- 압축
- 삭제 정책

위 작업은 실제 Compaction 프로그램 책임이다.

## 검증

- 일 단위 스케줄인지
- Data Interval이 하루 범위인지
- 과거 Run 재실행 시 동일 기간을 처리하는지
"""