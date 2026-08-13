"""Airflow에서 Collector CLI 실행 Task를 생성하는 공통 모듈.

## 이 모듈의 역할

Airflow DAG와 Collector 실행 프로그램 사이의 경계를 담당한다.

DAG 파일에서 BashOperator, retry, timeout, Collector 실행 경로를
매번 직접 정의하지 않고 이 모듈의 builder를 통해 Collector Task를 만든다.

현재 개발 환경에서는 Airflow와 Collector가 동일 EC2에서 실행되므로
BashOperator를 사용한다.

향후 Collector가 별도 EC2로 분리될 경우 SSH 또는 SSM 기반 실행으로
교체할 수 있도록 DAG와 실행 방식을 분리한다.

## Collector 실행 계약

Airflow는 Collector 내부 구현을 알지 않는다.

Collector는 다음 형태의 CLI를 제공한다고 가정한다.

    uv run python main.py \
        --source bike \
        --run-id 20260813T143000

Airflow가 Collector에 전달하는 값은 최소한 다음 두 개다.

- source
- run_id

run_id는 CronTriggerTimetable의 logical_date를 KST로 변환하여
YYYYMMDDTHHMMSS 형태로 전달한다.

같은 DAG Run의 모든 Collector Task는 동일한 run_id를 사용한다.

예:

    14:30 DAG Run

    collect_bike       -> 20260813T143000
    collect_population -> 20260813T143000
    collect_weather    -> 20260813T143000

## 종료 코드 계약

Collector 내부 상태와 Airflow Task 상태의 계약은 다음과 같다.

    Collector SUCCESS
        -> exit 0
        -> Airflow SUCCESS

    Collector PARTIAL_SUCCESS
        -> exit 0
        -> Airflow SUCCESS

    Collector FAILED
        -> exit != 0
        -> Airflow FAILED
        -> Airflow retry

Airflow는 failed_pages나 failure_ratio를 직접 계산하지 않는다.

## Retry 책임

Collector retry와 Airflow retry를 구분한다.

Collector:
- API 호출/조각 단위 재시도
- 부분 실패 판단

Airflow:
- Collector 프로세스 전체가 실패했을 때 Task 전체 재시도

Airflow retries=2이므로 최초 실행을 포함해 최대 3번 실행된다.

재시도하더라도 logical_date가 바뀌지 않으므로 run_id도 동일하다.

## 병렬 실행

서로 다른 source Task 사이에는 dependency를 설정하지 않는다.

따라서 Executor 자원이 허용하는 범위에서 다음 Task는 병렬 실행된다.

    collect_bike
    collect_population
    collect_weather
    collect_event

## 실행 제한

기본값:

- retries = 2
- retry_delay = 30초
- execution_timeout = 4분

5분 Polling 작업이므로 이전 실행이 다음 스케줄까지 무한정
이어지지 않도록 timeout을 둔다.

## 금지 사항

이 모듈에서는 다음을 구현하지 않는다.

- 외부 API HTTP 호출
- 페이지네이션
- API 응답 검증
- Bronze 저장
- Silver 변환
- 실패 페이지 계산
- Backfill 대상 판정

위 기능은 전부 Collector 책임이다.

## 테스트

- 생성된 Task의 retries 확인
- execution_timeout 확인
- source CLI 인자 확인
- run_id Jinja template 확인
- exit code가 BashOperator 상태로 전달되는지 확인
"""



"""Collector Task 생성 공통 모듈."""

from datetime import timedelta

from airflow.operators.bash import BashOperator

COLLECTOR_DIR = "/workspace/collector"

COLLECTOR_RUN_ID = (
    "{{ logical_date.in_timezone('Asia/Seoul')"
    ".strftime('%Y%m%dT%H%M%S') }}"
)


def build_collector_task(source: str) -> BashOperator:
    """Collector CLI를 실행하는 BashOperator를 생성한다.

    Args:
        source: Collector에서 지원하는 source 식별자.

    Returns:
        BashOperator: source별 Collector 실행 Task.
    """
    return BashOperator(
        task_id=f"collect_{source}",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            "env -u VIRTUAL_ENV uv run python main.py "
            f"--source {source} "
            f"--run-id {COLLECTOR_RUN_ID}"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=4),
    )