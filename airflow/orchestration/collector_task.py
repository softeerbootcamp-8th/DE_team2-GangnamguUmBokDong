"""Airflow와 Collector CLI 사이의 실행 계약을 정의하는 공통 Task 모듈.

Airflow DAG는 Collector 내부 구현을 알지 않고 이 모듈의 builder를 통해
Collector 프로세스만 실행한다.

Collector 호출 계약:

    uv run python main.py \
        --source <source_id> \
        --window-start <window_start>

``source_id``는 Collector YAML에 정의된 실제 ``source_id``를 사용한다.
``window_start``는 Collector의 멱등 키 ``(source_id, window_start)``를
구성하는 값이며 Airflow retry에서도 동일하게 유지한다.

Collector 결과 계약:

- SUCCEEDED / PARTIAL / EMPTY / SKIPPED -> exit 0 -> Airflow SUCCESS
- FAILED -> non-zero exit -> Airflow retry / FAILED

Airflow는 Collector의 API retry, 페이지네이션, 품질 게이트, manifest 판정,
Bronze/Silver 저장, Backfill 대상 계산을 구현하지 않는다.
"""

import shlex
from datetime import timedelta

from airflow.models.mappedoperator import MappedOperator
from airflow.operators.bash import BashOperator

from callbacks.task_callbacks import log_task_failure, log_task_retry

COLLECTOR_DIR = "/workspace/collector"
COLLECTOR_WINDOW_START = (
    "{{ logical_date.in_timezone('Asia/Seoul').isoformat() }}"
)


def build_collector_task(source_id: str) -> BashOperator:
    """Collector 실행 Task를 생성한다.

    Args:
        source_id: Collector 설정에 정의된 source 식별자.

    Returns:
        Collector를 실행하는 BashOperator.
    """
    return BashOperator(
        task_id=f"collect_{source_id}",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            "env -u VIRTUAL_ENV uv run python main.py "
            f"--source {source_id} "
            f"--window-start {COLLECTOR_WINDOW_START}"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=4),
        on_retry_callback=log_task_retry,
        on_failure_callback=log_task_failure,
    )


def build_backfill_command(target: dict[str, str]) -> str:
    """백필 대상 하나에 대한 Collector CLI 명령을 생성한다.

    Args:
        target: source_id와 window_start를 포함한 백필 대상.

    Returns:
        Collector backfill CLI 실행 명령.
    """
    source_id = shlex.quote(target["source_id"])
    window_start = shlex.quote(target["window_start"])

    return (
        f"cd {COLLECTOR_DIR} && "
        "env -u VIRTUAL_ENV uv run python main.py "
        f"--source {source_id} "
        f"--window-start {window_start} "
        "--backfill"
    )


def build_backfill_task(targets) -> MappedOperator:
    """백필 대상 목록을 기반으로 Collector Task를 동적으로 생성한다.

    Args:
        targets: Collector 조회 Task가 반환한 백필 대상 XComArg.

    Returns:
        대상별 Collector backfill CLI를 실행하는 동적 BashOperator.
    """
    commands = targets.map(build_backfill_command)

    return BashOperator.partial(
        task_id="run_backfill",
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=4),
        on_retry_callback=log_task_retry,
        on_failure_callback=log_task_failure,
    ).expand(
        bash_command=commands,
    )