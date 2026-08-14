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

from datetime import timedelta

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
def build_backfill_task(
    source_id: str,
    window_start: str,
) -> BashOperator:
    """Collector의 누락 조각 Backfill Task를 생성한다.

    Args:
        source_id: Collector YAML에 정의된 source 식별자.
        window_start: 복구할 원래 수집 window 시작 시각.

    Returns:
        Collector backfill CLI를 실행하는 BashOperator.
    """
    return BashOperator(
        task_id="backfill_collector",
        bash_command=(
            f"cd {COLLECTOR_DIR} && "
            "env -u VIRTUAL_ENV uv run python main.py "
            f"--source {source_id} "
            f"--window-start '{window_start}' "
            "--backfill"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=4),
    )