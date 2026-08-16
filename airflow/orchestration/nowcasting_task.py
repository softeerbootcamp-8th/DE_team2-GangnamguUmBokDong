"""seoul-pop-nowcasting CLI를 실행하는 Airflow Task builder.

seoul-pop-nowcasting 호출 계약:

    cd seoul-pop-nowcasting && uv run python main.py estimate --target-date <date>

exit 0 = 성공, non-zero = 실패(Airflow retry 대상).
"""

from datetime import timedelta

from airflow.operators.bash import BashOperator

from callbacks.task_callbacks import log_task_failure, log_task_retry

NOWCASTING_DIR = "/workspace/seoul-pop-nowcasting"
NOWCASTING_TARGET_DATE = "{{ logical_date.in_timezone('Asia/Seoul').format('YYYY-MM-DD') }}"


def build_nowcasting_task(task_id: str = "estimate_living_population") -> BashOperator:
    """오늘 기준 D-3~D+3 생활인구 추정(nowcasting) Task를 생성한다."""
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {NOWCASTING_DIR} && "
            "env -u VIRTUAL_ENV uv run python main.py estimate "
            f"--target-date {NOWCASTING_TARGET_DATE}"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=10),
        on_retry_callback=log_task_retry,
        on_failure_callback=log_task_failure,
    )
