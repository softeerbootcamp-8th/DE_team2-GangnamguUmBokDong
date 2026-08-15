"""seoul-pop-normalizer CLI를 실행하는 Airflow Task builder.

seoul-pop-normalizer 호출 계약:

    cd seoul-pop-normalizer && uv run python main.py \
        --window-start <window_start> [--baseline-date-mode latest]

exit 0 = 성공, non-zero = 실패(Airflow retry 대상). strict 모드(기본값)는
플래그를 붙이지 않는다.
"""

from datetime import timedelta

from airflow.operators.bash import BashOperator

from callbacks.task_callbacks import log_task_failure, log_task_retry
from orchestration.collector_task import COLLECTOR_WINDOW_START

NORMALIZER_DIR = "/workspace/seoul-pop-normalizer"


def build_normalizer_task(
    task_id: str,
    *,
    baseline_date_mode: str = "strict",
    trigger_rule: str = "all_success",
) -> BashOperator:
    """living_population_normalized 정규화 Task를 생성한다.

    Args:
        task_id: Airflow task id(예: "normalize_pop_grid", "normalize_pop_grid_fallback").
        baseline_date_mode: "strict"(기본) 또는 "latest". "strict"면 CLI에 플래그를
            붙이지 않는다(모듈 기본값과 동일하므로).
        trigger_rule: 이 Task가 실행되는 조건(Airflow TriggerRule 문자열).

    Returns:
        seoul-pop-normalizer CLI를 실행하는 BashOperator.
    """
    mode_flag = "" if baseline_date_mode == "strict" else f" --baseline-date-mode {baseline_date_mode}"
    return BashOperator(
        task_id=task_id,
        bash_command=(
            f"cd {NORMALIZER_DIR} && "
            "env -u VIRTUAL_ENV uv run python main.py "
            f"--window-start {COLLECTOR_WINDOW_START}"
            f"{mode_flag}"
        ),
        retries=2,
        retry_delay=timedelta(seconds=30),
        execution_timeout=timedelta(minutes=4),
        trigger_rule=trigger_rule,
        on_retry_callback=log_task_retry,
        on_failure_callback=log_task_failure,
    )
