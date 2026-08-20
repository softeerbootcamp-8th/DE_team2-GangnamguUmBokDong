"""5개 모듈(collector/normalizer/nowcasting/ml-inference/loader) 전체가 쓰는
공용 subprocess-CLI BashOperator 팩토리.

Airflow는 각 모듈을 독립된 CLI로만 호출한다 — 모듈 내부 코드를 import하지 않는다.
모든 모듈은 저마다 별도의 uv 프로젝트/venv를 유지하므로, Airflow 자체가
`uv run airflow ...`로 떠 있어 설정된 VIRTUAL_ENV와 UV_PROJECT_ENVIRONMENT를
중첩된 uv run이 잘못 물려받지 않도록 모든 명령 앞에서 두 환경변수를 제거한다.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    DEFAULT_EXECUTION_TIMEOUT,
    DEFAULT_RETRIES,
    DEFAULT_RETRY_DELAY,
)

# 이 파일: airflow/orchestration/task_builder.py -> parents[2] == 저장소 루트.
# docker-compose.yml이 저장소 전체를 /workspace에 마운트하므로 컨테이너 안에서도
# 동일하게 성립한다.
REPO_ROOT = Path(__file__).resolve().parents[2]


def build_module_task(
    dag,
    task_id: str,
    module_dir: str,
    bash_command: str,
    *,
    retries: int = DEFAULT_RETRIES,
    retry_delay=DEFAULT_RETRY_DELAY,
    execution_timeout=DEFAULT_EXECUTION_TIMEOUT,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
    env: dict[str, str] | None = None,
    output_processor: Callable[[str], Any] | None = None,
) -> BashOperator:
    """독립 uv project CLI를 공통 retry/callback 환경의 BashOperator로 만든다."""
    arguments: dict[str, Any] = {
        "task_id": task_id,
        "bash_command": f"env -u VIRTUAL_ENV -u UV_PROJECT_ENVIRONMENT {bash_command}",
        "cwd": module_dir,
        "retries": retries,
        "retry_delay": retry_delay,
        "execution_timeout": execution_timeout,
        "trigger_rule": trigger_rule,
        "env": env,
        "append_env": True,
        "on_success_callback": on_success_callback,
        "on_failure_callback": on_failure_callback,
        "dag": dag,
    }
    if output_processor is not None:
        arguments["output_processor"] = output_processor
    return BashOperator(**arguments)
