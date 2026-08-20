"""6개 모듈(collector/normalizer/nowcasting/ml-inference/loader/rebalance)이 쓰는
공용 subprocess-CLI BashOperator 팩토리.

Airflow는 각 모듈을 독립된 CLI로만 호출한다 — 모듈 내부 코드를 import하지 않는다.
모든 모듈은 저마다 별도의 uv 프로젝트/venv를 유지한다. Airflow 자체의
``VIRTUAL_ENV``는 제거하되 모듈 환경은 Docker 전용 named volume 아래에 명시해,
bind mount된 호스트 ``.venv``를 macOS와 Linux 컨테이너가 서로 덮어쓰지 않게 한다.
"""

from __future__ import annotations

import re
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
_MODULE_UV_ENV_ROOT = "/opt/venvs/modules"


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
    uv_environment_name: str | None = None,
) -> BashOperator:
    """독립 uv 환경을 사용하는 모듈 CLI 태스크를 만든다.

    args:
        uv_environment_name: 기본값은 ``module_dir``의 마지막 경로명이다. uv의
            ``--project``가 작업 디렉터리와 다른 프로젝트를 가리키는 경우에만
            명시한다.
    """
    environment_name = uv_environment_name or Path(module_dir).name
    if (
        type(environment_name) is not str
        or re.fullmatch(r"[a-z][a-z0-9-]*", environment_name) is None
    ):
        raise ValueError(
            "uv_environment_name은 lowercase 영숫자와 하이픈만 허용합니다."
        )
    project_environment = f"{_MODULE_UV_ENV_ROOT}/{environment_name}"
    arguments: dict[str, Any] = {
        "task_id": task_id,
        "bash_command": (
            "env -u VIRTUAL_ENV "
            f"UV_PROJECT_ENVIRONMENT={project_environment} {bash_command}"
        ),
        "cwd": module_dir,
        "retries": retries,
        "retry_delay": retry_delay,
        "execution_timeout": execution_timeout,
        "trigger_rule": trigger_rule,
        "append_env": True,
        "on_success_callback": on_success_callback,
        "on_failure_callback": on_failure_callback,
        "dag": dag,
    }
    if env is not None:
        arguments["env"] = env
    if output_processor is not None:
        arguments["output_processor"] = output_processor
    return BashOperator(**arguments)
