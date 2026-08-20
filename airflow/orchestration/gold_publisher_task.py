"""Gold publication CLI를 실행하는 태스크 빌더."""

from __future__ import annotations

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from config.schedules import DB_LOADER_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

GOLD_PUBLISHER_DIR = str(REPO_ROOT / "loader")

_SOURCE_PUBLICATIONS = frozenset(
    {
        "event:cultural_event",
        "event:performance_event",
    }
)


def build_gold_publisher_task(dag: DAG, publication: str) -> BashOperator:
    """허용된 원천 Gold publication을 fail-closed 태스크로 만든다."""
    if publication not in _SOURCE_PUBLICATIONS:
        raise ValueError(f"지원하지 않는 원천 Gold publication입니다: {publication}")

    task_suffix = publication.replace(":", "_").replace("-", "_")
    command = (
        "uv run --frozen python gold_cli.py "
        f"--publication {publication} --window-start {KST_WINDOW_START}"
    )
    return build_module_task(
        dag,
        f"publish_{task_suffix}",
        GOLD_PUBLISHER_DIR,
        command,
        execution_timeout=DB_LOADER_EXECUTION_TIMEOUT,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )
