"""collector CLI를 실행하는 태스크 빌더.

호출 계약(docs/airflow/implementation-plan.md 참고): `--source`/`--window-start`만
전달하고, 페이지 번호·API URL·fetch round 등 collector 내부 세부사항은 Airflow가
알지 않는다.
"""

from __future__ import annotations

from config.schedules import DEFAULT_EXECUTION_TIMEOUT, EXECUTION_TIMEOUT_OVERRIDES
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

COLLECTOR_DIR = str(REPO_ROOT / "collector")


def build_collector_task(dag, source_id: str):
    timeout = EXECUTION_TIMEOUT_OVERRIDES.get(source_id, DEFAULT_EXECUTION_TIMEOUT)
    cmd = f"uv run --frozen python main.py --source {source_id} --window-start {KST_WINDOW_START}"
    return build_module_task(
        dag,
        f"collect_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=timeout,
    )
