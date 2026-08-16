"""loader CLI를 실행하는 태스크 빌더."""

from __future__ import annotations

from config.schedules import DB_LOADER_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

DB_LOADER_DIR = str(REPO_ROOT / "loader")


def build_db_loader_task(dag, table: str, *, trigger_rule="all_success"):
    cmd = f"uv run --frozen python main.py --table {table} --window-start {KST_WINDOW_START}"
    return build_module_task(
        dag,
        f"load_{table}",
        DB_LOADER_DIR,
        cmd,
        execution_timeout=DB_LOADER_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )
