"""seoul-pop-nowcasting CLI를 실행하는 태스크 빌더.

`estimate` 서브커맨드만 쓴다(`backfill-archive`는 1회성 CSV 적재용이라 정기
스케줄 대상이 아니다).
"""

from __future__ import annotations

from config.schedules import NOWCASTING_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE, KST_WINDOW_START

NOWCASTING_DIR = str(REPO_ROOT / "nowcaster")


def build_nowcasting_task(dag):
    """Collector와 같은 logical window를 넘겨 authoritative 실측만 승격한다."""
    cmd = (
        "uv run --frozen python main.py estimate "
        f"--target-date {KST_DATE} --source-window-start {KST_WINDOW_START}"
    )
    return build_module_task(
        dag,
        "run_nowcasting_estimate",
        NOWCASTING_DIR,
        cmd,
        execution_timeout=NOWCASTING_EXECUTION_TIMEOUT,
    )
