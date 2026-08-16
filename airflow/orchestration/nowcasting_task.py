"""seoul-pop-nowcasting CLI를 실행하는 태스크 빌더.

`estimate` 서브커맨드만 쓴다(`backfill-archive`는 1회성 CSV 적재용이라 정기
스케줄 대상이 아니다).
"""

from __future__ import annotations

from config.schedules import NOWCASTING_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_DATE

NOWCASTING_DIR = str(REPO_ROOT / "seoul-pop-nowcasting")


def build_nowcasting_task(dag):
    cmd = f"uv run python main.py estimate --target-date {KST_DATE}"
    return build_module_task(
        dag,
        "run_nowcasting_estimate",
        NOWCASTING_DIR,
        cmd,
        execution_timeout=NOWCASTING_EXECUTION_TIMEOUT,
    )
