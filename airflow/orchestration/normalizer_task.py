"""seoul-pop-normalizer CLI를 실행하는 태스크 빌더.

`--baseline-date-mode`는 normalizer 자신의 docstring이 명시한 두 값을 그대로 쓴다:
strict(기본, 정확한 날짜 파티션 없으면 실패) / latest(Airflow fallback 태스크용,
가장 최근 파티션으로 대체). 5분 DAG는 strict를 먼저 시도하고, 실패하면(그날
아침 daily DAG가 아직 안 돌았을 때) latest로 재시도하는 두 태스크 쌍으로 구성한다.
"""

from __future__ import annotations

from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

NORMALIZER_DIR = str(REPO_ROOT / "normalizer")


def build_normalizer_task(dag, task_id: str, baseline_date_mode: str, *, trigger_rule="all_success"):
    cmd = (
        f"uv run python main.py --window-start {KST_WINDOW_START} "
        f"--baseline-date-mode {baseline_date_mode}"
    )
    return build_module_task(dag, task_id, NORMALIZER_DIR, cmd, trigger_rule=trigger_rule)
