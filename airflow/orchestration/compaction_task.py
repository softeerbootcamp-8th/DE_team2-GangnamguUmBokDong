"""collector의 compaction CLI를 실행하는 태스크 빌더.

`--source`만 넘긴다. 검사 범위(며칠 전까지 볼지)는 소스 설정의 `backfill.max_age`와
배치 복구 하한에서 collector가 스스로 유도하므로, Airflow는 백필 창을 알 필요가 없다.
수집 태스크가 페이지 번호를 모르는 것과 같은 원칙이다.
"""

from __future__ import annotations

from config.schedules import COMPACTION_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task

COLLECTOR_DIR = str(REPO_ROOT / "collector")


def build_compaction_task(dag, source_id: str):
    cmd = f"uv run --frozen python compact.py --source {source_id}"
    return build_module_task(
        dag,
        f"compact_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=COMPACTION_EXECUTION_TIMEOUT,
    )
