"""seoul-pop-normalizer CLI를 실행하는 태스크 빌더.

한 번 실행하면 현재 시각과 실시간 도시데이터가 주는 향후 12시간 예측 시각을 모두
보정한다. baseline은 항상 nowcaster 추정치(`nowcast.parquet`)이므로 예전의
`--baseline-date-mode strict|latest`와 strict/fallback 태스크 쌍은 없앴다 — 실측
원본은 관측일이 수집일보다 4~5일 늦어 "그날 파티션이 있다"가 "그날 격자다"를
뜻하지 않았다.
"""

from __future__ import annotations

from config.schedules import NORMALIZER_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

NORMALIZER_DIR = str(REPO_ROOT / "normalizer")


def build_normalizer_task(dag, task_id: str = "run_normalizer"):
    cmd = f"uv run --frozen python main.py --window-start {KST_WINDOW_START}"
    return build_module_task(
        dag, task_id, NORMALIZER_DIR, cmd, execution_timeout=NORMALIZER_EXECUTION_TIMEOUT
    )


def build_station_master_enrichment_task(dag):
    """API 대여소 master에 생활인구 CELL_ID를 보강하는 태스크를 만든다."""
    cmd = f"uv run --frozen python station_master.py --window-start {KST_WINDOW_START}"
    return build_module_task(dag, "enrich_station_master", NORMALIZER_DIR, cmd)
