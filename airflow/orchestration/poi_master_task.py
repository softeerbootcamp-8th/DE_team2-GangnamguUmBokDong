"""POI Master 갱신과 exact 실행 버전 선택 CLI 태스크를 만든다."""

from __future__ import annotations

import json

from config.schedules import (
    POI_MASTER_REFRESH_EXECUTION_TIMEOUT,
    POI_MASTER_RESOLVE_EXECUTION_TIMEOUT,
)
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

POI_MASTER_DIR = str(REPO_ROOT / "poi_master")


def poi_master_ref_env(task_id: str) -> dict[str, str]:
    """Resolver XCom에서 downstream CLI가 공유할 exact POI Master 환경을 만든다."""
    payload = f"ti.xcom_pull(task_ids='{task_id}')"
    return {
        "POI_MASTER_MODE": "{{ " + payload + "['mode'] }}",
        "POI_MASTER_MANIFEST_URI": (
            "{{ " + payload + ".get('manifest_uri') or '' }}"
        ),
        "POI_MASTER_MANIFEST_SHA256": (
            "{{ " + payload + ".get('manifest_sha256') or '' }}"
        ),
    }


def build_poi_master_refresh_task(dag):
    """서울시 파일 변경을 검증하고 새 POI Master를 게시하는 태스크를 만든다."""
    return build_module_task(
        dag,
        "refresh_poi_master",
        POI_MASTER_DIR,
        "uv run --frozen python main.py refresh",
        execution_timeout=POI_MASTER_REFRESH_EXECUTION_TIMEOUT,
        output_processor=json.loads,
        uv_environment_name="poi-master",
    )


def build_poi_master_resolve_task(dag):
    """한 realtime run이 사용할 최신 정상 POI Master ref를 고정하는 태스크를 만든다."""
    return build_module_task(
        dag,
        "resolve_poi_master",
        POI_MASTER_DIR,
        (
            "uv run --frozen python main.py resolve "
            '--as-of "$POI_MASTER_AS_OF"'
        ),
        env={"POI_MASTER_AS_OF": KST_WINDOW_START},
        execution_timeout=POI_MASTER_RESOLVE_EXECUTION_TIMEOUT,
        output_processor=json.loads,
        uv_environment_name="poi-master",
    )
