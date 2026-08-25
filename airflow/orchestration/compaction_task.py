"""Collector의 날짜 단위 Archive compaction 태스크를 만든다.

일반 Airflow 배치는 ``--source``만 넘겨 Collector가 소스의 ``backfill.max_age``와
배치 복구 하한에서 검사 범위를 유도하게 한다. 이 recovery sweep은 DAG가 실행되지
않았거나 이전 압축이 실패한 날짜도 다음 실행에서 다시 검사한다. 특정 날짜만 처리하는
수동·운영 호출에는 선택적으로 ``target_date``를 전달할 수 있다.
"""

from __future__ import annotations

from airflow.task.trigger_rule import TriggerRule
from config.schedules import COMPACTION_EXECUTION_TIMEOUT

from orchestration.task_builder import REPO_ROOT, build_module_task

COLLECTOR_DIR = str(REPO_ROOT / "collector")


def build_compaction_task(
    dag,
    source_id: str,
    *,
    target_date: str | None = None,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """소스의 기본 범위 또는 지정 날짜를 압축하는 태스크를 만든다."""
    date_arg = f" --date {target_date}" if target_date else ""
    cmd = f"uv run --frozen python compact.py --source {source_id}{date_arg}"
    return build_module_task(
        dag,
        f"compact_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=COMPACTION_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )


def build_cold_bronze_compaction_task(
    dag,
    source_id: str,
    target_date: str,
    *,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """검증이 끝난 날짜의 모든 Hot Bronze revision을 Cold 파일로 묶는다."""
    cmd = (
        f"uv run --frozen python cold_compact.py --source {source_id} "
        f"--date {target_date}"
    )
    return build_module_task(
        dag,
        f"cold_compact_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=COMPACTION_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )


def build_silver_gc_task(
    dag,
    source_id: str,
    target_date: str,
    *,
    require_archive: bool = False,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
):
    """Cold와 선택적인 Archive 검증 뒤 보존기간 지난 Silver를 정리한다."""
    archive_arg = " --require-archive" if require_archive else ""
    cmd = (
        f"uv run --frozen python silver_gc_cli.py --source {source_id} "
        f"--date {target_date}{archive_arg}"
    )
    return build_module_task(
        dag,
        f"gc_silver_{source_id}",
        COLLECTOR_DIR,
        cmd,
        execution_timeout=COMPACTION_EXECUTION_TIMEOUT,
        trigger_rule=trigger_rule,
    )
