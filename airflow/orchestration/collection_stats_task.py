"""collector manifest에서 수집 통계(성공/실패/결측치/이상치)를 읽어오는 태스크.

`daily_collection_report`/`hourly_collection_alert` DAG가 소스마다 이 태스크로
`collector/main.py --report-window-stats`를 호출해 JSON을 XCom에 남긴다.
"""

from __future__ import annotations

import json
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from orchestration.collector_task import COLLECTOR_DIR
from orchestration.task_builder import build_module_task

_STATS_EXECUTION_TIMEOUT = timedelta(seconds=60)


def build_window_stats_task(
    dag,
    source_id: str,
    *,
    day_template: str,
    hour_template: str | None = None,
) -> BashOperator:
    """해당 KST 날짜(및 선택적으로 시)의 수집 통계를 JSON으로 받는 태스크를 만든다."""
    cmd = (
        "uv run --frozen python main.py --report-window-stats "
        f'--source {source_id} --window-day "{day_template}"'
    )
    if hour_template is not None:
        cmd += f' --window-hour "{hour_template}"'
    return build_module_task(
        dag,
        f"collection_stats_{source_id}",
        COLLECTOR_DIR,
        cmd,
        output_processor=json.loads,
        execution_timeout=_STATS_EXECUTION_TIMEOUT,
        retries=1,
    )
