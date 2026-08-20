"""Exact urgency authority ref로 rebalance route publication 태스크를 만든다."""

from __future__ import annotations

import json

from config.schedules import ROUTES_EXECUTION_TIMEOUT
from orchestration.serving_task import LOADER_DIR, xcom_ref
from orchestration.task_builder import build_module_task


def build_routes_task(dag, *, urgency_task_id: str = "publish_station_urgency"):
    """Urgency JSON XCom의 URI·SHA만 받아 route를 게시한다."""
    cmd = (
        "uv run --frozen python serving_cli.py route "
        '--urgency-uri "$URGENCY_URI" --urgency-sha256 "$URGENCY_SHA256"'
    )
    return build_module_task(
        dag,
        "publish_rebalance_route",
        LOADER_DIR,
        cmd,
        execution_timeout=ROUTES_EXECUTION_TIMEOUT,
        env={
            "URGENCY_URI": xcom_ref(urgency_task_id, "station_urgency", "uri"),
            "URGENCY_SHA256": xcom_ref(
                urgency_task_id,
                "station_urgency",
                "byte_sha256",
            ),
        },
        output_processor=json.loads,
        retries=0,
    )
