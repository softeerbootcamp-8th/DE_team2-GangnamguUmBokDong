"""Coordinated serving release exact refs로 urgency publication 태스크를 만든다."""

from __future__ import annotations

import json

from config.schedules import URGENCY_EXECUTION_TIMEOUT
from orchestration.serving_task import LOADER_DIR, xcom_ref
from orchestration.task_builder import build_module_task


def build_urgency_task(dag, *, final_task_id: str = "finalize_serving_release"):
    """Final release의 station·demand·stock exact refs로 urgency를 게시한다."""
    cmd = (
        "uv run --frozen python serving_cli.py urgency "
        '--station-uri "$STATION_URI" --station-sha256 "$STATION_SHA256" '
        '--demand-uri "$DEMAND_URI" --demand-sha256 "$DEMAND_SHA256" '
        '--stock-uri "$STOCK_URI" --stock-sha256 "$STOCK_SHA256"'
    )
    return build_module_task(
        dag,
        "publish_station_urgency",
        LOADER_DIR,
        cmd,
        execution_timeout=URGENCY_EXECUTION_TIMEOUT,
        env={
            "STATION_URI": xcom_ref(final_task_id, "station", "uri"),
            "STATION_SHA256": xcom_ref(final_task_id, "station", "byte_sha256"),
            "DEMAND_URI": xcom_ref(final_task_id, "station_demand_forecast", "uri"),
            "DEMAND_SHA256": xcom_ref(
                final_task_id,
                "station_demand_forecast",
                "byte_sha256",
            ),
            "STOCK_URI": xcom_ref(final_task_id, "station_stock", "uri"),
            "STOCK_SHA256": xcom_ref(final_task_id, "station_stock", "byte_sha256"),
        },
        output_processor=json.loads,
        retries=0,
    )
