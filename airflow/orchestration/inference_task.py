"""Serving plan에 결합된 immutable inference publication 태스크를 만든다."""

from __future__ import annotations

import json

from airflow.task.trigger_rule import TriggerRule
from config.schedules import INFERENCE_EXECUTION_TIMEOUT

from orchestration.serving_task import xcom_ref
from orchestration.task_builder import REPO_ROOT, build_module_task

ML_DIR = str(REPO_ROOT / "ml")


def build_inference_task(dag, *, plan_task_id: str = "prepare_serving_plan"):
    """Exact plan XCom ref를 읽어 pinned inference authority를 게시한다."""
    cmd = (
        "uv --project inference run --frozen python -m inference.publication_cli "
        '--plan-uri "$PLAN_URI" --plan-sha256 "$PLAN_SHA256"'
    )
    return build_module_task(
        dag,
        "run_inference",
        ML_DIR,
        cmd,
        execution_timeout=INFERENCE_EXECUTION_TIMEOUT,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        env={
            "PLAN_URI": xcom_ref(plan_task_id, "plan", "uri"),
            "PLAN_SHA256": xcom_ref(plan_task_id, "plan", "byte_sha256"),
        },
        output_processor=json.loads,
        uv_environment_name="ml-inference",
    )
