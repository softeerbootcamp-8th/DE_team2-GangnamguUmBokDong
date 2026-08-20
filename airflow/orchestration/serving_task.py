"""Coordinated Gold serving prepare/finalize CLI 태스크를 만든다."""

from __future__ import annotations

import json

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.task.trigger_rule import TriggerRule

from config.schedules import DB_LOADER_EXECUTION_TIMEOUT
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

LOADER_DIR = str(REPO_ROOT / "loader")


def xcom_ref(task_id: str, key: str, field: str) -> str:
    """URI·SHA JSON XCom의 exact nested field를 Jinja expression으로 만든다."""
    return (
        "{{ ti.xcom_pull(task_ids='" + task_id + "')['" + key + "']['" + field + "'] }}"
    )


def build_prepare_serving_task(dag: DAG) -> BashOperator:
    """최신 source와 current model support를 immutable plan으로 준비한다."""
    return build_module_task(
        dag,
        "prepare_serving_plan",
        LOADER_DIR,
        (
            "uv run --frozen python serving_cli.py prepare "
            '--logical-dttm "$SERVING_LOGICAL_DTTM"'
        ),
        env={"SERVING_LOGICAL_DTTM": KST_WINDOW_START},
        output_processor=json.loads,
        execution_timeout=DB_LOADER_EXECUTION_TIMEOUT,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )


def build_finalize_serving_task(
    dag: DAG,
    *,
    plan_task_id: str,
    inference_task_id: str,
) -> BashOperator:
    """Plan-bound inference를 station·stock·demand·weather로 원자 게시한다.

    Source/catalog drift는 같은 exact refs로 finalize만 재시도해도 회복되지 않는다.
    새 plan부터 전체 DAG run을 다시 시작하도록 이 task의 retry를 0으로 고정한다.
    """
    return build_module_task(
        dag,
        "finalize_serving_release",
        LOADER_DIR,
        (
            "uv run --frozen python serving_cli.py finalize "
            '--plan-uri "$PLAN_URI" --plan-sha256 "$PLAN_SHA256" '
            '--inference-uri "$INFERENCE_URI" '
            '--inference-sha256 "$INFERENCE_SHA256"'
        ),
        env={
            "PLAN_URI": xcom_ref(plan_task_id, "plan", "uri"),
            "PLAN_SHA256": xcom_ref(plan_task_id, "plan", "byte_sha256"),
            "INFERENCE_URI": xcom_ref(
                inference_task_id,
                "inference",
                "uri",
            ),
            "INFERENCE_SHA256": xcom_ref(
                inference_task_id,
                "inference",
                "byte_sha256",
            ),
        },
        output_processor=json.loads,
        execution_timeout=DB_LOADER_EXECUTION_TIMEOUT,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        retries=0,
    )
