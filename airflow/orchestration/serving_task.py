"""Coordinated Gold serving prepare/finalize CLI 태스크를 만든다."""

from __future__ import annotations

import json

from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.sensors.bash import BashSensor
from airflow.task.trigger_rule import TriggerRule
from callbacks.task_callbacks import on_failure_callback, on_success_callback
from config.schedules import (
    DB_LOADER_EXECUTION_TIMEOUT,
    WEATHER_MANIFEST_POKE_INTERVAL_SECONDS,
    WEATHER_MANIFEST_WAIT_TIMEOUT_SECONDS,
)

from airflow import DAG
from orchestration.task_builder import REPO_ROOT, build_module_task
from orchestration.templates import KST_WINDOW_START

LOADER_DIR = str(REPO_ROOT / "loader")
_LOADER_UV_ENV = "/opt/venvs/modules/loader"


def xcom_ref(task_id: str, key: str, field: str) -> str:
    """URI·SHA JSON XCom의 exact nested field를 Jinja expression으로 만든다."""
    return (
        "{{ ti.xcom_pull(task_ids='" + task_id + "')['" + key + "']['" + field + "'] }}"
    )


def build_weather_manifest_sensor(dag: DAG) -> BashSensor:
    """예정된 날씨 authority를 2초마다 최대 30초 기다린다.

    Timeout은 ``soft_fail``로 SKIPPED가 된다. Station 수집이 성공한 경우 prepare의
    ``NONE_FAILED_MIN_ONE_SUCCESS``가 이전 유효 날씨 snapshot으로 계속 진행시킨다.
    """
    return BashSensor(
        task_id="wait_for_weather_manifests",
        bash_command=(
            "env -u VIRTUAL_ENV "
            f"UV_PROJECT_ENVIRONMENT={_LOADER_UV_ENV} "
            f"uv run --project {LOADER_DIR} --frozen python "
            f"{LOADER_DIR}/serving_cli.py weather-ready "
            f"--logical-dttm '{KST_WINDOW_START}'"
        ),
        poke_interval=WEATHER_MANIFEST_POKE_INTERVAL_SECONDS,
        timeout=WEATHER_MANIFEST_WAIT_TIMEOUT_SECONDS,
        mode="poke",
        soft_fail=True,
        retries=0,
        on_success_callback=on_success_callback,
        on_failure_callback=on_failure_callback,
        dag=dag,
    )


def build_prepare_serving_task(
    dag: DAG,
    *,
    trigger_rule: str = TriggerRule.ALL_SUCCESS,
) -> BashOperator:
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
        trigger_rule=trigger_rule,
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
