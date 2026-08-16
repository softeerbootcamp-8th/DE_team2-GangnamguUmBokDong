"""MinIO + PostgreSQL 기반 Mock E2E 검증 DAG."""

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from config.schedules import TIMEZONE


E2E_WINDOW_START = (
    "{{ logical_date.in_timezone('Asia/Seoul')"
    ".start_of('minute')"
    ".subtract(minutes=logical_date.in_timezone('Asia/Seoul').minute % 5)"
    ".isoformat() }}"
)

E2E_RUN_KEY = (
    "{{ logical_date.in_timezone('Asia/Seoul')"
    ".start_of('minute')"
    ".subtract(minutes=logical_date.in_timezone('Asia/Seoul').minute % 5)"
    ".format('YYYYMMDDTHHmm') }}"
)

COMMON_ENV = "S3_ENDPOINT_URL=http://minio:9000"


with DAG(
    dag_id="e2e_mock_realtime",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        8,
        14,
        tz=TIMEZONE,
    ),
    catchup=False,
    max_active_runs=1,
    tags=["e2e", "mock", "realtime"],
) as dag:

    collect = BashOperator(
        task_id="collect",
        bash_command=(
            f"{COMMON_ENV} "
            "uv run python /workspace/mocks/collector/main.py "
            f"--window-start '{E2E_WINDOW_START}' "
            f"--run-key '{E2E_RUN_KEY}'"
        ),
        cwd="/workspace/airflow",
    )

    build_inference_input = BashOperator(
        task_id="build_inference_input",
        bash_command=(
            f"{COMMON_ENV} "
            "uv run python /workspace/mocks/combiner/main.py "
            "--job inference-input "
            f"--run-key '{E2E_RUN_KEY}'"
        ),
        cwd="/workspace/airflow",
    )

    run_inference = BashOperator(
        task_id="run_inference",
        bash_command=(
            f"{COMMON_ENV} "
            "uv run python /workspace/mocks/inference/main.py "
            f"--window-start '{E2E_WINDOW_START}' "
            f"--run-key '{E2E_RUN_KEY}'"
        ),
        cwd="/workspace/airflow",
    )

    build_serving_output = BashOperator(
        task_id="build_serving_output",
        bash_command=(
            f"{COMMON_ENV} "
            "uv run python /workspace/mocks/combiner/main.py "
            "--job serving-output "
            f"--run-key '{E2E_RUN_KEY}'"
        ),
        cwd="/workspace/airflow",
    )

    load_gold = BashOperator(
        task_id="load_gold",
        bash_command=(
            f"{COMMON_ENV} "
            "uv run python /workspace/mocks/gold/main.py "
            f"--run-key '{E2E_RUN_KEY}'"
        ),
        cwd="/workspace/airflow",
    )

    (
        collect
        >> build_inference_input
        >> run_inference
        >> build_serving_output
        >> load_gold
    )