

"""실시간 E2E 파이프라인의 Walking Skeleton DAG.

현재 단계에서는 Collector만 실제 CLI를 호출하고, 조합기/추론기/Gold 적재는
실제 구현이 준비될 때까지 stub Task로 둔다.

목표 흐름:
    collect
        -> build_inference_input
        -> run_inference
        -> build_serving_output
        -> load_gold

모든 Task는 같은 Airflow logical_date(window_start)를 사용한다.
"""

import pendulum
from airflow import DAG
from airflow.operators.bash import BashOperator

from config.schedules import TIMEZONE
from orchestration.collector_task import build_collector_task

SOURCE_ID = "bike_station_realtime"
E2E_WINDOW_START = (
    "{{ logical_date.in_timezone('Asia/Seoul')"
    ".start_of('minute')"
    ".subtract(minutes=logical_date.in_timezone('Asia/Seoul').minute % 5)"
    ".isoformat() }}"
)


with DAG(
    dag_id="e2e_realtime",
    schedule=None,
    start_date=pendulum.datetime(
        2026,
        8,
        14,
        tz=TIMEZONE,
    ),
    catchup=False,
    max_active_runs=1,
    tags=["e2e", "realtime"],
) as dag:
    collect = build_collector_task(SOURCE_ID)
    collect.bash_command = (
        "/bin/bash -c \"cd /workspace/collector && "
        "env -u VIRTUAL_ENV uv run python main.py "
        f"--source {SOURCE_ID} "
        f"--window-start {E2E_WINDOW_START}\""
    )

    build_inference_input = BashOperator(
        task_id="build_inference_input",
        bash_command=(
            'echo "[stub][combiner] build inference input '
            f'window_start={E2E_WINDOW_START}"'
        ),
    )

    run_inference = BashOperator(
        task_id="run_inference",
        bash_command=(
            'echo "[stub][inference] run inference '
            f'window_start={E2E_WINDOW_START}"'
        ),
    )

    build_serving_output = BashOperator(
        task_id="build_serving_output",
        bash_command=(
            'echo "[stub][combiner] build serving output '
            f'window_start={E2E_WINDOW_START}"'
        ),
    )

    load_gold = BashOperator(
        task_id="load_gold",
        bash_command=(
            'echo "[stub][gold] load RDS '
            f'window_start={E2E_WINDOW_START}"'
        ),
    )

    (
        collect
        >> build_inference_input
        >> run_inference
        >> build_serving_output
        >> load_gold
    )