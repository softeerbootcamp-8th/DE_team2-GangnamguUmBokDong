"""실제 컴포넌트 없이 Airflow 전체 E2E 흐름을 검증하는 Mock DAG."""

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

MOCK_ROOT = "/tmp/e2e_mock"


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

    mock_collect = BashOperator(
        task_id="mock_collect",
        bash_command=f"""
        set -euo pipefail

        RUN_DIR="{MOCK_ROOT}/{E2E_RUN_KEY}"
        mkdir -p "$RUN_DIR"

        cat > "$RUN_DIR/silver.json" <<EOF
        {{
          "window_start": "{E2E_WINDOW_START}",
          "station_id": 102,
          "station_name": "102. 망원역 1번출구 앞",
          "current_bikes": 8,
          "temperature": 27.5,
          "population": 3200
        }}
        EOF

        echo "[mock collector] $RUN_DIR/silver.json"
        cat "$RUN_DIR/silver.json"
        """,
    )

    build_inference_input = BashOperator(
        task_id="build_inference_input",
        bash_command=f"""
        set -euo pipefail

        RUN_DIR="{MOCK_ROOT}/{E2E_RUN_KEY}"

        test -f "$RUN_DIR/silver.json"

        cat > "$RUN_DIR/inference_input.json" <<EOF
        {{
          "window_start": "{E2E_WINDOW_START}",
          "station_id": 102,
          "current_bikes": 8,
          "temperature": 27.5,
          "population": 3200
        }}
        EOF

        echo "[mock combiner] inference input created"
        cat "$RUN_DIR/inference_input.json"
        """,
    )

    run_inference = BashOperator(
        task_id="run_inference",
        bash_command=f"""
        set -euo pipefail

        RUN_DIR="{MOCK_ROOT}/{E2E_RUN_KEY}"

        test -f "$RUN_DIR/inference_input.json"

        cat > "$RUN_DIR/prediction.json" <<EOF
        {{
          "window_start": "{E2E_WINDOW_START}",
          "station_id": 102,
          "predicted_dttm": "{E2E_WINDOW_START}",
          "predicted_rent_cnt": 5.2,
          "predicted_return_cnt": 3.1
        }}
        EOF

        echo "[mock inference] prediction created"
        cat "$RUN_DIR/prediction.json"
        """,
    )

    build_serving_output = BashOperator(
        task_id="build_serving_output",
        bash_command=f"""
        set -euo pipefail

        RUN_DIR="{MOCK_ROOT}/{E2E_RUN_KEY}"

        test -f "$RUN_DIR/silver.json"
        test -f "$RUN_DIR/prediction.json"

        cat > "$RUN_DIR/gold.json" <<EOF
        {{
          "sta_id": 102,
          "predicted_dttm": "{E2E_WINDOW_START}",
          "predicted_rent_cnt": 5.2,
          "predicted_return_cnt": 3.1,
          "batch_run_at": "{E2E_WINDOW_START}"
        }}
        EOF

        echo "[mock combiner] serving output created"
        cat "$RUN_DIR/gold.json"
        """,
    )

    load_gold = BashOperator(
        task_id="load_gold",
        bash_command=f"""
        set -euo pipefail

        RUN_DIR="{MOCK_ROOT}/{E2E_RUN_KEY}"
        MOCK_RDS="{MOCK_ROOT}/mock_rds/forecast_points"

        test -f "$RUN_DIR/gold.json"

        mkdir -p "$MOCK_RDS"

        cp \
          "$RUN_DIR/gold.json" \
          "$MOCK_RDS/{E2E_RUN_KEY}.json"

        echo "[mock RDS] upsert completed"
        cat "$MOCK_RDS/{E2E_RUN_KEY}.json"
        """,
    )

    (
        mock_collect
        >> build_inference_input
        >> run_inference
        >> build_serving_output
        >> load_gold
    )