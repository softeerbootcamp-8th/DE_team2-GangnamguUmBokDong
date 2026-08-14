"""Collector retry queue를 기반으로 누락 데이터를 복구하는 Backfill DAG."""

from __future__ import annotations

import json
import subprocess

import pendulum
from airflow.decorators import dag, task

COLLECTOR_DIR = "/workspace/collector"
BACKFILL_SOURCE_ID = "bike_station_realtime"


@dag(
    dag_id="collector_backfill",
    schedule=None,
    start_date=pendulum.datetime(2026, 8, 14, tz="Asia/Seoul"),
    catchup=False,
    max_active_runs=1,
    tags=["collection", "backfill"],
)
def collector_backfill():
    """Collector가 제공하는 백필 대상 목록을 받아 대상별 복구 작업을 실행한다."""

    @task
    def list_backfill_targets(source_id: str) -> list[dict[str, str]]:
        """Collector CLI에서 백필 대상 목록을 조회한다.

        Args:
            source_id: Collector YAML에 정의된 source 식별자.

        Returns:
            source_id와 window_start만 포함한 백필 대상 목록.
        """
        result = subprocess.run(
            [
                "uv",
                "run",
                "python",
                "main.py",
                "--list-backfill-targets",
                "--source",
                source_id,
            ],
            cwd=COLLECTOR_DIR,
            capture_output=True,
            text=True,
            check=True,
        )
        return json.loads(result.stdout)

    @task
    def run_backfill(target: dict[str, str]) -> None:
        """백필 대상 하나에 대해 Collector의 --backfill 실행을 요청한다.

        Args:
            target: source_id와 window_start를 포함한 백필 대상.
        """
        subprocess.run(
            [
                "uv",
                "run",
                "python",
                "main.py",
                "--source",
                target["source_id"],
                "--window-start",
                target["window_start"],
                "--backfill",
            ],
            cwd=COLLECTOR_DIR,
            check=True,
        )

    targets = list_backfill_targets(BACKFILL_SOURCE_ID)
    run_backfill.expand(target=targets)


dag = collector_backfill()