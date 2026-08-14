"""Collector retry queue를 기반으로 누락 데이터를 복구하는 Backfill DAG."""

from __future__ import annotations

import json
import subprocess

import pendulum
from airflow.decorators import dag, task

from orchestration.collector_task import build_backfill_task

COLLECTOR_DIR = "/workspace/collector"
BACKFILL_SOURCE_IDS = (
    "bike_rental_history",
    "cultural_event",
    "living_population_grid",
    "weather_ultra_short_term",
    "weather_short_term_forecast",
)


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
    def list_backfill_targets(
        source_ids: tuple[str, ...],
    ) -> list[dict[str, str]]:
        """Collector CLI에서 모든 백필 대상 목록을 조회한다.

        Args:
            source_ids: Backfill이 활성화된 Collector source 식별자 목록.

        Returns:
            source_id와 window_start를 포함한 전체 백필 대상 목록.
        """
        targets: list[dict[str, str]] = []

        for source_id in source_ids:
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
            targets.extend(json.loads(result.stdout))

        return targets

    targets = list_backfill_targets(BACKFILL_SOURCE_IDS)
    build_backfill_task(targets)


dag = collector_backfill()