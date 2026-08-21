"""Paused 운영 DAG를 scheduler와 분리된 Airflow test run으로 실행한다."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

from airflow.sdk import DagRunState

_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_ROOT / "airflow"))

from dags.realtime_5min import dag


def _logical_dttm(raw: str) -> datetime:
    """Timezone offset을 가진 smoke logical time을 파싱한다."""
    logical = datetime.fromisoformat(raw)
    if logical.tzinfo is None or logical.utcoffset() is None:
        raise ValueError("logical time에 timezone offset이 필요합니다.")
    return logical


def main(argv: list[str] | None = None) -> int:
    """Airflow metadata에 보존되는 단일 realtime_5min test run을 실행한다."""
    parser = argparse.ArgumentParser(description="realtime_5min local smoke를 실행한다.")
    parser.add_argument("--logical-dttm", required=True)
    args = parser.parse_args(argv)
    try:
        logical = _logical_dttm(args.logical_dttm)
        dag_run = dag.test(logical_date=logical, run_after=logical)
    except Exception as exc:  # noqa: BLE001 - CLI 경계에서 실패를 보존한다.
        print(f"Airflow local DAG test failed: {exc}", file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "dag_id": dag_run.dag_id,
                "run_id": dag_run.run_id,
                "state": dag_run.state,
            },
            ensure_ascii=False,
            sort_keys=True,
        )
    )
    return 0 if dag_run.state == DagRunState.SUCCESS else 1


if __name__ == "__main__":
    sys.exit(main())
