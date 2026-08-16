"""db-loader CLI 진입점.

Airflow가 (추후 wiring 시) `uv run python main.py --table <table> --window-start <run_id>`
형태로 호출하는 것을 전제로 한다. Silver를 읽어 변환한 뒤 Gold DB에 upsert하고,
성공하면 0, 실패하면 0이 아닌 코드로 종료한다.

    uv run python main.py --table stations --window-start 2026-08-16T09:05:00+00:00
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from core.db import get_connection

import s3_reader
from config import TABLE_SPECS
from upsert import upsert


def run(table: str, window_start: datetime) -> None:
    spec = TABLE_SPECS[table]
    silver = s3_reader.read_silver(spec.source_id, window_start).to_pandas()

    if table == "station_stock":
        rows = spec.transform(silver, observed_at=window_start)
    else:
        rows = spec.transform(silver)

    with get_connection() as conn:
        upsert(conn, table, rows, spec.conflict_cols, spec.update_cols)
        conn.commit()

    print(f"upserted {len(rows)} rows into {table}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Silver parquet을 읽어 Gold DB에 upsert한다.")
    parser.add_argument("--table", required=True, choices=sorted(TABLE_SPECS))
    parser.add_argument("--window-start", required=True, help="ISO8601 시각, 예: 2026-08-16T09:05:00+00:00")
    args = parser.parse_args()

    window_start = datetime.fromisoformat(args.window_start)

    try:
        run(args.table, window_start)
    except Exception as exc:  # noqa: BLE001 - CLI 최상위: 실패를 종료 코드로 변환하기 위해 넓게 잡는다
        print(f"db-loader failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
