"""CLI 진입점: anchor 시점 기준 권역별 재배치 라우트를 만들어 S3에 저장한다.

Airflow(compute_routes 태스크)가 compute_urgency 뒤에 `--date/--hour/--minute`
(KST 벽시계 시각, main.py/urgency와 동일한 값)으로 실행한다. urgency는
compute_urgency가 S3에 이미 써둔 결과를 routes.py가 그대로 읽어서 쓰고
(재계산하지 않음), dispatched 넷팅을 위한 RDS 조회만 이 배치의 유일한 예외다.
"""

from __future__ import annotations

import argparse
import sys

import pandas as pd
from core.s3 import write_parquet

import routes
from reader import anchor_timestamp


def _routes_key(window_start: pd.Timestamp) -> str:
    return f"routes/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/routes_{window_start:%H%M}.parquet"


def _route_stops_key(window_start: pd.Timestamp) -> str:
    return f"route_stops/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/route_stops_{window_start:%H%M}.parquet"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="권역별 재배치 라우트 배치 계산")
    parser.add_argument("--date", required=True, help="YYYY-MM-DD (KST)")
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument("--minute", type=int, required=True)
    args = parser.parse_args(argv)

    anchor = anchor_timestamp(args.date, args.hour, args.minute)
    route_rows, stop_rows = routes.compute_all(anchor)

    routes_path = _routes_key(anchor)
    stops_path = _route_stops_key(anchor)
    write_parquet(pd.DataFrame(route_rows, columns=["route_id", "region", "status", "proposed_at"]), routes_path)
    write_parquet(
        pd.DataFrame(stop_rows, columns=["route_id", "visit_order", "sta_id", "action", "bike_cnt"]), stops_path
    )
    print(f"라우트 결과 저장: {routes_path}({len(route_rows)}개), {stops_path}({len(stop_rows)}개)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
