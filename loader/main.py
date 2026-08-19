"""Silver 계층 데이터를 읽어 Gold DB에 Upsert하는 CLI 진입점 모듈."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime

from core.db import get_connection
from core.upsert import upsert

from config import TABLE_SPECS


def _only_known_stations(rows: list[dict], known_station_ids: set[str]) -> list[dict]:
    """stations FK가 존재하는 urgency row만 반환한다."""
    return [row for row in rows if row["sta_id"] in known_station_ids]


def _retire_stale_proposed_routes(conn) -> None:
    """이번 배치가 새 proposed 라우트를 넣기 전에, 아직 proposed인(=아무도 안
    건드린) 예전 라우트를 지운다. compute_routes는 매 사이클 전체 권역의 수요를
    다시 계산하므로, 지난 사이클의 proposed는 이번 사이클 결과로 완전히
    대체되는 게 맞다 — 그대로 두면 아무도 안 건드린 예전 제안이 사이클마다
    계속 쌓인다. dispatched/completed로 이미 넘어간 라우트는 운영자가 실제로
    처리한 기록이라 안 건드린다. S3(routes_main.py가 매 사이클 써두는 parquet)에
    "그때 뭘 제안했었는지"는 이미 영구히 남으므로, 아무도 안 건드린 proposed를
    RDS에서 지워도 잃는 정보가 없다."""
    with conn.cursor() as cur:
        cur.execute(
            "DELETE FROM rebalance_route_stops "
            "WHERE route_id IN (SELECT route_id FROM rebalance_routes WHERE status = 'proposed')"
        )
        cur.execute("DELETE FROM rebalance_routes WHERE status = 'proposed'")


def run(table: str, window_start: datetime) -> None:
    """지정된 테이블 스펙에 따라 S3 데이터를 읽고 변환하여 Gold DB에 Upsert한다.

    args:
        table: 대상 테이블 스펙 식별자
        window_start: 수집 기준 시각 (KST)
    """
    spec = TABLE_SPECS[table]
    silver = spec.read(window_start)

    if table == "station_stock":
        rows = spec.transform(silver, observed_at=window_start)
    elif table in ("forecast_points", "station_urgency"):
        rows = spec.transform(silver, batch_run_at=window_start)
    else:
        rows = spec.transform(silver)

    # 논리 spec 이름 → 물리 DB 테이블 이름 매핑
    # 서로 다른 여러 데이터 소스가 단일 Gold 테이블로 통합 적재되는 경우를 처리한다:
    # 1) 문화/공연 행사: cultural_event(문화행사)와 performance_event(공연행사) → cultural_events 테이블로 병합
    # 2) 날씨 예보: weather_short_term_forecast(단기)와 weather_ultra_short_forecast(초단기) → weather_forecast 테이블로 병합
    _TABLE_ALIASES = {
        "cultural_events_performance": "cultural_events",
        "weather_forecast_ultra": "weather_forecast",
    }
    target_table = _TABLE_ALIASES.get(table, table)

    with get_connection() as conn:
        if table == "station_urgency" and rows:
            station_ids = [row["sta_id"] for row in rows]
            with conn.cursor() as cur:
                cur.execute("SELECT sta_id FROM stations WHERE sta_id = ANY(%s)", (station_ids,))
                known_station_ids = {row[0] for row in cur.fetchall()}
            filtered_rows = _only_known_stations(rows, known_station_ids)
            excluded_count = len(rows) - len(filtered_rows)
            if excluded_count:
                print(f"excluded {excluded_count} urgency rows absent from stations")
            rows = filtered_rows
        if table == "rebalance_routes":
            _retire_stale_proposed_routes(conn)
        upsert(conn, target_table, rows, spec.conflict_cols, spec.update_cols, guard_col=spec.guard_col)
        conn.commit()

    print(f"upserted {len(rows)} rows into {target_table}")


def main() -> int:
    """CLI 인자를 파싱하고 테이블 적재 파이프라인을 실행한다 (성공 0, 실패 1)."""
    parser = argparse.ArgumentParser(description="Silver parquet을 읽어 Gold DB에 upsert한다.")
    parser.add_argument("--table", required=True, choices=sorted(TABLE_SPECS))
    parser.add_argument("--window-start", required=True, help="ISO8601 시각(KST), 예: 2026-08-16T14:05:00+09:00")
    args = parser.parse_args()

    window_start = datetime.fromisoformat(args.window_start)

    try:
        run(args.table, window_start)
    except Exception as exc:  # noqa: BLE001 - CLI 최상위: 실패를 종료 코드로 변환하기 위해 포괄 처리
        print(f"loader failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
