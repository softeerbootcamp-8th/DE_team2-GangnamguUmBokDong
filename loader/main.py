"""Silver 계층 데이터를 읽어 Gold DB에 Upsert하는 CLI 진입점 모듈."""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from core.db import get_connection
from core.upsert import upsert

from config import TABLE_SPECS, target_table_for
from retention_config import DATE_TYPED_EXPIRE_TABLES, grace_for

KST = ZoneInfo("Asia/Seoul")


def _expire_cutoff(target_table: str, window_start: datetime):
    """target_table의 만료 기준 시각(또는 날짜)을 계산한다. window_start에서
    유예기간(retention_config.RETENTION_GRACE)만큼 뺀 시점보다 오래된 행이 삭제 대상이다."""
    cutoff = window_start - grace_for(target_table)
    return cutoff.date() if target_table in DATE_TYPED_EXPIRE_TABLES else cutoff


def _delete_expired(conn, target_table: str, expire_col: str, cutoff) -> int:
    """target_table에서 expire_col < cutoff인 행을 지우고 지운 행 수를 반환한다."""
    with conn.cursor() as cur:
        cur.execute(f"DELETE FROM {target_table} WHERE {expire_col} < %(cutoff)s", {"cutoff": cutoff})
        return cur.rowcount


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
    elif table == "forecast_points":
        rows = spec.transform(silver, batch_run_at=window_start)
    else:
        rows = spec.transform(silver)

    target_table = target_table_for(table)

    with get_connection() as conn:
        upsert(conn, target_table, rows, spec.conflict_cols, spec.update_cols)
        if spec.expire_col:
            cutoff = _expire_cutoff(target_table, window_start)
            deleted = _delete_expired(conn, target_table, spec.expire_col, cutoff)
            print(f"deleted {deleted} expired rows from {target_table} (expire_col={spec.expire_col}, cutoff={cutoff})")
        conn.commit()

    print(f"upserted {len(rows)} rows into {target_table}")


def _parse_window_start(raw: str) -> datetime:
    """--window-start를 파싱한다. 오프셋이 없으면 KST로 간주해 채운다.

    naive datetime을 그대로 쓰면 DELETE 문의 cutoff가 psycopg를 거쳐 세션
    TimeZone(컨테이너 기본 UTC)으로 해석돼, KST로 의도한 시각보다 9시간 미래가
    기준이 된다. 그러면 아직 만료되지 않은 예보/예측 행까지 지워지므로
    (대시보드가 읽는 바로 그 행들) 여기서 오프셋을 반드시 확정한다."""
    parsed = datetime.fromisoformat(raw)
    if parsed.tzinfo is None:
        print(f"warning: --window-start에 오프셋이 없어 KST로 간주한다: {raw}", file=sys.stderr)
        return parsed.replace(tzinfo=KST)
    return parsed


def main() -> int:
    """CLI 인자를 파싱하고 테이블 적재 파이프라인을 실행한다 (성공 0, 실패 1)."""
    parser = argparse.ArgumentParser(description="Silver parquet을 읽어 Gold DB에 upsert한다.")
    parser.add_argument("--table", required=True, choices=sorted(TABLE_SPECS))
    parser.add_argument("--window-start", required=True, help="ISO8601 시각(KST), 예: 2026-08-16T14:05:00+09:00")
    args = parser.parse_args()

    window_start = _parse_window_start(args.window_start)

    try:
        run(args.table, window_start)
    except Exception as exc:  # noqa: BLE001 - CLI 최상위: 실패를 종료 코드로 변환하기 위해 포괄 처리
        print(f"loader failed: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
