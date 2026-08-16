from collections import defaultdict
from datetime import UTC, datetime, timedelta

from core.db import get_connection

STOCK_HISTORY_WINDOW_MIN = 25


def _floor_to_5min(dt: datetime) -> datetime:
    """주어진 시각을 5분 단위로 내림한다."""
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)


def _rows_as_dicts(cur) -> list[dict]:
    """psycopg 커서의 조회 결과를 컬럼명 기준 dict 리스트로 변환한다."""
    columns = [col.name for col in cur.description]
    return [dict(zip(columns, row)) for row in cur.fetchall()]


def _group_by_sta_id(rows: list[dict]) -> dict[str, list[dict]]:
    """sta_id 컬럼 기준으로 행을 묶는다(그 컬럼은 결과 dict에서 빠진다)."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        sta_id = row.pop("sta_id")
        grouped[sta_id].append(row)
    return grouped


def fetch_stations() -> list[dict]:
    """대여소 마스터 + 최신 재고 한 줄씩을 합쳐서 전체 목록을 반환한다."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.gu, s.sta_addr, s.lat, s.lon, s.hold_cnt,
               stock.parking_bike_tot_cnt, stock.observed_at AS base_dttm
        FROM stations s
        JOIN LATERAL (
            SELECT parking_bike_tot_cnt, observed_at
            FROM station_stock
            WHERE station_stock.sta_id = s.sta_id
            ORDER BY observed_at DESC
            LIMIT 1
        ) stock ON true
        ORDER BY s.sta_id
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query)
        return _rows_as_dicts(cur)


def fetch_station(sta_id: str) -> dict | None:
    """대여소 하나의 마스터 + 최신 재고를 반환한다. 없으면 None."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.gu, s.sta_addr, s.lat, s.lon, s.hold_cnt,
               stock.parking_bike_tot_cnt, stock.observed_at AS base_dttm
        FROM stations s
        JOIN LATERAL (
            SELECT parking_bike_tot_cnt, observed_at
            FROM station_stock
            WHERE station_stock.sta_id = s.sta_id
            ORDER BY observed_at DESC
            LIMIT 1
        ) stock ON true
        WHERE s.sta_id = %(sta_id)s
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, {"sta_id": sta_id})
        rows = _rows_as_dicts(cur)
        return rows[0] if rows else None


def fetch_forecast_points(sta_id: str, now: datetime) -> list[dict]:
    """now 이후 시점의 예측 원본치(대여·반납량)를 시간순으로 반환한다."""
    query = """
        SELECT predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = %(sta_id)s AND predicted_dttm > %(now)s
        ORDER BY predicted_dttm
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, {"sta_id": sta_id, "now": now})
        return _rows_as_dicts(cur)


def fetch_all_stock_history(sta_ids: list[str], now: datetime) -> dict[str, list[dict]]:
    """여러 대여소의 최근 재고 이력을 대여소당 1번이 아니라 쿼리 1번으로 가져와
    sta_id별로 묶어서 반환한다(/alerts처럼 전체 대여소를 훑는 경우 N+1을 피하려고)."""
    query = """
        SELECT sta_id, observed_at, parking_bike_tot_cnt
        FROM station_stock
        WHERE sta_id = ANY(%(sta_ids)s) AND observed_at >= %(since)s
        ORDER BY sta_id, observed_at
    """
    since = now - timedelta(minutes=STOCK_HISTORY_WINDOW_MIN)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, {"sta_ids": sta_ids, "since": since})
        rows = _rows_as_dicts(cur)
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_all_forecast_points(sta_ids: list[str], now: datetime) -> dict[str, list[dict]]:
    """여러 대여소의 예측 원본치를 쿼리 1번으로 가져와 sta_id별로 묶어서 반환한다."""
    query = """
        SELECT sta_id, predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = ANY(%(sta_ids)s) AND predicted_dttm > %(now)s
        ORDER BY sta_id, predicted_dttm
    """
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, {"sta_ids": sta_ids, "now": now})
        rows = _rows_as_dicts(cur)
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_batch_run_at(now: datetime) -> datetime:
    """가장 최근 예측 배치 실행 시각. 배치 결과가 아직 없으면 지금을 5분 단위로 내림해 대신한다."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(batch_run_at) FROM forecast_points")
        (latest,) = cur.fetchone()
    return latest if latest is not None else _floor_to_5min(now)


def now_utc() -> datetime:
    """UTC 현재 시각(timezone-aware)을 반환한다."""
    return datetime.now(UTC)
