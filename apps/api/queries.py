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


def fetch_station(sta_id: int) -> dict | None:
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


def fetch_stock_history(sta_id: int, now: datetime) -> list[dict]:
    """최근 STOCK_HISTORY_WINDOW_MIN분간 재고 이력. 추세감지에 쓰인다."""
    query = """
        SELECT observed_at, parking_bike_tot_cnt
        FROM station_stock
        WHERE sta_id = %(sta_id)s AND observed_at >= %(since)s
        ORDER BY observed_at
    """
    since = now - timedelta(minutes=STOCK_HISTORY_WINDOW_MIN)
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute(query, {"sta_id": sta_id, "since": since})
        return _rows_as_dicts(cur)


def fetch_forecast_points(sta_id: int, now: datetime) -> list[dict]:
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


def fetch_batch_run_at(now: datetime) -> datetime:
    """가장 최근 예측 배치 실행 시각. 배치 결과가 아직 없으면 지금을 5분 단위로 내림해 대신한다."""
    with get_connection() as conn, conn.cursor() as cur:
        cur.execute("SELECT max(batch_run_at) FROM forecast_points")
        (latest,) = cur.fetchone()
    return latest if latest is not None else _floor_to_5min(now)


def now_utc() -> datetime:
    """UTC 현재 시각(timezone-aware)을 반환한다."""
    return datetime.now(UTC)
