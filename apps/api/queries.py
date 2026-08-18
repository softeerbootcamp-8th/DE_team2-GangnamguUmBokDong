from collections import defaultdict
from datetime import UTC, datetime, timedelta

from core.db import fetch_all, fetch_one

STOCK_HISTORY_WINDOW_MIN = 25


def _floor_to_5min(dt: datetime) -> datetime:
    """주어진 시각을 5분 단위로 내림한다."""
    return dt - timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)


def _group_by_sta_id(rows: list[dict]) -> dict[str, list[dict]]:
    """sta_id 컬럼 기준으로 행을 묶는다(그 컬럼은 결과 dict에서 빠진다)."""
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        sta_id = row.pop("sta_id")
        grouped[sta_id].append(row)
    return grouped


def fetch_stations() -> list[dict]:
    """대여소 마스터와 각 대여소의 최신 재고를 반환한다."""
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
    return fetch_all(query)


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
    return fetch_one(query, {"sta_id": sta_id})


def fetch_forecast_points(sta_id: str, now: datetime) -> list[dict]:
    """미래 구간을 가진 최신 배치 한 건의 예측만 시간순으로 반환한다."""
    query = """
        WITH latest_batch AS (
            SELECT max(batch_run_at) AS batch_run_at
            FROM forecast_points
            WHERE predicted_dttm > %(now)s
        )
        SELECT predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = %(sta_id)s
          AND predicted_dttm > %(now)s
          AND batch_run_at = (SELECT batch_run_at FROM latest_batch)
        ORDER BY predicted_dttm
    """
    return fetch_all(query, {"sta_id": sta_id, "now": now})


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
    rows = fetch_all(query, {"sta_ids": sta_ids, "since": since})
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_all_forecast_points(sta_ids: list[str], now: datetime) -> dict[str, list[dict]]:
    """여러 대여소의 예측 원본치를 쿼리 1번으로 가져와 sta_id별로 묶어서 반환한다."""
    query = """
        WITH latest_batch AS (
            SELECT max(batch_run_at) AS batch_run_at
            FROM forecast_points
            WHERE predicted_dttm > %(now)s
        )
        SELECT sta_id, predicted_dttm, predicted_rent_cnt, predicted_return_cnt
        FROM forecast_points
        WHERE sta_id = ANY(%(sta_ids)s)
          AND predicted_dttm > %(now)s
          AND batch_run_at = (SELECT batch_run_at FROM latest_batch)
        ORDER BY sta_id, predicted_dttm
    """
    rows = fetch_all(query, {"sta_ids": sta_ids, "now": now})
    grouped = _group_by_sta_id(rows)
    return {sta_id: grouped.get(sta_id, []) for sta_id in sta_ids}


def fetch_batch_run_at(now: datetime) -> datetime:
    """미래 예측이 있는 최신 배치 시각. 결과가 없으면 전체 최신값 또는 현재 시각."""
    row = fetch_one(
        """
        SELECT COALESCE(
            max(batch_run_at) FILTER (WHERE predicted_dttm > %(now)s),
            max(batch_run_at)
        ) AS latest
        FROM forecast_points
        """,
        {"now": now},
    )
    latest = row["latest"] if row else None
    return latest if latest is not None else _floor_to_5min(now)


def now_utc() -> datetime:
    """UTC 현재 시각(timezone-aware)을 반환한다."""
    return datetime.now(UTC)
