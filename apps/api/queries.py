from collections import defaultdict
from datetime import UTC, datetime, timedelta

from core.db import execute, fetch_all, fetch_one

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
    """최신 미래예측 배치 대상 대여소의 마스터 + 최신 재고를 반환한다."""
    query = """
        WITH latest_batch AS (
            SELECT max(batch_run_at) AS batch_run_at
            FROM forecast_points
            WHERE predicted_dttm > now()
        ), forecasted_stations AS (
            SELECT DISTINCT sta_id
            FROM forecast_points
            WHERE batch_run_at = (SELECT batch_run_at FROM latest_batch)
              AND predicted_dttm > now()
        )
        SELECT s.sta_id, s.sta_nm, s.gu, s.sta_addr, s.lat, s.lon, s.hold_cnt,
               stock.parking_bike_tot_cnt, stock.observed_at AS base_dttm
        FROM stations s
        JOIN forecasted_stations forecasted ON forecasted.sta_id = s.sta_id
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


def fetch_alerts() -> list[dict]:
    """전체 대여소의 재배치 우선순위 알림을 station_urgency(배치가 미리 계산한
    결과)에서 urgency_score 내림차순으로 조회한다. region은 위경도가 있어야
    계산되므로 stations와 조인해서 같이 가져온다."""
    query = """
        SELECT s.sta_id, s.sta_nm, s.lat, s.lon,
               u.action_type, u.urgency_score, u.minutes_until_critical
        FROM station_urgency u
        JOIN stations s ON s.sta_id = u.sta_id
        ORDER BY u.urgency_score DESC
    """
    return fetch_all(query)


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


def _fetch_stops_for_routes(route_ids: list[str]) -> dict[str, list[dict]]:
    """여러 라우트의 스톱을 쿼리 1번으로 가져와 route_id별로 묶어서 반환한다
    (fetch_all_stock_history와 같은 N+1 방지 패턴). stations와 조인해 지도 렌더링에
    필요한 sta_nm/lat/lon도 같이 준다."""
    if not route_ids:
        return {}
    query = """
        SELECT s.route_id, s.visit_order, s.sta_id, st.sta_nm, st.lat, st.lon, s.action, s.bike_cnt
        FROM rebalance_route_stops s
        JOIN stations st ON st.sta_id = s.sta_id
        WHERE s.route_id = ANY(%(route_ids)s)
        ORDER BY s.route_id, s.visit_order
    """
    rows = fetch_all(query, {"route_ids": route_ids})
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in rows:
        grouped[row.pop("route_id")].append(row)
    return dict(grouped)


def fetch_routes(region: str | None = None, status: str | None = None) -> list[dict]:
    """재배치 라우트 목록을 스톱과 함께 조회한다. region/status로 선택적으로 필터링한다."""
    conditions: list[str] = []
    params: dict = {}
    if region is not None:
        conditions.append("region = %(region)s")
        params["region"] = region
    if status is not None:
        conditions.append("status = %(status)s")
        params["status"] = status
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    query = f"""
        SELECT route_id, region, status, proposed_at, dispatched_at, completed_at
        FROM rebalance_routes
        {where}
        ORDER BY proposed_at DESC
    """
    routes = fetch_all(query, params)
    stops_by_route = _fetch_stops_for_routes([route["route_id"] for route in routes])
    for route in routes:
        route["stops"] = stops_by_route.get(route["route_id"], [])
    return routes


def fetch_route(route_id: str) -> dict | None:
    """라우트 하나를 스톱과 함께 조회한다. 없으면 None."""
    query = """
        SELECT route_id, region, status, proposed_at, dispatched_at, completed_at
        FROM rebalance_routes
        WHERE route_id = %(route_id)s
    """
    route = fetch_one(query, {"route_id": route_id})
    if route is None:
        return None
    route["stops"] = _fetch_stops_for_routes([route_id]).get(route_id, [])
    return route


def dispatch_route(route_id: str, now: datetime) -> str:
    """proposed 상태인 라우트를 dispatched로 전이한다. 상태 체크를 UPDATE의 WHERE절에
    넣어 원자적으로 처리한다(별도로 조회한 뒤 갱신하면 그 사이 레이스가 생길 수 있음).

    returns: "dispatched"(성공) | "not_found"(라우트 없음) | "wrong_status"(proposed가 아님)
    """
    updated = execute(
        """
        UPDATE rebalance_routes
        SET status = 'dispatched', dispatched_at = %(now)s
        WHERE route_id = %(route_id)s AND status = 'proposed'
        """,
        {"route_id": route_id, "now": now},
    )
    if updated:
        return "dispatched"
    return "not_found" if fetch_route(route_id) is None else "wrong_status"


def complete_route(route_id: str, now: datetime) -> str:
    """dispatched 상태인 라우트를 completed로 전이한다. dispatch_route와 동일한 패턴.

    returns: "completed"(성공) | "not_found"(라우트 없음) | "wrong_status"(dispatched가 아님)
    """
    updated = execute(
        """
        UPDATE rebalance_routes
        SET status = 'completed', completed_at = %(now)s
        WHERE route_id = %(route_id)s AND status = 'dispatched'
        """,
        {"route_id": route_id, "now": now},
    )
    if updated:
        return "completed"
    return "not_found" if fetch_route(route_id) is None else "wrong_status"
