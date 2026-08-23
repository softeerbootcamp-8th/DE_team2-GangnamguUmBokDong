"""Gold PostGIS serving schema를 조회하고 route 상태를 전이한다."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import StrEnum
from typing import Any
from uuid import UUID

from core.db import fetch_all, fetch_one, get_connection
from psycopg import Cursor
from psycopg.errors import CheckViolation
from psycopg.rows import dict_row

STOCK_FRESHNESS = timedelta(minutes=10)
DEMAND_FRESHNESS = timedelta(minutes=10)
ULTRA_SHORT_FRESHNESS = timedelta(hours=2)
SHORT_TERM_FRESHNESS = timedelta(hours=4)
EVENT_FRESHNESS = timedelta(hours=36)
FUTURE_TOLERANCE = timedelta(minutes=5)
FORECAST_HOUR_COUNT = 12
NEARBY_EVENT_RADIUS_KM = 1.5
# 날씨 freshness는 DB 기록 시각이 아니라 기상청 발표 시각(base_dttm)으로 판정한다.
# 게시 파이프라인은 값이 바뀐 행만 다시 쓰므로 DB 기록 시각은 발표 주기보다 오래
# 멈춰 있을 수 있다. 임계값은 각 제품의 발표 주기(초단기 1시간, 단기 3시간)에
# 한 주기만큼의 여유를 더한 값이다.
WEATHER_FRESHNESS_BY_PRODUCT = {
    "ultra_short": ULTRA_SHORT_FRESHNESS,
    "short_term": SHORT_TERM_FRESHNESS,
}
_WEATHER_LINEAGE_KEYS = frozenset({"base_dttm", "source_product_cd"})


class ForecastState(StrEnum):
    """수요예측 API가 응답할 수 있는 상태를 나타낸다."""

    READY = "ready"
    STATION_NOT_FOUND = "station_not_found"
    FORECAST_NOT_AVAILABLE = "forecast_not_available"
    FORECAST_NOT_READY = "forecast_not_ready"
    STOCK_NOT_ALIGNED = "stock_forecast_not_aligned"


@dataclass(frozen=True)
class ForecastResult:
    """수요예측 조회 결과와 실패 원인을 함께 보관한다."""

    state: ForecastState
    station: dict[str, Any] | None = None
    base_dttm: datetime | None = None
    points: tuple[dict[str, Any], ...] = ()


class WeatherState(StrEnum):
    """시간별 날씨 API가 응답할 수 있는 상태를 나타낸다."""

    READY = "ready"
    STATION_NOT_FOUND = "station_not_found"
    WEATHER_NOT_READY = "weather_not_ready"


@dataclass(frozen=True)
class WeatherResult:
    """시간별 날씨 조회 결과와 실패 원인을 함께 보관한다."""

    state: WeatherState
    points: tuple[dict[str, Any], ...] = ()


class RouteTransitionResult(StrEnum):
    """route 상태 전이 결과를 API와 독립적인 값으로 나타낸다."""

    NOT_FOUND = "not_found"
    WRONG_STATUS = "wrong_status"
    CONSTRAINT_CONFLICT = "constraint_conflict"
    STATION_CONFLICT = "station_conflict"


def now_utc() -> datetime:
    """UTC 현재 시각을 timezone-aware 값으로 반환한다."""
    return datetime.now(UTC)


def _is_fresh(value: datetime | None, now: datetime, max_age: timedelta) -> bool:
    """시각이 과거 freshness와 5분 미래 허용 범위 안인지 확인한다."""
    return value is not None and now - max_age <= value <= now + FUTURE_TOLERANCE


def _start_read_snapshot(cursor: Cursor[dict[str, Any]]) -> None:
    """여러 SELECT가 하나의 읽기 snapshot을 사용하도록 transaction을 설정한다."""
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")


def fetch_stations(now: datetime) -> list[dict[str, Any]]:
    """활성 대여소와 같은 anchor의 신선한 최신 재고를 반환한다."""
    return fetch_all(
        """
        SELECT s.sta_id,
               s.sta_nm,
               ST_Y(s.sta_point) AS lat,
               ST_X(s.sta_point) AS lon,
               s.hold_cnt,
               stock.parking_bike_tot_cnt,
               stock.base_dttm,
               center.dispatch_center_nm AS region
          FROM station AS s
          JOIN station_stock AS stock
            ON stock.sta_id = s.sta_id
           AND stock.base_dttm = s.last_seen_dttm
          JOIN dispatch_center AS center
            ON center.dispatch_center_id = s.dispatch_center_id
           AND center.is_active
         WHERE s.is_active
           AND stock.base_dttm BETWEEN %(now)s - INTERVAL '10 minutes'
                                   AND %(now)s + INTERVAL '5 minutes'
         ORDER BY s.sta_id
        """,
        {"now": now},
    )


def fetch_station(sta_id: str, now: datetime) -> dict[str, Any] | None:
    """활성 대여소 하나와 같은 anchor의 신선한 최신 재고를 반환한다."""
    return fetch_one(
        """
        SELECT s.sta_id,
               s.sta_nm,
               s.sta_addr,
               ST_Y(s.sta_point) AS lat,
               ST_X(s.sta_point) AS lon,
               s.hold_cnt,
               stock.parking_bike_tot_cnt,
               stock.base_dttm,
               center.dispatch_center_nm AS region
          FROM station AS s
          JOIN station_stock AS stock
            ON stock.sta_id = s.sta_id
           AND stock.base_dttm = s.last_seen_dttm
          JOIN dispatch_center AS center
            ON center.dispatch_center_id = s.dispatch_center_id
           AND center.is_active
         WHERE s.sta_id = %(sta_id)s
           AND s.is_active
           AND stock.base_dttm BETWEEN %(now)s - INTERVAL '10 minutes'
                                   AND %(now)s + INTERVAL '5 minutes'
        """,
        {"sta_id": sta_id, "now": now},
    )


def _read_forecast_snapshot(
    sta_id: str,
) -> tuple[
    dict[str, Any] | None,
    dict[str, Any],
    list[dict[str, Any]],
    dict[str, Any] | None,
]:
    """대여소·수요 projection·재고를 하나의 DB snapshot에서 읽는다."""
    with (
        get_connection() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        _start_read_snapshot(cursor)
        cursor.execute(
            """
            SELECT sta_id, hold_cnt, last_seen_dttm
              FROM station
             WHERE sta_id = %(sta_id)s
               AND is_active
            """,
            {"sta_id": sta_id},
        )
        station = cursor.fetchone()
        cursor.execute(
            """
            SELECT count(*) AS row_cnt,
                   min(base_dttm) AS min_base_dttm,
                   max(base_dttm) AS max_base_dttm
              FROM station_demand_forecast
            """
        )
        demand_summary = cursor.fetchone()
        assert demand_summary is not None
        cursor.execute(
            """
            SELECT base_dttm,
                   predicted_dttm,
                   predicted_rent_cnt,
                   predicted_rtn_cnt AS predicted_return_cnt
              FROM station_demand_forecast
             WHERE sta_id = %(sta_id)s
             ORDER BY predicted_dttm
            """,
            {"sta_id": sta_id},
        )
        points = cursor.fetchall()
        cursor.execute(
            """
            SELECT sta_id, base_dttm, parking_bike_tot_cnt
              FROM station_stock
             WHERE sta_id = %(sta_id)s
            """,
            {"sta_id": sta_id},
        )
        stock = cursor.fetchone()
    return station, demand_summary, points, stock


def fetch_forecast(sta_id: str, now: datetime) -> ForecastResult:
    """대여소의 정렬된 미래 12시간 수요예측과 같은 anchor 재고를 판정한다."""
    station, summary, points, stock = _read_forecast_snapshot(sta_id)
    if station is None:
        return ForecastResult(ForecastState.STATION_NOT_FOUND)

    common_base = summary["min_base_dttm"]
    if (
        summary["row_cnt"] == 0
        or common_base != summary["max_base_dttm"]
        or not _is_fresh(common_base, now, DEMAND_FRESHNESS)
    ):
        return ForecastResult(ForecastState.FORECAST_NOT_READY)

    if not points:
        return ForecastResult(ForecastState.FORECAST_NOT_AVAILABLE)

    expected_targets = [
        common_base + timedelta(hours=hour)
        for hour in range(1, FORECAST_HOUR_COUNT + 1)
    ]
    actual_targets = [point["predicted_dttm"] for point in points]
    if (
        len(points) != FORECAST_HOUR_COUNT
        or any(point["base_dttm"] != common_base for point in points)
        or actual_targets != expected_targets
        or any(target <= now for target in actual_targets)
    ):
        return ForecastResult(ForecastState.FORECAST_NOT_READY)

    if (
        stock is None
        or stock["base_dttm"] != common_base
        or station["last_seen_dttm"] != stock["base_dttm"]
        or not _is_fresh(stock["base_dttm"], now, STOCK_FRESHNESS)
    ):
        return ForecastResult(ForecastState.STOCK_NOT_ALIGNED)

    response_station = {
        **station,
        "parking_bike_tot_cnt": stock["parking_bike_tot_cnt"],
    }
    response_points = tuple(
        {
            "predicted_dttm": point["predicted_dttm"],
            "predicted_rent_cnt": point["predicted_rent_cnt"],
            "predicted_return_cnt": point["predicted_return_cnt"],
        }
        for point in points
    )
    return ForecastResult(
        ForecastState.READY,
        response_station,
        common_base,
        response_points,
    )


def fetch_status_base_dttm(now: datetime) -> datetime | None:
    """fresh하고 전체 행에 공통인 실제 demand base를 반환한다."""
    summary = fetch_one(
        """
        SELECT count(*) AS row_cnt,
               min(base_dttm) AS min_base_dttm,
               max(base_dttm) AS max_base_dttm
          FROM station_demand_forecast
        """
    )
    if summary is None or summary["row_cnt"] == 0:
        return None
    base_dttm = summary["min_base_dttm"]
    if base_dttm != summary["max_base_dttm"] or not _is_fresh(
        base_dttm,
        now,
        DEMAND_FRESHNESS,
    ):
        return None
    return base_dttm


def fetch_nearby_events(
    sta_id: str,
    now: datetime,
    radius_km: float = NEARBY_EVENT_RADIUS_KM,
) -> list[dict[str, Any]] | None:
    """활성 station 주변의 신선한 현재·예정 행사를 PostGIS 거리순으로 반환한다.

    활성 station이 없으면 None을 반환하고, station은 있지만 행사가 없으면 빈 목록을
    반환해 API가 404와 정상 EMPTY를 구분할 수 있게 한다.
    """
    with (
        get_connection() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        _start_read_snapshot(cursor)
        cursor.execute(
            """
            SELECT 1
              FROM station
             WHERE sta_id = %(sta_id)s
               AND is_active
            """,
            {"sta_id": sta_id},
        )
        if cursor.fetchone() is None:
            return None
        cursor.execute(
            """
            SELECT e.event_id,
                   e.event_name AS title,
                   e.event_spot_nm AS place,
                   e.event_start_dt AS start_date,
                   e.event_end_dt AS end_date,
                   ST_Y(e.event_point) AS lat,
                   ST_X(e.event_point) AS lon,
                   ST_Distance(
                       e.event_point::geography,
                       s.sta_point::geography
                   ) / 1000.0 AS distance_km
              FROM station AS s
              JOIN event AS e
                ON ST_DWithin(
                       e.event_point::geography,
                       s.sta_point::geography,
                       %(radius_m)s
                   )
             WHERE s.sta_id = %(sta_id)s
               AND s.is_active
               AND e.event_end_dt >= (%(now)s AT TIME ZONE 'Asia/Seoul')::date
               AND e.last_seen_dttm BETWEEN %(now)s - INTERVAL '36 hours'
                                            AND %(now)s + INTERVAL '5 minutes'
             ORDER BY distance_km, e.event_id
            """,
            {"sta_id": sta_id, "now": now, "radius_m": radius_km * 1000.0},
        )
        events = cursor.fetchall()
    return [
        {**event, "distance_km": round(event["distance_km"], 2)} for event in events
    ]


def _read_weather_snapshot(
    sta_id: str,
    now: datetime,
    hours: int,
) -> tuple[bool, list[dict[str, Any]]]:
    """대여소 존재와 날씨 horizon을 하나의 DB snapshot에서 읽는다."""
    with (
        get_connection() as connection,
        connection.cursor(row_factory=dict_row) as cursor,
    ):
        _start_read_snapshot(cursor)
        cursor.execute(
            """
            SELECT 1
              FROM station
             WHERE sta_id = %(sta_id)s
               AND is_active
            """,
            {"sta_id": sta_id},
        )
        if cursor.fetchone() is None:
            return False, []
        cursor.execute(
            """
            WITH horizon AS (
                SELECT date_trunc('hour', %(now)s::TIMESTAMPTZ) + INTERVAL '1 hour'
                           AS first_forecast_dttm
            )
            SELECT wf.forecast_dttm,
                   wf.temperature,
                   wf.sky_condition_cd,
                   wf.precipitation_type_cd,
                   wf.precipitation_prob,
                   wf.precipitation_amount,
                   wf.humidity,
                   wf.wind_speed,
                   wf.source_product_cd,
                   wf.base_dttm
              FROM station AS s
              JOIN weather_forecast AS wf USING (weather_grid_id)
             CROSS JOIN horizon AS h
             WHERE s.sta_id = %(sta_id)s
               AND s.is_active
               AND wf.forecast_dttm >= h.first_forecast_dttm
               AND wf.forecast_dttm < h.first_forecast_dttm
                                          + %(hours)s * INTERVAL '1 hour'
             ORDER BY wf.forecast_dttm
            """,
            {"sta_id": sta_id, "now": now, "hours": hours},
        )
        return True, cursor.fetchall()


def fetch_weather(
    sta_id: str,
    now: datetime,
    hours: int = FORECAST_HOUR_COUNT,
) -> WeatherResult:
    """활성 대여소의 fresh하고 완전한 미래 정시 날씨를 반환한다."""
    station_exists, rows = _read_weather_snapshot(sta_id, now, hours)
    if not station_exists:
        return WeatherResult(WeatherState.STATION_NOT_FOUND)

    first_target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    expected_targets = [
        first_target + timedelta(hours=offset) for offset in range(hours)
    ]
    if (
        len(rows) != hours
        or [row["forecast_dttm"] for row in rows] != expected_targets
        or any(not _is_forecast_issue_fresh(row, now) for row in rows)
    ):
        return WeatherResult(WeatherState.WEATHER_NOT_READY)

    points = tuple(
        {key: value for key, value in row.items() if key not in _WEATHER_LINEAGE_KEYS}
        for row in rows
    )
    return WeatherResult(WeatherState.READY, points)


def _is_forecast_issue_fresh(row: dict[str, Any], now: datetime) -> bool:
    """예보 행의 발표 시각이 그 제품의 허용 age 안인지 확인한다."""
    max_age = WEATHER_FRESHNESS_BY_PRODUCT.get(row["source_product_cd"])
    if max_age is None:
        return False
    return _is_fresh(row["base_dttm"], now, max_age)


def fetch_regions() -> list[dict[str, Any]]:
    """활성 배차 센터의 이름과 Point 파생 좌표를 반환한다."""
    return fetch_all(
        """
        SELECT dispatch_center_nm AS region,
               ST_Y(dispatch_center_point) AS lat,
               ST_X(dispatch_center_point) AS lon
          FROM dispatch_center
         WHERE is_active
         ORDER BY dispatch_center_id
        """
    )


def fetch_alerts(now: datetime) -> list[dict[str, Any]]:
    """같은 fresh anchor와 최신 correction을 만족하는 긴급도만 반환한다."""
    return fetch_all(
        """
        SELECT s.sta_id,
               s.sta_nm,
               urgency.rebalance_need_type_cd AS action_type,
               urgency.urgency_score,
               urgency.critical_remaining_min AS minutes_until_critical,
               center.dispatch_center_nm AS region
          FROM station_urgency AS urgency
          JOIN station_stock AS stock
            ON stock.sta_id = urgency.sta_id
           AND stock.base_dttm = urgency.base_dttm
          JOIN station AS s
            ON s.sta_id = urgency.sta_id
           AND s.is_active
           AND s.last_seen_dttm = stock.base_dttm
          JOIN dispatch_center AS center
            ON center.dispatch_center_id = s.dispatch_center_id
           AND center.is_active
         WHERE urgency.base_dttm BETWEEN %(now)s - INTERVAL '10 minutes'
                                       AND %(now)s + INTERVAL '5 minutes'
           AND stock.base_dttm BETWEEN %(now)s - INTERVAL '10 minutes'
                                     AND %(now)s + INTERVAL '5 minutes'
           AND urgency.updated_dttm >= stock.updated_dttm
           AND urgency.updated_dttm >= s.updated_dttm
         ORDER BY urgency.urgency_score DESC, s.sta_id ASC
        """,
        {"now": now},
    )


def _route_aggregate_query(where_clause: str, page_clause: str = "") -> str:
    """route header와 stop을 한 statement에서 읽는 SQL을 만든다."""
    return f"""
        WITH route_page AS MATERIALIZED (
            SELECT route.route_id,
                   center.dispatch_center_nm AS region,
                   route.route_status_cd AS status,
                   route.proposed_dttm AS proposed_at,
                   route.dispatched_dttm AS dispatched_at,
                   route.completed_dttm AS completed_at,
                   route.cancelled_dttm AS cancelled_at
              FROM rebalance_route AS route
              JOIN dispatch_center AS center USING (dispatch_center_id)
             {where_clause}
             ORDER BY route.proposed_dttm DESC, route.route_id ASC
             {page_clause}
        )
        SELECT page.route_id::text AS route_id,
               page.region,
               page.status,
               page.proposed_at,
               page.dispatched_at,
               page.completed_at,
               page.cancelled_at,
               COALESCE(stops.items, '[]'::jsonb) AS stops
          FROM route_page AS page
          LEFT JOIN LATERAL (
              SELECT jsonb_agg(
                         jsonb_build_object(
                             'visit_order', stop.visit_no,
                             'sta_id', stop.sta_id,
                             'sta_nm', station.sta_nm,
                             'lat', ST_Y(station.sta_point),
                             'lon', ST_X(station.sta_point),
                             'action', stop.route_action_type_cd,
                             'bike_cnt', stop.bike_cnt
                         )
                         ORDER BY stop.visit_no
                     ) AS items
                FROM rebalance_route_stop AS stop
                JOIN station USING (sta_id)
               WHERE stop.route_id = page.route_id
          ) AS stops ON true
         ORDER BY page.proposed_at DESC, page.route_id ASC
    """


def fetch_routes(
    region: str | None = None,
    status: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[dict[str, Any]]:
    """bounded filter와 결정적 순서로 route aggregate 목록을 반환한다."""
    conditions: list[str] = []
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if region is not None:
        conditions.append("center.dispatch_center_nm = %(region)s")
        params["region"] = region
    if status is not None:
        conditions.append("route.route_status_cd = %(status)s")
        params["status"] = status
    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    query = _route_aggregate_query(
        where_clause,
        "LIMIT %(limit)s OFFSET %(offset)s",
    )
    return fetch_all(query, params)


def _fetch_route_with_cursor(
    cursor: Cursor[dict[str, Any]],
    route_id: UUID,
) -> dict[str, Any] | None:
    """현재 transaction에서 route aggregate 하나를 조회한다."""
    cursor.execute(
        _route_aggregate_query("WHERE route.route_id = %(route_id)s"),
        {"route_id": route_id},
    )
    return cursor.fetchone()


def fetch_route(route_id: UUID) -> dict[str, Any] | None:
    """route header와 모든 stop을 한 SQL snapshot으로 반환한다."""
    return fetch_one(
        _route_aggregate_query("WHERE route.route_id = %(route_id)s"),
        {"route_id": route_id},
    )


_STATION_CONFLICT_SQL = """
SELECT count(DISTINCT candidate_stop.sta_id) AS conflict_cnt
  FROM rebalance_route_stop AS candidate_stop
  JOIN rebalance_route_stop AS active_stop USING (sta_id)
  JOIN rebalance_route AS active_route
       ON active_route.route_id = active_stop.route_id
 WHERE candidate_stop.route_id = %(route_id)s
   AND active_route.route_id <> %(route_id)s
   AND active_route.route_status_cd = 'dispatched'
"""


def _station_conflict_guard(
    cursor: Cursor[dict[str, Any]],
    route_id: UUID,
) -> RouteTransitionResult | None:
    """진행 중 경로와 대여소가 겹치면 STATION_CONFLICT를 반환한다.

    같은 대여소에 트럭 두 대가 배정되는 것을 막는다. 자기 경로는 이미 dispatched로
    바뀐 뒤라 제외한다.
    """
    cursor.execute(_STATION_CONFLICT_SQL, {"route_id": route_id})
    row = cursor.fetchone()
    if row is not None and row["conflict_cnt"] > 0:
        return RouteTransitionResult.STATION_CONFLICT
    return None


def _transition_route(
    route_id: UUID,
    now: datetime,
    *,
    expected_status: str,
    next_status: str,
    timestamp_column: str,
    guard: Callable[[Cursor[dict[str, Any]], UUID], RouteTransitionResult | None]
    | None = None,
) -> dict[str, Any] | RouteTransitionResult:
    """guarded update와 aggregate 재조회를 같은 transaction에서 수행한다.

    guard는 UPDATE 뒤에 실행한다. rebalance_route 쓰기가 statement trigger로
    route operation advisory lock을 잡으므로, UPDATE 전에 읽으면 lock 없이 읽어
    동시 전이가 서로를 놓칠 수 있다.
    """
    try:
        with (
            get_connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                f"""
                UPDATE rebalance_route
                   SET route_status_cd = %(next_status)s,
                       {timestamp_column} = %(now)s
                 WHERE route_id = %(route_id)s
                   AND route_status_cd = %(expected_status)s
                RETURNING route_id
                """,
                {
                    "route_id": route_id,
                    "now": now,
                    "expected_status": expected_status,
                    "next_status": next_status,
                },
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    "SELECT route_status_cd FROM rebalance_route WHERE route_id = %(route_id)s",
                    {"route_id": route_id},
                )
                if cursor.fetchone() is None:
                    return RouteTransitionResult.NOT_FOUND
                return RouteTransitionResult.WRONG_STATUS
            if guard is not None:
                blocked = guard(cursor, route_id)
                if blocked is not None:
                    connection.rollback()
                    return blocked
            route = _fetch_route_with_cursor(cursor, route_id)
            if route is None:
                raise RuntimeError("상태 전이 직후 route aggregate를 찾을 수 없습니다.")
            return route
    except CheckViolation:
        return RouteTransitionResult.CONSTRAINT_CONFLICT


def dispatch_route(
    route_id: UUID,
    now: datetime,
) -> dict[str, Any] | RouteTransitionResult:
    """route를 proposed에서 dispatched로 원자 전이한다."""
    return _transition_route(
        route_id,
        now,
        expected_status="proposed",
        next_status="dispatched",
        timestamp_column="dispatched_dttm",
        guard=_station_conflict_guard,
    )


def complete_route(
    route_id: UUID,
    now: datetime,
) -> dict[str, Any] | RouteTransitionResult:
    """route를 dispatched에서 completed로 원자 전이한다."""
    return _transition_route(
        route_id,
        now,
        expected_status="dispatched",
        next_status="completed",
        timestamp_column="completed_dttm",
    )


def cancel_route(
    route_id: UUID,
    now: datetime,
) -> dict[str, Any] | RouteTransitionResult:
    """route를 dispatched에서 cancelled로 원자 전이한다."""
    return _transition_route(
        route_id,
        now,
        expected_status="dispatched",
        next_status="cancelled",
        timestamp_column="cancelled_dttm",
    )
