"""Gold PostGIS serving schema를 조회하고 route 상태를 전이한다."""

from __future__ import annotations

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
URGENCY_FRESHNESS = timedelta(minutes=10)
URGENCY_EXPIRY = timedelta(minutes=60)
ULTRA_SHORT_FRESHNESS = timedelta(hours=2)
SHORT_TERM_FRESHNESS = timedelta(hours=4)
EVENT_FRESHNESS = timedelta(hours=36)
SERVING_EXPIRY = timedelta(minutes=60)
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
    ALREADY_DISMISSED = "already_dismissed"
    SERVING_NOT_READY = "serving_not_ready"


def now_utc() -> datetime:
    """UTC 현재 시각을 timezone-aware 값으로 반환한다."""
    return datetime.now(UTC)


def _is_fresh(value: datetime | None, now: datetime, max_age: timedelta) -> bool:
    """시각이 과거 freshness와 5분 미래 허용 범위 안인지 확인한다."""
    return value is not None and now - max_age <= value <= now + FUTURE_TOLERANCE


def _start_read_snapshot(cursor: Cursor[dict[str, Any]]) -> None:
    """여러 SELECT가 하나의 읽기 snapshot을 사용하도록 transaction을 설정한다."""
    cursor.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")


def fetch_stations(now: datetime, *, allow_stale: bool = False) -> list[dict[str, Any]]:
    """활성 대여소와 같은 anchor의 최신 재고를 freshness 정책에 맞춰 반환한다."""
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
           AND stock.base_dttm <= %(now)s + INTERVAL '5 minutes'
           AND (
               %(allow_stale)s
               OR stock.base_dttm >= %(now)s - INTERVAL '10 minutes'
           )
         ORDER BY s.sta_id
        """,
        {"now": now, "allow_stale": allow_stale},
    )


def fetch_station(
    sta_id: str,
    now: datetime,
    *,
    allow_stale: bool = False,
) -> dict[str, Any] | None:
    """활성 대여소 하나와 같은 anchor의 최신 재고를 freshness 정책에 맞춰 반환한다."""
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
           AND stock.base_dttm <= %(now)s + INTERVAL '5 minutes'
           AND (
               %(allow_stale)s
               OR stock.base_dttm >= %(now)s - INTERVAL '10 minutes'
           )
        """,
        {"sta_id": sta_id, "now": now, "allow_stale": allow_stale},
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


def fetch_forecast(
    sta_id: str,
    now: datetime,
    *,
    allow_stale: bool = False,
) -> ForecastResult:
    """대여소의 완전한 12시간 수요예측과 같은 anchor 재고를 판정한다."""
    station, summary, points, stock = _read_forecast_snapshot(sta_id)
    if station is None:
        return ForecastResult(ForecastState.STATION_NOT_FOUND)

    common_base = summary["min_base_dttm"]
    if (
        summary["row_cnt"] == 0
        or common_base != summary["max_base_dttm"]
        or common_base > now + FUTURE_TOLERANCE
        or (not allow_stale and not _is_fresh(common_base, now, DEMAND_FRESHNESS))
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
        or (not allow_stale and any(target <= now for target in actual_targets))
    ):
        return ForecastResult(ForecastState.FORECAST_NOT_READY)

    if (
        stock is None
        or stock["base_dttm"] != common_base
        or station["last_seen_dttm"] != stock["base_dttm"]
        or (
            not allow_stale
            and not _is_fresh(stock["base_dttm"], now, STOCK_FRESHNESS)
        )
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


def _publication_state(
    row: dict[str, Any] | None,
    now: datetime,
    *,
    freshness: timedelta,
    expiry: timedelta | None = SERVING_EXPIRY,
) -> dict[str, Any]:
    """publication row를 프런트 공통 상태로 변환한다."""
    if row is None or row.get("logical_dttm") is None:
        return {
            "state": "missing",
            "data_dttm": None,
            "age_minutes": None,
            "reason": "not_published",
        }
    logical_dttm = row["logical_dttm"]
    age_minutes = max(0.0, (now - logical_dttm).total_seconds() / 60.0)
    if _is_fresh(logical_dttm, now, freshness):
        state = "ready"
        reason = "fresh"
    elif expiry is not None and now - logical_dttm > expiry:
        state = "expired"
        reason = "publication_expired"
    else:
        state = "stale"
        reason = "publication_stale"
    return {
        "state": state,
        "data_dttm": logical_dttm,
        "age_minutes": round(age_minutes, 1),
        "reason": reason,
    }


def _dispatch_health_components(
    by_key: dict[str, dict[str, Any]],
    now: datetime,
) -> tuple[dict[str, dict[str, Any]], bool]:
    """신규 작업 승인에 필요한 핵심 publication의 상태와 정합성을 판정한다."""
    station = by_key.get("station")
    stock = by_key.get("station_stock")
    stock_component = _publication_state(
        stock,
        now,
        freshness=STOCK_FRESHNESS,
    )
    if station is None or stock is None:
        stock_component = _publication_state(None, now, freshness=STOCK_FRESHNESS)
    elif (
        station["logical_dttm"] != stock["logical_dttm"]
        and stock_component["state"] not in {"missing", "expired"}
    ):
        stock_component = {
            **stock_component,
            "state": "misaligned",
            "reason": "station_stock_anchor_mismatch",
        }

    components = {
        "stock": stock_component,
        "demand": _publication_state(
            by_key.get("station_demand_forecast"),
            now,
            freshness=DEMAND_FRESHNESS,
        ),
        "urgency": _publication_state(
            by_key.get("station_urgency"),
            now,
            freshness=URGENCY_FRESHNESS,
            expiry=URGENCY_EXPIRY,
        ),
        "routes": _publication_state(
            by_key.get("rebalance_route"),
            now,
            freshness=URGENCY_FRESHNESS,
        ),
    }
    dispatch_times = [component["data_dttm"] for component in components.values()]
    can_dispatch = (
        all(component["state"] == "ready" for component in components.values())
        and len(set(dispatch_times)) == 1
    )
    if not can_dispatch and all(
        component["state"] == "ready" for component in components.values()
    ):
        components = {
            key: {
                **component,
                "state": "misaligned",
                "reason": "operational_anchor_mismatch",
            }
            for key, component in components.items()
        }
    return components, can_dispatch


def fetch_serving_health(now: datetime) -> dict[str, Any]:
    """publication state와 실제 날씨 horizon으로 대시보드 서빙 상태를 판정한다."""
    rows = fetch_all(
        """
        WITH desired(publication_key) AS (
            VALUES ('dispatch_center'),
                   ('station'),
                   ('station_stock'),
                   ('station_demand_forecast'),
                   ('station_urgency'),
                   ('rebalance_route'),
                   ('weather_forecast'),
                   ('event:cultural_event'),
                   ('event:performance_event')
        ),
        weather_horizon AS (
            SELECT COUNT(*) AS weather_row_cnt,
                   MIN(wf.base_dttm) AS oldest_weather_issue_dttm,
                   (
                       SELECT COUNT(DISTINCT weather_grid_id) * %(forecast_hours)s
                         FROM station
                        WHERE is_active
                   ) AS expected_weather_row_cnt,
                   COALESCE(
                       BOOL_AND(
                           CASE wf.source_product_cd
                               WHEN 'ultra_short' THEN
                                   wf.base_dttm BETWEEN %(now)s - INTERVAL '2 hours'
                                                       AND %(now)s + INTERVAL '5 minutes'
                               WHEN 'short_term' THEN
                                   wf.base_dttm BETWEEN %(now)s - INTERVAL '4 hours'
                                                       AND %(now)s + INTERVAL '5 minutes'
                               ELSE FALSE
                           END
                       ),
                       FALSE
                   ) AS weather_rows_fresh
              FROM weather_forecast AS wf
             WHERE wf.forecast_dttm >= date_trunc('hour', %(now)s::TIMESTAMPTZ)
                                           + INTERVAL '1 hour'
               AND wf.forecast_dttm < date_trunc('hour', %(now)s::TIMESTAMPTZ)
                                           + (%(forecast_hours)s + 1) * INTERVAL '1 hour'
        )
        SELECT desired.publication_key,
               state.logical_dttm,
               state.published_row_cnt,
               weather.weather_row_cnt,
               weather.expected_weather_row_cnt,
               weather.weather_rows_fresh,
               weather.oldest_weather_issue_dttm
          FROM desired
          LEFT JOIN gold_meta.publication_state AS state USING (publication_key)
         CROSS JOIN weather_horizon AS weather
         ORDER BY desired.publication_key
        """,
        {"now": now, "forecast_hours": FORECAST_HOUR_COUNT},
    )
    by_key = {row["publication_key"]: row for row in rows}

    dispatch_components, can_dispatch = _dispatch_health_components(by_key, now)
    weather_row = by_key.get("weather_forecast")
    weather_component = _publication_state(
        weather_row,
        now,
        freshness=DEMAND_FRESHNESS,
    )
    if weather_row is not None:
        weather_component["source_dttm"] = weather_row["oldest_weather_issue_dttm"]
    if weather_row is not None and weather_component["state"] == "ready":
        complete = weather_row["weather_row_cnt"] == weather_row["expected_weather_row_cnt"]
        if not complete:
            weather_component = {
                **weather_component,
                "state": "misaligned",
                "reason": "weather_horizon_incomplete",
            }
        elif not weather_row["weather_rows_fresh"]:
            weather_component = {
                **weather_component,
                "state": "stale",
                "reason": "weather_issue_stale",
            }

    event_rows = [
        by_key.get("event:cultural_event"),
        by_key.get("event:performance_event"),
    ]
    event_parts = [
        _publication_state(row, now, freshness=EVENT_FRESHNESS, expiry=None)
        for row in event_rows
    ]
    event_component = min(
        event_parts,
        key=lambda item: {
            "missing": 0,
            "expired": 1,
            "misaligned": 2,
            "stale": 3,
            "ready": 4,
        }[item["state"]],
    )
    event_times = [item["data_dttm"] for item in event_parts if item["data_dttm"]]
    if event_times:
        event_component = {
            **event_component,
            "data_dttm": min(event_times),
            "age_minutes": round((now - min(event_times)).total_seconds() / 60.0, 1),
            "reason": (
                "fresh" if all(item["state"] == "ready" for item in event_parts)
                else "event_source_incomplete"
            ),
        }

    region_row = by_key.get("dispatch_center")
    region_component = {
        **_publication_state(region_row, now, freshness=timedelta(days=36500), expiry=None),
        "reason": "fresh" if region_row and region_row["published_row_cnt"] > 0 else "not_published",
    }
    if region_row is None or region_row["published_row_cnt"] <= 0:
        region_component["state"] = "missing"

    components = {
        **dispatch_components,
        "weather": weather_component,
        "events": event_component,
        "regions": region_component,
    }

    stock_time = components["stock"]["data_dttm"]
    demand_time = components["demand"]["data_dttm"]
    operational_base = stock_time if stock_time is not None and stock_time == demand_time else None
    core_unavailable = any(
        components[key]["state"] in {"missing", "expired"}
        for key in ("stock", "demand")
    )
    overall = (
        "unavailable"
        if core_unavailable
        else "healthy"
        if all(component["state"] == "ready" for component in components.values())
        else "degraded"
    )
    return {
        "overall": overall,
        "operational_base_dttm": operational_base,
        "checked_at": now,
        "can_dispatch_new_routes": can_dispatch,
        "components": components,
    }


def fetch_nearby_events(
    sta_id: str,
    now: datetime,
    radius_km: float = NEARBY_EVENT_RADIUS_KM,
    *,
    allow_stale: bool = False,
) -> list[dict[str, Any]] | None:
    """활성 station 주변의 현재·예정 행사를 freshness 옵션에 맞춰 반환한다.

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
               AND e.last_seen_dttm <= %(now)s + INTERVAL '5 minutes'
               AND (
                   %(allow_stale)s
                   OR e.last_seen_dttm >= %(now)s - INTERVAL '36 hours'
               )
             ORDER BY distance_km, e.event_id
            """,
            {
                "sta_id": sta_id,
                "now": now,
                "radius_m": radius_km * 1000.0,
                "allow_stale": allow_stale,
            },
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
    *,
    allow_stale: bool = False,
) -> WeatherResult:
    """활성 대여소의 미래 정시 날씨를 freshness 옵션에 맞춰 반환한다."""
    station_exists, rows = _read_weather_snapshot(sta_id, now, hours)
    if not station_exists:
        return WeatherResult(WeatherState.STATION_NOT_FOUND)

    first_target = now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    expected_targets = [
        first_target + timedelta(hours=offset) for offset in range(hours)
    ]
    actual_targets = [row["forecast_dttm"] for row in rows]
    expected_available_targets = expected_targets[: len(rows)]
    invalid_shape = (
        not rows
        or actual_targets != expected_available_targets
        or any(row["base_dttm"] > now + FUTURE_TOLERANCE for row in rows)
        or (not allow_stale and len(rows) != hours)
    )
    if invalid_shape or (
        not allow_stale
        and any(not _is_forecast_issue_fresh(row, now) for row in rows)
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


def fetch_alerts(now: datetime, *, include_expired: bool = False) -> list[dict[str, Any]]:
    """마지막 완전 성공 urgency snapshot을 freshness 정책 안에서 반환한다.

    계산용 current stock/demand와 다시 조인하지 않는다. ``publication_state``가
    가리키는 exact anchor의 전체 urgency set만 선택하고 게시 row count가 맞을
    때만 제공하므로, 새 tick 일부만 섞인 비정상 상태도 정상 snapshot으로 보지
    않는다.
    """
    return fetch_all(
        """
        WITH urgency_authority AS MATERIALIZED (
            SELECT logical_dttm,
                   published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = 'station_urgency'
               AND logical_dttm BETWEEN %(now)s - %(expiry)s
                                    AND %(now)s + %(future_tolerance)s
        ),
        urgency_snapshot AS MATERIALIZED (
            SELECT urgency.*
              FROM station_urgency AS urgency
              JOIN urgency_authority AS authority
                ON authority.logical_dttm = urgency.base_dttm
        ),
        complete_authority AS MATERIALIZED (
            SELECT authority.logical_dttm
              FROM urgency_authority AS authority
             WHERE authority.published_row_cnt = (
                       SELECT COUNT(*) FROM urgency_snapshot
                   )
        )
        SELECT s.sta_id,
               s.sta_nm,
               urgency.rebalance_need_type_cd AS action_type,
               urgency.urgency_score,
               urgency.critical_remaining_min AS minutes_until_critical,
               center.dispatch_center_nm AS region,
               urgency.base_dttm,
               CASE
                   WHEN urgency.base_dttm >= %(now)s - %(freshness)s
                   THEN 'fresh'
                   ELSE 'stale'
               END AS data_status,
               GREATEST(
                   0.0,
                   EXTRACT(EPOCH FROM (%(now)s - urgency.base_dttm)) / 60.0
               )::double precision AS age_minutes
          FROM complete_authority AS authority
          JOIN urgency_snapshot AS urgency
            ON urgency.base_dttm = authority.logical_dttm
          JOIN station AS s
            ON s.sta_id = urgency.sta_id
           AND s.is_active
          JOIN dispatch_center AS center
            ON center.dispatch_center_id = s.dispatch_center_id
           AND center.is_active
         ORDER BY urgency.urgency_score DESC, s.sta_id ASC
        """,
        {
            "now": now,
            "freshness": URGENCY_FRESHNESS,
            "expiry": timedelta(days=36500) if include_expired else URGENCY_EXPIRY,
            "future_tolerance": FUTURE_TOLERANCE,
        },
    )


def _route_aggregate_query(where_clause: str, page_clause: str = "") -> str:
    """route header와 stop을 한 statement에서 읽는 SQL을 만든다."""
    return f"""
        WITH approved_route_number AS MATERIALIZED (
            SELECT stored_route.route_id,
                   ROW_NUMBER() OVER (
                       PARTITION BY
                           stored_route.dispatch_center_id,
                           (stored_route.dispatched_dttm AT TIME ZONE 'Asia/Seoul')::date
                       ORDER BY stored_route.dispatched_dttm, stored_route.route_id
                   ) AS work_no
              FROM rebalance_route AS stored_route
             WHERE stored_route.dispatched_dttm IS NOT NULL
        ),
        route_page AS MATERIALIZED (
            SELECT route.route_id,
                   number.work_no,
                   center.dispatch_center_nm AS region,
                   route.route_status_cd AS status,
                   route.proposed_dttm AS proposed_at,
                   route.dispatched_dttm AS dispatched_at,
                   route.completed_dttm AS completed_at,
                   route.cancelled_dttm AS cancelled_at,
                   route.dismissed_dttm AS dismissed_at,
                   route.restored_from_route_id::text AS restored_from_route_id
              FROM rebalance_route AS route
              JOIN dispatch_center AS center USING (dispatch_center_id)
              LEFT JOIN approved_route_number AS number USING (route_id)
             {where_clause}
             ORDER BY route.proposed_dttm DESC, route.route_id ASC
             {page_clause}
        )
        SELECT page.route_id::text AS route_id,
               page.work_no,
               page.region,
               page.status,
               page.proposed_at,
               page.dispatched_at,
               page.completed_at,
               page.cancelled_at,
               page.dismissed_at,
               page.restored_from_route_id,
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
    closed_since: datetime | None = None,
) -> list[dict[str, Any]]:
    """상태·종료 시간창과 bounded pagination으로 route 목록을 반환한다."""
    # 삭제한 작업과 과거 복제 방식으로 이미 후속 route가 생긴 원본은 목록에서
    # 제외한다. 단건 조회(fetch_route)는 감사 이력을 읽을 수 있도록 유지한다.
    conditions: list[str] = [
        "route.dismissed_dttm IS NULL",
        """
        NOT EXISTS (
            SELECT 1
              FROM rebalance_route AS restored_route
             WHERE restored_route.restored_from_route_id = route.route_id
        )
        """,
    ]
    params: dict[str, Any] = {"limit": limit, "offset": offset}
    if region is not None:
        conditions.append("center.dispatch_center_nm = %(region)s")
        params["region"] = region
    if status is not None:
        conditions.append("route.route_status_cd = %(status)s")
        params["status"] = status
    if closed_since is not None:
        conditions.append(
            """
            (
                route.route_status_cd IN ('proposed', 'dispatched')
                OR (
                    route.route_status_cd = 'completed'
                    AND route.completed_dttm >= %(closed_since)s
                )
                OR (
                    route.route_status_cd = 'cancelled'
                    AND route.cancelled_dttm >= %(closed_since)s
                )
            )
            """
        )
        params["closed_since"] = closed_since
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


def _transition_route(
    route_id: UUID,
    now: datetime,
    *,
    expected_status: str,
    next_status: str,
    timestamp_column: str,
) -> dict[str, Any] | RouteTransitionResult:
    """guarded update와 aggregate 재조회를 같은 transaction에서 수행한다."""
    try:
        with (
            get_connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            return _transition_route_with_cursor(
                cursor,
                route_id,
                now,
                expected_status=expected_status,
                next_status=next_status,
                timestamp_column=timestamp_column,
            )
    except CheckViolation:
        return RouteTransitionResult.CONSTRAINT_CONFLICT


def _transition_route_with_cursor(
    cursor: Cursor[dict[str, Any]],
    route_id: UUID,
    now: datetime,
    *,
    expected_status: str,
    next_status: str,
    timestamp_column: str,
) -> dict[str, Any] | RouteTransitionResult:
    """주어진 transaction cursor에서 route 상태를 전이하고 aggregate를 반환한다."""
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
    route = _fetch_route_with_cursor(cursor, route_id)
    if route is None:
        raise RuntimeError("상태 전이 직후 route aggregate를 찾을 수 없습니다.")
    return route


def _fetch_dispatch_publications(
    cursor: Cursor[dict[str, Any]],
) -> dict[str, dict[str, Any]]:
    """승인 transaction의 snapshot에서 핵심 publication 상태를 조회한다."""
    cursor.execute(
        """
        SELECT publication_key,
               logical_dttm,
               published_row_cnt
          FROM gold_meta.publication_state
         WHERE publication_key IN (
             'station',
             'station_stock',
             'station_demand_forecast',
             'station_urgency',
             'rebalance_route'
         )
         FOR SHARE
        """
    )
    return {row["publication_key"]: row for row in cursor.fetchall()}


def dispatch_route(
    route_id: UUID,
    now: datetime,
) -> dict[str, Any] | RouteTransitionResult:
    """핵심 publication을 잠근 동일 transaction에서 route를 원자 승인한다."""
    try:
        with (
            get_connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            by_key = _fetch_dispatch_publications(cursor)
            _, can_dispatch = _dispatch_health_components(by_key, now)
            if not can_dispatch:
                return RouteTransitionResult.SERVING_NOT_READY
            return _transition_route_with_cursor(
                cursor,
                route_id,
                now,
                expected_status="proposed",
                next_status="dispatched",
                timestamp_column="dispatched_dttm",
            )
    except CheckViolation:
        return RouteTransitionResult.CONSTRAINT_CONFLICT


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


def dismiss_route(
    route_id: UUID,
    now: datetime,
) -> dict[str, Any] | RouteTransitionResult:
    """종료된 route를 목록에서만 감춘다. 행과 이력은 그대로 남긴다."""
    try:
        with (
            get_connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                UPDATE rebalance_route
                   SET dismissed_dttm = %(now)s
                 WHERE route_id = %(route_id)s
                   AND route_status_cd IN ('completed', 'cancelled')
                   AND dismissed_dttm IS NULL
                RETURNING route_id
                """,
                {"route_id": route_id, "now": now},
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    """
                    SELECT dismissed_dttm
                      FROM rebalance_route
                     WHERE route_id = %(route_id)s
                    """,
                    {"route_id": route_id},
                )
                existing = cursor.fetchone()
                if existing is None:
                    return RouteTransitionResult.NOT_FOUND
                if existing["dismissed_dttm"] is not None:
                    return RouteTransitionResult.ALREADY_DISMISSED
                return RouteTransitionResult.WRONG_STATUS
            route = _fetch_route_with_cursor(cursor, route_id)
            if route is None:
                raise RuntimeError("삭제 직후 route aggregate를 찾을 수 없습니다.")
            return route
    except CheckViolation:
        return RouteTransitionResult.CONSTRAINT_CONFLICT


def restore_route(
    route_id: UUID,
) -> dict[str, Any] | RouteTransitionResult:
    """취소된 route를 동일 ID의 dispatched 상태로 원자 복원한다.

    취소 시각만 비우고 최초 승인 시각과 stop은 보존한다. 같은 요청이 재시도되어
    이미 dispatched라면 현재 route를 반환해 idempotent하게 처리한다.
    """
    try:
        with (
            get_connection() as connection,
            connection.cursor(row_factory=dict_row) as cursor,
        ):
            cursor.execute(
                """
                UPDATE rebalance_route
                   SET route_status_cd = 'dispatched',
                       cancelled_dttm = NULL
                 WHERE route_id = %(route_id)s
                   AND route_status_cd = 'cancelled'
                   AND dismissed_dttm IS NULL
                RETURNING route_id
                """,
                {"route_id": route_id},
            )
            if cursor.fetchone() is None:
                cursor.execute(
                    """
                    SELECT route_status_cd, dismissed_dttm
                      FROM rebalance_route
                     WHERE route_id = %(route_id)s
                    """,
                    {"route_id": route_id},
                )
                existing = cursor.fetchone()
                if existing is None:
                    return RouteTransitionResult.NOT_FOUND
                if existing["dismissed_dttm"] is not None:
                    return RouteTransitionResult.ALREADY_DISMISSED
                if existing["route_status_cd"] != "dispatched":
                    return RouteTransitionResult.WRONG_STATUS
            route = _fetch_route_with_cursor(cursor, route_id)
            if route is None:
                raise RuntimeError("복원 직후 route aggregate를 찾을 수 없습니다.")
            return route
    except CheckViolation:
        return RouteTransitionResult.CONSTRAINT_CONFLICT
