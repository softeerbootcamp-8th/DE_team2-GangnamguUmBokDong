"""Gold PostGIS API query와 freshness 판정 단위 테스트다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Self
from uuid import UUID

import pytest
import queries
from psycopg.errors import CheckViolation, RaiseException

NOW = datetime(2026, 8, 20, 1, 5, tzinfo=UTC)
BASE = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
ROUTE_ID = UUID("11111111-1111-4111-8111-111111111111")
_ULTRA_SHORT_HOUR_COUNT = 6


def _forecast_points(base_dttm: datetime = BASE) -> list[dict[str, Any]]:
    """정확한 12시간 demand fixture를 만든다."""
    return [
        {
            "base_dttm": base_dttm,
            "predicted_dttm": base_dttm + timedelta(hours=hour),
            "predicted_rent_cnt": hour,
            "predicted_return_cnt": hour + 1,
        }
        for hour in range(1, queries.FORECAST_HOUR_COUNT + 1)
    ]


def _forecast_snapshot(
    *,
    base_dttm: datetime = BASE,
) -> tuple[dict, dict, list[dict], dict]:
    """ready forecast 판정에 필요한 DB snapshot fixture를 만든다."""
    station = {
        "sta_id": "ST-1",
        "hold_cnt": 10,
        "last_seen_dttm": base_dttm,
    }
    summary = {
        "row_cnt": queries.FORECAST_HOUR_COUNT,
        "min_base_dttm": base_dttm,
        "max_base_dttm": base_dttm,
    }
    stock = {
        "sta_id": "ST-1",
        "base_dttm": base_dttm,
        "parking_bike_tot_cnt": 4,
    }
    return station, summary, _forecast_points(base_dttm), stock


def _weather_rows(
    *,
    ultra_short_base_dttm: datetime = NOW - timedelta(hours=1),
    short_term_base_dttm: datetime = NOW - timedelta(hours=3),
) -> list[dict[str, Any]]:
    """앞 6정시는 초단기, 뒤 6정시는 단기예보인 12행 fixture를 만든다.

    각 base_dttm 기본값은 그 제품의 직전 발표(초단기 1시간 주기, 단기 3시간 주기)다.
    """
    first_target = NOW.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    return [
        {
            "forecast_dttm": first_target + timedelta(hours=offset),
            "temperature": 25.0,
            "sky_condition_cd": "clear",
            "precipitation_type_cd": "none",
            "precipitation_prob": None,
            "precipitation_amount": None,
            "humidity": 60.0,
            "wind_speed": 2.0,
            "source_product_cd": (
                "ultra_short" if offset < _ULTRA_SHORT_HOUR_COUNT else "short_term"
            ),
            "base_dttm": (
                ultra_short_base_dttm
                if offset < _ULTRA_SHORT_HOUR_COUNT
                else short_term_base_dttm
            ),
        }
        for offset in range(queries.FORECAST_HOUR_COUNT)
    ]


@pytest.mark.parametrize(
    "value,max_age,expected",
    [
        (NOW - timedelta(minutes=10), timedelta(minutes=10), True),
        (NOW - timedelta(minutes=10, microseconds=1), timedelta(minutes=10), False),
        (NOW + timedelta(minutes=5), timedelta(minutes=10), True),
        (NOW + timedelta(minutes=5, microseconds=1), timedelta(minutes=10), False),
        (None, timedelta(minutes=10), False),
    ],
)
def test_is_fresh_uses_inclusive_bidirectional_boundaries(
    value: datetime | None,
    max_age: timedelta,
    expected: bool,
) -> None:
    """freshness는 과거 cutoff와 미래 5분 경계를 모두 포함한다."""
    assert queries._is_fresh(value, NOW, max_age) is expected


def test_fetch_stations_uses_active_postgis_contract(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """station 목록은 Point와 center를 읽고 같은 anchor의 fresh stock만 조인한다."""
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params: dict) -> list[dict]:
        """SQL과 parameter를 기록한다."""
        captured.update(query=query, params=params)
        return [{"sta_id": "ST-1"}]

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert queries.fetch_stations(NOW) == [{"sta_id": "ST-1"}]
    normalized = " ".join(captured["query"].split())
    assert "FROM station AS s" in normalized
    assert "JOIN station_stock AS stock" in normalized
    assert "stock.base_dttm = s.last_seen_dttm" in normalized
    assert "JOIN dispatch_center AS center" in normalized
    assert "ST_Y(s.sta_point) AS lat" in normalized
    assert "ST_X(s.sta_point) AS lon" in normalized
    assert "s.is_active" in normalized
    assert "center.is_active" in normalized
    assert "BETWEEN %(now)s - INTERVAL '10 minutes'" in normalized
    assert "stations" not in normalized
    assert ".gu" not in normalized
    assert captured["params"] == {"now": NOW}


def test_fetch_station_adds_address_without_legacy_columns(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """station 상세는 목표 단수 테이블과 Point 파생 좌표를 사용한다."""
    captured: dict[str, Any] = {}

    def fake_fetch_one(query: str, params: dict) -> dict:
        """SQL과 parameter를 기록한다."""
        captured.update(query=query, params=params)
        return {"sta_id": "ST-1"}

    monkeypatch.setattr(queries, "fetch_one", fake_fetch_one)

    assert queries.fetch_station("ST-1", NOW) == {"sta_id": "ST-1"}
    normalized = " ".join(captured["query"].split())
    assert "s.sta_addr" in normalized
    assert "ST_Y(s.sta_point) AS lat" in normalized
    assert "FROM station AS s" in normalized
    assert "FROM stations" not in normalized
    assert captured["params"] == {"sta_id": "ST-1", "now": NOW}


def test_fetch_forecast_returns_exact_ready_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """완전 demand와 같은 anchor stock이면 ready 12행을 반환한다."""
    monkeypatch.setattr(
        queries, "_read_forecast_snapshot", lambda _sta_id: _forecast_snapshot()
    )

    result = queries.fetch_forecast("ST-1", NOW)

    assert result.state is queries.ForecastState.READY
    assert result.base_dttm == BASE
    assert result.station == {
        "sta_id": "ST-1",
        "hold_cnt": 10,
        "last_seen_dttm": BASE,
        "parking_bike_tot_cnt": 4,
    }
    assert len(result.points) == 12
    assert result.points[0]["predicted_return_cnt"] == 2
    assert "base_dttm" not in result.points[0]


def test_fetch_forecast_reports_missing_station(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """inactive 또는 없는 station은 forecast에서도 station 404 상태다."""
    _station, summary, points, stock = _forecast_snapshot()
    monkeypatch.setattr(
        queries,
        "_read_forecast_snapshot",
        lambda _sta_id: (None, summary, points, stock),
    )

    assert (
        queries.fetch_forecast("ST-1", NOW).state
        is queries.ForecastState.STATION_NOT_FOUND
    )


def test_fetch_forecast_reports_unpublished_projection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """공통 demand projection이 없으면 현재시각으로 위장하지 않고 503 상태다."""
    station, _summary, _points, stock = _forecast_snapshot()
    summary = {"row_cnt": 0, "min_base_dttm": None, "max_base_dttm": None}
    monkeypatch.setattr(
        queries,
        "_read_forecast_snapshot",
        lambda _sta_id: (station, summary, [], stock),
    )

    assert (
        queries.fetch_forecast("ST-1", NOW).state
        is queries.ForecastState.FORECAST_NOT_READY
    )


def test_fetch_forecast_reports_model_unsupported_station(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """fresh 공통 projection에 station row가 없으면 모델 미지원 404 상태다."""
    station, summary, _points, stock = _forecast_snapshot()
    monkeypatch.setattr(
        queries,
        "_read_forecast_snapshot",
        lambda _sta_id: (station, summary, [], stock),
    )

    result = queries.fetch_forecast("ST-1", NOW)

    assert result.state is queries.ForecastState.FORECAST_NOT_AVAILABLE


@pytest.mark.parametrize(
    "defect", ["mixed_base", "eleven_rows", "wrong_target", "stale"]
)
def test_fetch_forecast_fails_closed_for_incomplete_demand(
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    """mixed·부분·잘못된 horizon·stale demand를 부분 차트로 반환하지 않는다."""
    station, summary, points, stock = _forecast_snapshot()
    if defect == "mixed_base":
        summary["max_base_dttm"] = BASE + timedelta(minutes=5)
    elif defect == "eleven_rows":
        points.pop()
        summary["row_cnt"] -= 1
    elif defect == "wrong_target":
        points[3]["predicted_dttm"] += timedelta(minutes=5)
    else:
        stale_base = NOW - queries.DEMAND_FRESHNESS - timedelta(microseconds=1)
        station, summary, points, stock = _forecast_snapshot(base_dttm=stale_base)
    monkeypatch.setattr(
        queries,
        "_read_forecast_snapshot",
        lambda _sta_id: (station, summary, points, stock),
    )

    assert (
        queries.fetch_forecast("ST-1", NOW).state
        is queries.ForecastState.FORECAST_NOT_READY
    )


@pytest.mark.parametrize(
    "defect", ["missing", "different_base", "different_seen", "future"]
)
def test_fetch_forecast_rejects_unaligned_stock(
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    """stock 부재·anchor 불일치·혼합 release·future 재고를 503 상태로 판정한다."""
    station, summary, points, stock = _forecast_snapshot()
    if defect == "missing":
        stock = None
    elif defect == "different_base":
        stock["base_dttm"] = BASE - timedelta(minutes=5)
    elif defect == "different_seen":
        station["last_seen_dttm"] = BASE - timedelta(minutes=5)
    else:
        future_base = NOW + queries.FUTURE_TOLERANCE + timedelta(microseconds=1)
        station["last_seen_dttm"] = future_base
        stock["base_dttm"] = future_base
    monkeypatch.setattr(
        queries,
        "_read_forecast_snapshot",
        lambda _sta_id: (station, summary, points, stock),
    )

    assert (
        queries.fetch_forecast("ST-1", NOW).state
        is queries.ForecastState.STOCK_NOT_ALIGNED
    )


@pytest.mark.parametrize(
    "summary,expected",
    [
        ({"row_cnt": 0, "min_base_dttm": None, "max_base_dttm": None}, None),
        ({"row_cnt": 12, "min_base_dttm": BASE, "max_base_dttm": BASE}, BASE),
        (
            {
                "row_cnt": 12,
                "min_base_dttm": BASE,
                "max_base_dttm": BASE + timedelta(minutes=5),
            },
            None,
        ),
    ],
)
def test_status_returns_only_fresh_common_published_base(
    monkeypatch: pytest.MonkeyPatch,
    summary: dict,
    expected: datetime | None,
) -> None:
    """status는 실제 공통 demand base만 반환하고 fallback을 만들지 않는다."""
    monkeypatch.setattr(queries, "fetch_one", lambda _query: summary)

    assert queries.fetch_status_base_dttm(NOW) == expected


def test_fetch_weather_returns_exact_fresh_horizon(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """날씨는 미래 첫 정시부터 정확히 12행이고 모두 fresh해야 한다."""
    rows = _weather_rows(
        ultra_short_base_dttm=NOW - queries.ULTRA_SHORT_FRESHNESS,
        short_term_base_dttm=NOW - queries.SHORT_TERM_FRESHNESS,
    )
    monkeypatch.setattr(
        queries,
        "_read_weather_snapshot",
        lambda _sta_id, _now, _hours: (True, rows),
    )

    result = queries.fetch_weather("ST-1", NOW)

    assert result.state is queries.WeatherState.READY
    assert len(result.points) == 12
    assert "base_dttm" not in result.points[0]
    assert "source_product_cd" not in result.points[0]


def test_fetch_weather_accepts_short_term_issued_over_45_minutes_ago(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """단기예보는 3시간 주기 발표이므로 3시간 전 발표도 fresh다."""
    rows = _weather_rows(short_term_base_dttm=NOW - timedelta(hours=3))
    monkeypatch.setattr(
        queries,
        "_read_weather_snapshot",
        lambda _sta_id, _now, _hours: (True, rows),
    )

    assert queries.fetch_weather("ST-1", NOW).state is queries.WeatherState.READY


def test_fetch_weather_reports_missing_station(monkeypatch: pytest.MonkeyPatch) -> None:
    """inactive 또는 없는 station의 날씨는 station 404 상태다."""
    monkeypatch.setattr(
        queries,
        "_read_weather_snapshot",
        lambda _sta_id, _now, _hours: (False, []),
    )

    assert (
        queries.fetch_weather("ST-1", NOW).state
        is queries.WeatherState.STATION_NOT_FOUND
    )


@pytest.mark.parametrize(
    "defect",
    [
        "missing",
        "wrong_hour",
        "stale_ultra_short",
        "stale_short_term",
        "future",
        "unknown_product",
    ],
)
def test_fetch_weather_fails_closed_for_partial_or_stale_rows(
    monkeypatch: pytest.MonkeyPatch,
    defect: str,
) -> None:
    """행 누락·시간 틀림·제품별 freshness 위반·미지 제품은 weather_not_ready다."""
    rows = _weather_rows()
    if defect == "missing":
        rows.pop()
    elif defect == "wrong_hour":
        rows[4]["forecast_dttm"] += timedelta(minutes=30)
    elif defect == "stale_ultra_short":
        rows[4]["base_dttm"] = (
            NOW - queries.ULTRA_SHORT_FRESHNESS - timedelta(microseconds=1)
        )
    elif defect == "stale_short_term":
        rows[8]["base_dttm"] = (
            NOW - queries.SHORT_TERM_FRESHNESS - timedelta(microseconds=1)
        )
    elif defect == "future":
        rows[4]["base_dttm"] = (
            NOW + queries.FUTURE_TOLERANCE + timedelta(microseconds=1)
        )
    else:
        rows[4]["source_product_cd"] = "nowcast"
    monkeypatch.setattr(
        queries,
        "_read_weather_snapshot",
        lambda _sta_id, _now, _hours: (True, rows),
    )

    assert (
        queries.fetch_weather("ST-1", NOW).state
        is queries.WeatherState.WEATHER_NOT_READY
    )


def test_fetch_regions_uses_active_center_points(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """regions는 Python 상수가 아니라 active center Point를 읽는다."""
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str) -> list[dict]:
        """실행할 SQL을 기록한다."""
        captured["query"] = query
        return []

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert queries.fetch_regions() == []
    normalized = " ".join(captured["query"].split())
    assert "FROM dispatch_center" in normalized
    assert "WHERE is_active" in normalized
    assert "ST_Y(dispatch_center_point) AS lat" in normalized
    assert "ST_X(dispatch_center_point) AS lon" in normalized


def test_fetch_alerts_uses_complete_publication_state_snapshot(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """alerts는 current stock이 아니라 authoritative urgency snapshot을 읽는다."""
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params: dict) -> list[dict]:
        """실행할 SQL과 parameter를 기록한다."""
        captured.update(query=query, params=params)
        return []

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert queries.fetch_alerts(NOW) == []
    normalized = " ".join(captured["query"].split())
    assert "FROM gold_meta.publication_state" in normalized
    assert "publication_key = 'station_urgency'" in normalized
    assert "authority.logical_dttm = urgency.base_dttm" in normalized
    assert "authority.published_row_cnt = ( SELECT COUNT(*) FROM urgency_snapshot )" in normalized
    assert "JOIN station_stock" not in normalized
    assert "s.last_seen_dttm" not in normalized
    assert "ORDER BY urgency.urgency_score DESC, s.sta_id ASC" in normalized
    assert "rebalance_need_type_cd AS action_type" in normalized
    assert "critical_remaining_min AS minutes_until_critical" in normalized
    assert "END AS data_status" in normalized
    assert "AS age_minutes" in normalized
    assert captured["params"] == {
        "now": NOW,
        "freshness": queries.URGENCY_FRESHNESS,
        "expiry": queries.URGENCY_EXPIRY,
        "future_tolerance": queries.FUTURE_TOLERANCE,
    }


def test_fetch_routes_builds_bounded_single_statement_aggregate(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route page와 정렬 stop은 한 SQL statement에서 함께 조회한다."""
    captured: dict[str, Any] = {}

    def fake_fetch_all(query: str, params: dict) -> list[dict]:
        """실행할 route SQL과 parameter를 기록한다."""
        captured.update(query=query, params=params)
        return []

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    result = queries.fetch_routes("강남", "proposed", limit=20, offset=40)

    assert result == []
    normalized = " ".join(captured["query"].split())
    assert "FROM rebalance_route AS route" in normalized
    assert "FROM rebalance_route_stop AS stop" in normalized
    assert "LEFT JOIN LATERAL" in normalized
    assert "jsonb_agg" in normalized
    assert "route.route_id::text AS route_id" not in normalized
    assert "page.route_id::text AS route_id" in normalized
    assert "ORDER BY route.proposed_dttm DESC, route.route_id ASC" in normalized
    assert "ORDER BY stop.visit_no" in normalized
    assert "LIMIT %(limit)s OFFSET %(offset)s" in normalized
    assert captured["params"] == {
        "limit": 20,
        "offset": 40,
        "region": "강남",
        "status": "proposed",
    }


def test_fetch_route_casts_uuid_to_text_in_one_statement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route 상세는 UUID 입력과 문자열 출력 alias를 같은 aggregate SQL에 둔다."""
    captured: dict[str, Any] = {}

    def fake_fetch_one(query: str, params: dict) -> dict:
        """실행할 route 상세 SQL과 parameter를 기록한다."""
        captured.update(query=query, params=params)
        return {"route_id": str(ROUTE_ID), "stops": []}

    monkeypatch.setattr(queries, "fetch_one", fake_fetch_one)

    result = queries.fetch_route(ROUTE_ID)

    assert result == {"route_id": str(ROUTE_ID), "stops": []}
    normalized = " ".join(captured["query"].split())
    assert "page.route_id::text AS route_id" in normalized
    assert "route.route_id = %(route_id)s" in normalized
    assert "rebalance_routes" not in normalized
    assert captured["params"] == {"route_id": ROUTE_ID}


def test_station_conflict_sql_excludes_self_and_closed_routes() -> None:
    """겹침 검사는 자기 경로를 빼고 dispatched 경로만 본다."""
    normalized = " ".join(queries._STATION_CONFLICT_SQL.split())

    assert "count(DISTINCT candidate_stop.sta_id) AS conflict_cnt" in normalized
    assert "FROM rebalance_route_stop AS candidate_stop" in normalized
    assert "JOIN rebalance_route_stop AS active_stop USING (sta_id)" in normalized
    assert "active_route.route_id <> %(route_id)s" in normalized
    assert "active_route.route_status_cd = 'dispatched'" in normalized


class _TransitionCursor:
    """route 상태 전이의 동일 cursor 사용을 검증하는 최소 fake다."""

    def __init__(self, responses: list[dict | None]) -> None:
        """fetchone 응답 순서를 초기화한다."""
        self.responses = responses
        self.statements: list[str] = []

    def __enter__(self) -> Self:
        """context manager 진입 시 자신을 반환한다."""
        return self

    def __exit__(self, *_args: object) -> None:
        """context manager 종료를 처리한다."""

    def execute(self, query: str, _params: dict | None = None) -> None:
        """실행한 SQL을 순서대로 기록한다."""
        self.statements.append(query)

    def fetchone(self) -> dict | None:
        """준비된 단일 행 응답을 반환한다."""
        return self.responses.pop(0)


class _TransitionConnection:
    """한 route transaction이 cursor 하나만 쓰는지 검증하는 fake다."""

    def __init__(self, cursor: _TransitionCursor) -> None:
        """공유할 cursor를 저장한다."""
        self.cursor_value = cursor
        self.cursor_calls = 0
        self.rollback_calls = 0

    def __enter__(self) -> Self:
        """context manager 진입 시 자신을 반환한다."""
        return self

    def __exit__(self, *_args: object) -> None:
        """context manager 종료를 처리한다."""

    def cursor(self, **_kwargs: object) -> _TransitionCursor:
        """같은 cursor를 반환하고 호출 횟수를 센다."""
        self.cursor_calls += 1
        return self.cursor_value

    def rollback(self) -> None:
        """rollback 호출 횟수를 센다."""
        self.rollback_calls += 1


def test_dispatch_updates_and_reads_aggregate_in_same_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dispatch guarded update와 aggregate 조회는 같은 connection/cursor를 사용한다."""
    route = {"route_id": str(ROUTE_ID), "status": "dispatched", "stops": []}
    cursor = _TransitionCursor(
        [{"route_id": ROUTE_ID}, {"conflict_cnt": 0}, route]
    )
    connection = _TransitionConnection(cursor)
    monkeypatch.setattr(queries, "get_connection", lambda: connection)

    result = queries.dispatch_route(ROUTE_ID, NOW)

    assert result == route
    assert connection.cursor_calls == 1
    assert len(cursor.statements) == 3
    assert "UPDATE rebalance_route" in cursor.statements[0]
    assert "route_status_cd = %(expected_status)s" in cursor.statements[0]
    assert "rebalance_route_stop AS candidate_stop" in cursor.statements[1]
    assert "rebalance_route_stop AS stop" in cursor.statements[2]


def test_dispatch_rolls_back_and_skips_aggregate_on_station_conflict(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """겹침 guard가 걸리면 rollback 후 aggregate 조회 없이 STATION_CONFLICT를 반환한다."""
    cursor = _TransitionCursor([{"route_id": ROUTE_ID}, {"conflict_cnt": 1}])
    connection = _TransitionConnection(cursor)
    monkeypatch.setattr(queries, "get_connection", lambda: connection)

    result = queries.dispatch_route(ROUTE_ID, NOW)

    assert result is queries.RouteTransitionResult.STATION_CONFLICT
    assert connection.rollback_calls == 1
    assert len(cursor.statements) == 2


def test_cancel_sets_cancelled_timestamp_in_same_transaction(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cancel은 dispatched guard와 cancelled_dttm을 같은 transaction에서 갱신한다."""
    route = {"route_id": str(ROUTE_ID), "status": "cancelled", "stops": []}
    cursor = _TransitionCursor([{"route_id": ROUTE_ID}, route])
    connection = _TransitionConnection(cursor)
    monkeypatch.setattr(queries, "get_connection", lambda: connection)

    result = queries.cancel_route(ROUTE_ID, NOW)

    assert result == route
    assert "cancelled_dttm" in cursor.statements[0]
    assert "route_status_cd = %(expected_status)s" in cursor.statements[0]


@pytest.mark.parametrize(
    "existing,expected",
    [
        (None, queries.RouteTransitionResult.NOT_FOUND),
        ({"route_status_cd": "completed"}, queries.RouteTransitionResult.WRONG_STATUS),
    ],
)
def test_transition_distinguishes_missing_and_wrong_status_in_same_transaction(
    monkeypatch: pytest.MonkeyPatch,
    existing: dict | None,
    expected: queries.RouteTransitionResult,
) -> None:
    """guard가 매치하지 않으면 같은 transaction에서 404와 409 원인을 구분한다."""
    cursor = _TransitionCursor([None, existing])
    connection = _TransitionConnection(cursor)
    monkeypatch.setattr(queries, "get_connection", lambda: connection)

    assert queries.complete_route(ROUTE_ID, NOW) is expected
    assert connection.cursor_calls == 1
    assert len(cursor.statements) == 2


def test_transition_maps_only_target_schema_check_violation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """목표 DDL의 SQLSTATE 23514만 route constraint 409 결과로 변환한다."""

    def fail_connection() -> None:
        """target route trigger와 같은 CheckViolation을 발생시킨다."""
        raise CheckViolation("route topology conflict")

    monkeypatch.setattr(queries, "get_connection", fail_connection)

    assert (
        queries.dispatch_route(ROUTE_ID, NOW)
        is queries.RouteTransitionResult.CONSTRAINT_CONFLICT
    )


def test_transition_does_not_hide_unexpected_db_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """SSOT에 없는 SQLSTATE는 넓게 409 처리하지 않고 원인을 그대로 노출한다."""

    def fail_connection() -> None:
        """계약 밖 PL/pgSQL RaiseException을 발생시킨다."""
        raise RaiseException("unexpected database exception")

    monkeypatch.setattr(queries, "get_connection", fail_connection)

    with pytest.raises(RaiseException, match="unexpected database exception"):
        queries.dispatch_route(ROUTE_ID, NOW)
