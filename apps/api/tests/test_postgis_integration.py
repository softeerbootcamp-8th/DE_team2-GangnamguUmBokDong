"""빈 baseline을 적용한 disposable PostGIS에서 API SQL 계약을 검증한다."""

from __future__ import annotations

import os
from datetime import UTC, datetime, timedelta
from uuid import UUID
from zoneinfo import ZoneInfo

import main
import psycopg
import pytest
import queries
from fastapi.testclient import TestClient

DATABASE_URL_ENV = "GOLD_API_TEST_DATABASE_URL"
ROUTE_ID = UUID("11111111-1111-4111-8111-111111111111")
CONFLICT_ROUTE_ID = UUID("22222222-2222-4222-8222-222222222222")
CANCEL_ROUTE_ID = UUID("33333333-3333-4333-8333-333333333333")


@pytest.fixture
def database_url(monkeypatch: pytest.MonkeyPatch) -> str:
    """명시적으로 제공된 disposable PostGIS URL만 integration에 사용한다."""
    database_url = os.getenv(DATABASE_URL_ENV)
    if not database_url:
        pytest.skip(f"{DATABASE_URL_ENV}가 설정되지 않았습니다.")
    monkeypatch.setenv("DATABASE_URL", database_url)
    _truncate_targets(database_url)
    yield database_url
    _truncate_targets(database_url)


def _truncate_targets(database_url: str) -> None:
    """disposable DB의 API fixture target만 비운다."""
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            TRUNCATE TABLE
                rebalance_route_stop,
                rebalance_route,
                station_urgency,
                event,
                weather_forecast,
                station_demand_forecast,
                station_stock,
                station,
                dispatch_center,
                weather_grid
            CASCADE
            """
        )


def _executemany(
    connection: psycopg.Connection,
    query: str,
    params: list[dict],
) -> None:
    """psycopg cursor로 동일 statement의 fixture 여러 행을 삽입한다."""
    with connection.cursor() as cursor:
        cursor.executemany(query, params)


def _seed_serving_fixture(database_url: str, now: datetime) -> datetime:
    """모든 API endpoint가 읽을 최소 Gold projection을 게시한다."""
    base_dttm = now.replace(second=0, microsecond=0) - timedelta(minutes=5)
    first_weather_dttm = now.replace(minute=0, second=0, microsecond=0) + timedelta(
        hours=1
    )
    kst_today = now.astimezone(ZoneInfo("Asia/Seoul")).date()
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            INSERT INTO weather_grid (
                weather_grid_id,
                weather_grid_x_no,
                weather_grid_y_no
            ) VALUES ('60_127', 60, 127)
            """
        )
        connection.execute(
            """
            INSERT INTO dispatch_center (
                dispatch_center_id,
                dispatch_center_nm,
                dispatch_center_point,
                location_accuracy_cd,
                location_source_desc,
                location_verified_dt,
                is_active
            ) VALUES (
                'test_center',
                '테스트 센터',
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'verified_site',
                'API integration fixture',
                DATE '2026-08-20',
                true
            )
            """
        )
        _executemany(
            connection,
            """
            INSERT INTO station (
                sta_id,
                sta_nm,
                sta_addr,
                hold_cnt,
                sta_point,
                sta_point_source_cd,
                weather_grid_id,
                dispatch_center_id,
                master_base_dttm,
                last_seen_dttm,
                is_active
            ) VALUES (
                %(sta_id)s,
                %(sta_nm)s,
                %(sta_addr)s,
                %(hold_cnt)s,
                ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
                'bike_station_master',
                '60_127',
                'test_center',
                %(master_base_dttm)s,
                %(last_seen_dttm)s,
                true
            )
            """,
            [
                {
                    "sta_id": "ST-1",
                    "sta_nm": "신선 대여소",
                    "sta_addr": "서울시 테스트로 1",
                    "hold_cnt": 10,
                    "lon": 127.0,
                    "lat": 37.5,
                    "master_base_dttm": now - timedelta(days=1),
                    "last_seen_dttm": base_dttm,
                },
                {
                    "sta_id": "ST-2",
                    "sta_nm": "낡은 대여소",
                    "sta_addr": "서울시 테스트로 2",
                    "hold_cnt": 10,
                    "lon": 127.01,
                    "lat": 37.51,
                    "master_base_dttm": now - timedelta(days=1),
                    "last_seen_dttm": now - timedelta(minutes=11),
                },
            ],
        )
        _executemany(
            connection,
            """
            INSERT INTO station_stock (sta_id, base_dttm, parking_bike_tot_cnt)
            VALUES (%(sta_id)s, %(base_dttm)s, %(parking_bike_tot_cnt)s)
            """,
            [
                {
                    "sta_id": "ST-1",
                    "base_dttm": base_dttm,
                    "parking_bike_tot_cnt": 12,
                },
                {
                    "sta_id": "ST-2",
                    "base_dttm": now - timedelta(minutes=11),
                    "parking_bike_tot_cnt": 2,
                },
            ],
        )
        _executemany(
            connection,
            """
            INSERT INTO station_demand_forecast (
                base_dttm,
                sta_id,
                predicted_dttm,
                predicted_rent_cnt,
                predicted_rtn_cnt
            ) VALUES (
                %(base_dttm)s,
                'ST-1',
                %(predicted_dttm)s,
                %(predicted_rent_cnt)s,
                %(predicted_rtn_cnt)s
            )
            """,
            [
                {
                    "base_dttm": base_dttm,
                    "predicted_dttm": base_dttm + timedelta(hours=hour),
                    "predicted_rent_cnt": hour,
                    "predicted_rtn_cnt": hour + 1,
                }
                for hour in range(1, 13)
            ],
        )
        _executemany(
            connection,
            """
            INSERT INTO weather_forecast (
                weather_grid_id,
                forecast_dttm,
                source_product_cd,
                base_dttm,
                sky_condition_cd,
                precipitation_type_cd,
                temperature,
                precipitation_prob,
                precipitation_amount,
                humidity,
                wind_speed
            ) VALUES (
                '60_127',
                %(forecast_dttm)s,
                'short_term',
                %(base_dttm)s,
                'clear',
                'none',
                25.0,
                NULL,
                NULL,
                60.0,
                2.0
            )
            """,
            [
                {
                    "forecast_dttm": first_weather_dttm + timedelta(hours=offset),
                    "base_dttm": now - timedelta(hours=1),
                }
                for offset in range(12)
            ],
        )
        _executemany(
            connection,
            """
            INSERT INTO event (
                event_id,
                event_source_cd,
                source_event_id,
                event_name,
                event_spot_nm,
                event_point,
                event_point_source_cd,
                location_accuracy_cd,
                event_start_dt,
                event_end_dt,
                last_seen_dttm
            ) VALUES (
                %(event_id)s,
                'performance_event',
                %(source_event_id)s,
                %(event_name)s,
                %(event_spot_nm)s,
                ST_SetSRID(ST_MakePoint(%(lon)s, %(lat)s), 4326),
                'curated_osm_nominatim',
                'approximate',
                %(event_start_dt)s,
                %(event_end_dt)s,
                %(last_seen_dttm)s
            )
            """,
            [
                {
                    "event_id": "performance_event:near",
                    "source_event_id": "near",
                    "event_name": "가까운 행사",
                    "event_spot_nm": None,
                    "lon": 127.0001,
                    "lat": 37.5001,
                    "event_start_dt": kst_today,
                    "event_end_dt": kst_today,
                    "last_seen_dttm": now - queries.EVENT_FRESHNESS,
                },
                {
                    "event_id": "performance_event:far",
                    "source_event_id": "far",
                    "event_name": "먼 행사",
                    "event_spot_nm": "먼 곳",
                    "lon": 127.1,
                    "lat": 37.5,
                    "event_start_dt": kst_today,
                    "event_end_dt": kst_today,
                    "last_seen_dttm": now,
                },
                {
                    "event_id": "performance_event:stale",
                    "source_event_id": "stale",
                    "event_name": "낡은 행사",
                    "event_spot_nm": "가까운 곳",
                    "lon": 127.0002,
                    "lat": 37.5002,
                    "event_start_dt": kst_today,
                    "event_end_dt": kst_today,
                    "last_seen_dttm": now
                    - queries.EVENT_FRESHNESS
                    - timedelta(seconds=1),
                },
            ],
        )
        connection.execute(
            """
            INSERT INTO station_urgency (
                sta_id,
                base_dttm,
                urgency_score,
                critical_remaining_min,
                rebalance_need_type_cd
            ) VALUES ('ST-1', %(base_dttm)s, 80.0, 15, 'retrieval_needed')
            """,
            {"base_dttm": base_dttm},
        )
        _executemany(
            connection,
            """
            INSERT INTO rebalance_route (
                route_id,
                dispatch_center_id,
                route_status_cd,
                proposed_dttm
            ) VALUES (%(route_id)s, 'test_center', 'proposed', %(proposed_dttm)s)
            """,
            [
                {"route_id": ROUTE_ID, "proposed_dttm": base_dttm},
                {"route_id": CONFLICT_ROUTE_ID, "proposed_dttm": base_dttm},
            ],
        )
        _executemany(
            connection,
            """
            INSERT INTO rebalance_route_stop (
                route_id,
                visit_no,
                sta_id,
                route_action_type_cd,
                bike_cnt
            ) VALUES (%(route_id)s, 1, 'ST-1', 'pickup', 2)
            """,
            [{"route_id": ROUTE_ID}, {"route_id": CONFLICT_ROUTE_ID}],
        )
    return base_dttm


def test_serving_queries_use_real_postgis_and_fresh_projection(
    database_url: str,
) -> None:
    """Point alias·거리·freshness·exact horizon·route aggregate를 실제 DB에서 검증한다."""
    now = datetime.now(UTC)
    base_dttm = _seed_serving_fixture(database_url, now)

    stations = queries.fetch_stations(now)
    assert [station["sta_id"] for station in stations] == ["ST-1"]
    assert stations[0]["lat"] == pytest.approx(37.5)
    assert stations[0]["lon"] == pytest.approx(127.0)
    assert stations[0]["region"] == "테스트 센터"
    assert queries.fetch_station("ST-2", now) is None

    forecast = queries.fetch_forecast("ST-1", now)
    assert forecast.state is queries.ForecastState.READY
    assert forecast.base_dttm == base_dttm
    assert len(forecast.points) == 12
    assert forecast.points[0]["predicted_return_cnt"] == 2
    assert queries.fetch_status_base_dttm(now) == base_dttm

    events = queries.fetch_nearby_events("ST-1", now)
    assert events is not None
    assert [event["event_id"] for event in events] == ["performance_event:near"]
    assert events[0]["lat"] == pytest.approx(37.5001)
    assert events[0]["lon"] == pytest.approx(127.0001)
    assert 0 < events[0]["distance_km"] < 0.1

    weather = queries.fetch_weather("ST-1", now)
    assert weather.state is queries.WeatherState.READY
    assert len(weather.points) == 12
    assert weather.points[0]["forecast_dttm"].minute == 0

    assert queries.fetch_regions() == [
        {"region": "테스트 센터", "lat": 37.5, "lon": 127.0}
    ]
    alerts = queries.fetch_alerts(now)
    assert len(alerts) == 1
    assert alerts[0]["action_type"] == "retrieval_needed"
    assert alerts[0]["minutes_until_critical"] == 15

    routes = queries.fetch_routes(status="proposed", limit=1)
    assert len(routes) == 1
    assert routes[0]["route_id"] == str(ROUTE_ID)
    assert routes[0]["stops"][0]["visit_order"] == 1
    assert routes[0]["stops"][0]["lat"] == pytest.approx(37.5)
    assert queries.fetch_route(ROUTE_ID) == routes[0]


def test_route_lifecycle_and_constraint_error_mapping(database_url: str) -> None:
    """실제 DDL trigger의 lifecycle과 23514 constraint를 명시적 결과로 매핑한다."""
    now = datetime.now(UTC)
    _seed_serving_fixture(database_url, now)

    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            INSERT INTO rebalance_route (
                route_id, dispatch_center_id, route_status_cd, proposed_dttm
            ) VALUES (%(route_id)s, 'test_center', 'proposed', %(proposed_dttm)s)
            """,
            {"route_id": CANCEL_ROUTE_ID, "proposed_dttm": now},
        )
        connection.execute(
            """
            INSERT INTO rebalance_route_stop (
                route_id, visit_no, sta_id, route_action_type_cd, bike_cnt
            ) VALUES (%(route_id)s, 1, 'ST-1', 'pickup', 2)
            """,
            {"route_id": CANCEL_ROUTE_ID},
        )

    dispatched = queries.dispatch_route(ROUTE_ID, now)
    assert isinstance(dispatched, dict)
    assert dispatched["status"] == "dispatched"
    assert dispatched["stops"][0]["visit_order"] == 1

    completed = queries.complete_route(ROUTE_ID, now + timedelta(seconds=1))
    assert isinstance(completed, dict)
    assert completed["status"] == "completed"
    assert (
        queries.complete_route(ROUTE_ID, now + timedelta(seconds=2))
        is queries.RouteTransitionResult.WRONG_STATUS
    )

    cancelled = queries.dispatch_route(CANCEL_ROUTE_ID, now)
    assert isinstance(cancelled, dict)
    cancelled = queries.cancel_route(CANCEL_ROUTE_ID, now + timedelta(seconds=1))
    assert isinstance(cancelled, dict)
    assert cancelled["status"] == "cancelled"
    assert cancelled["cancelled_at"] == now + timedelta(seconds=1)

    with psycopg.connect(database_url) as connection:
        connection.execute("SET session_replication_role = replica")
        connection.execute(
            "UPDATE dispatch_center SET is_active = false WHERE dispatch_center_id = 'test_center'"
        )
        connection.execute("SET session_replication_role = origin")

    assert (
        queries.dispatch_route(
            CONFLICT_ROUTE_ID,
            now,
        )
        is queries.RouteTransitionResult.CONSTRAINT_CONFLICT
    )
    response = TestClient(main.app).post(f"/routes/{CONFLICT_ROUTE_ID}/dispatch")
    assert response.status_code == 409
    assert response.json()["detail"] == "route_transition_conflict"
