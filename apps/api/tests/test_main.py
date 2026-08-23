"""Gold PostGIS API의 HTTP 상태와 외부 alias 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, date, datetime, timedelta
from uuid import UUID

import main
import pytest
import queries
from fastapi.testclient import TestClient

NOW = datetime(2026, 8, 20, 1, 5, tzinfo=UTC)
BASE = datetime(2026, 8, 20, 1, 0, tzinfo=UTC)
ROUTE_ID = UUID("11111111-1111-4111-8111-111111111111")


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    """현재시각을 고정한 FastAPI test client를 반환한다."""
    monkeypatch.setattr(queries, "now_utc", lambda: NOW)
    return TestClient(main.app)


def _station_row() -> dict:
    """station API의 정상 응답 fixture를 만든다."""
    return {
        "sta_id": "ST-1",
        "sta_nm": "대여소",
        "sta_addr": "서울시 테스트로 1",
        "lat": 37.5,
        "lon": 127.0,
        "hold_cnt": 10,
        "parking_bike_tot_cnt": 12,
        "region": "테스트 센터",
        "base_dttm": BASE,
    }


def _forecast_result(state: queries.ForecastState) -> queries.ForecastResult:
    """지정한 상태의 forecast 결과 fixture를 만든다."""
    if state is not queries.ForecastState.READY:
        return queries.ForecastResult(state)
    points = tuple(
        {
            "predicted_dttm": BASE + timedelta(hours=hour),
            "predicted_rent_cnt": 2,
            "predicted_return_cnt": 1,
        }
        for hour in range(1, 13)
    )
    return queries.ForecastResult(
        state,
        {
            "sta_id": "ST-1",
            "hold_cnt": 10,
            "last_seen_dttm": BASE,
            "parking_bike_tot_cnt": 4,
        },
        BASE,
        points,
    )


def _weather_points() -> tuple[dict, ...]:
    """weather API의 미래 12개 정시 fixture를 만든다."""
    return tuple(
        {
            "forecast_dttm": BASE + timedelta(hours=hour),
            "temperature": 25.0,
            "sky_condition_cd": "clear",
            "precipitation_type_cd": "none",
            "precipitation_prob": None,
            "precipitation_amount": None,
            "humidity": 60.0,
            "wind_speed": 2.0,
        }
        for hour in range(1, 13)
    )


def _route(status: str = "proposed") -> dict:
    """route API 정상 응답 fixture를 만든다."""
    return {
        "route_id": str(ROUTE_ID),
        "region": "테스트 센터",
        "status": status,
        "proposed_at": BASE,
        "dispatched_at": NOW if status in {"dispatched", "completed", "cancelled"} else None,
        "completed_at": NOW if status == "completed" else None,
        "cancelled_at": NOW if status == "cancelled" else None,
        "dismissed_at": None,
        "restored_from_route_id": None,
        "stops": [
            {
                "visit_order": 1,
                "sta_id": "ST-1",
                "sta_nm": "대여소",
                "lat": 37.5,
                "lon": 127.0,
                "action": "pickup",
                "bike_cnt": 2,
            }
        ],
    }


def test_stations_preserve_aliases_without_gu(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """station 응답은 Point alias와 region을 유지하고 gu를 노출하지 않는다."""
    row = {**_station_row(), "gu": "남겨서는 안 됨"}
    monkeypatch.setattr(queries, "fetch_stations", lambda _now: [row])

    response = client.get("/stations")

    assert response.status_code == 200
    assert response.json() == [
        {
            "sta_id": "ST-1",
            "sta_nm": "대여소",
            "lat": 37.5,
            "lon": 127.0,
            "hold_cnt": 10,
            "parking_bike_tot_cnt": 12,
            "shared_rate": 1.2,
            "region": "테스트 센터",
            "base_dttm": "2026-08-20T01:00:00Z",
        }
    ]
    assert "gu" not in response.json()[0]


def test_station_detail_returns_404_when_not_servable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """missing·inactive·stale stock은 query에서 모두 제외되어 상세 404가 된다."""
    monkeypatch.setattr(queries, "fetch_station", lambda _sta_id, _now: None)

    response = client.get("/stations/ST-1")

    assert response.status_code == 404


@pytest.mark.parametrize(
    "state,status_code,detail",
    [
        (queries.ForecastState.STATION_NOT_FOUND, 404, "station ST-1 not found"),
        (queries.ForecastState.FORECAST_NOT_AVAILABLE, 404, "forecast_not_available"),
        (queries.ForecastState.FORECAST_NOT_READY, 503, "forecast_not_ready"),
        (queries.ForecastState.STOCK_NOT_ALIGNED, 503, "stock_forecast_not_aligned"),
    ],
)
def test_forecast_maps_contract_states(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    state: queries.ForecastState,
    status_code: int,
    detail: str,
) -> None:
    """forecast의 station/model/freshness 상태를 404와 503으로 구분한다."""
    monkeypatch.setattr(
        queries, "fetch_forecast", lambda _sta_id, _now: _forecast_result(state)
    )

    response = client.get("/stations/ST-1/forecast")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail


def test_forecast_returns_twelve_points_without_reasons(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """forecast 성공 응답은 기존 alias를 유지하고 빈 reasons 계약을 제거한다."""
    monkeypatch.setattr(
        queries,
        "fetch_forecast",
        lambda _sta_id, _now: _forecast_result(queries.ForecastState.READY),
    )

    response = client.get("/stations/ST-1/forecast")

    assert response.status_code == 200
    payload = response.json()
    assert payload["base_dttm"] == "2026-08-20T01:00:00Z"
    assert len(payload["points"]) == 12
    assert payload["points"][0]["predicted_return_cnt"] == 1
    assert payload["points"][0]["predicted_bikes"] == 3
    assert "reasons" not in payload


def test_status_returns_503_without_real_fresh_projection(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """status는 demand가 없을 때 현재시각 대신 forecast_not_ready를 반환한다."""
    monkeypatch.setattr(queries, "fetch_status_base_dttm", lambda _now: None)

    response = client.get("/status")

    assert response.status_code == 503
    assert response.json()["detail"] == "forecast_not_ready"


def test_events_remove_unused_fields_and_keep_radius(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """event 응답은 검색 반경과 소비 alias만 제공한다."""
    event = {
        "event_id": "performance_event:1",
        "title": "행사",
        "place": None,
        "start_date": date(2026, 8, 20),
        "end_date": date(2026, 8, 21),
        "lat": 37.51,
        "lon": 127.01,
        "distance_km": 1.23,
        "category": "제거 대상",
        "is_free": "제거 대상",
    }
    monkeypatch.setattr(queries, "fetch_nearby_events", lambda _sta_id, _now: [event])

    response = client.get("/stations/ST-1/events")

    assert response.status_code == 200
    payload = response.json()
    assert payload["radius_km"] == queries.NEARBY_EVENT_RADIUS_KM
    assert payload["events"][0]["start_date"] == "2026-08-20"
    assert "category" not in payload["events"][0]
    assert "is_free" not in payload["events"][0]


@pytest.mark.parametrize(
    "state,status_code,detail",
    [
        (queries.WeatherState.STATION_NOT_FOUND, 404, "station ST-1 not found"),
        (queries.WeatherState.WEATHER_NOT_READY, 503, "weather_not_ready"),
    ],
)
def test_weather_maps_missing_and_not_ready_states(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    state: queries.WeatherState,
    status_code: int,
    detail: str,
) -> None:
    """weather는 station 404와 projection 503을 구분한다."""
    monkeypatch.setattr(
        queries,
        "fetch_weather",
        lambda _sta_id, _now, _hours: queries.WeatherResult(state),
    )

    response = client.get("/stations/ST-1/weather?hours=12")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail


def test_weather_returns_exact_points_and_rejects_other_hours(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """weather 외부 계약은 hours=12만 허용하고 정확히 12행을 반환한다."""
    monkeypatch.setattr(
        queries,
        "fetch_weather",
        lambda _sta_id, _now, _hours: queries.WeatherResult(
            queries.WeatherState.READY,
            _weather_points(),
        ),
    )

    response = client.get("/stations/ST-1/weather?hours=12")

    assert response.status_code == 200
    assert len(response.json()["points"]) == 12
    assert response.json()["points"][0]["precipitation_prob"] is None
    assert client.get("/stations/ST-1/weather?hours=11").status_code == 422


def test_route_query_parameters_and_uuid_are_validated_before_db(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route status·pagination·UUID 오류는 DB cast 전에 422로 거부한다."""
    monkeypatch.setattr(queries, "fetch_routes", lambda *_args: [])
    monkeypatch.setattr(queries, "fetch_route", lambda _route_id: None)

    assert client.get("/routes?status=invalid").status_code == 422
    assert client.get("/routes?status=cancelled").status_code == 200
    assert client.get("/routes?limit=0").status_code == 422
    assert client.get("/routes?limit=501").status_code == 422
    assert client.get("/routes?offset=-1").status_code == 422
    assert client.get("/routes/not-a-uuid").status_code == 422


@pytest.mark.parametrize(
    "result,status_code,detail",
    [
        (queries.RouteTransitionResult.NOT_FOUND, 404, f"route {ROUTE_ID} not found"),
        (
            queries.RouteTransitionResult.WRONG_STATUS,
            409,
            f"route {ROUTE_ID} is not in proposed status",
        ),
        (
            queries.RouteTransitionResult.CONSTRAINT_CONFLICT,
            409,
            "route_transition_conflict",
        ),
        (
            queries.RouteTransitionResult.STATION_CONFLICT,
            409,
            "route_station_conflict",
        ),
    ],
)
def test_dispatch_maps_not_found_state_and_constraints(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    result: queries.RouteTransitionResult,
    status_code: int,
    detail: str,
) -> None:
    """dispatch는 404·상태 409·DB constraint 409를 명시적으로 매핑한다."""
    monkeypatch.setattr(queries, "dispatch_route", lambda _route_id, _now: result)

    response = client.post(f"/routes/{ROUTE_ID}/dispatch")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail


def test_cancel_returns_cancelled_route(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """cancel은 dispatched route를 cancelled 응답으로 전이한다."""
    monkeypatch.setattr(queries, "cancel_route", lambda _route_id, _now: _route("cancelled"))

    response = client.post(f"/routes/{ROUTE_ID}/cancel")

    assert response.status_code == 200
    assert response.json()["status"] == "cancelled"
    assert response.json()["cancelled_at"] == "2026-08-20T01:05:00Z"


def test_route_response_preserves_external_aliases(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route UUID와 표준 DB 컬럼은 기존 JSON alias로 직렬화된다."""
    monkeypatch.setattr(queries, "fetch_route", lambda _route_id: _route())

    response = client.get(f"/routes/{ROUTE_ID}")

    assert response.status_code == 200
    payload = response.json()
    assert payload["route_id"] == str(ROUTE_ID)
    assert payload["status"] == "proposed"
    assert payload["proposed_at"] == "2026-08-20T01:00:00Z"
    assert payload["stops"][0]["visit_order"] == 1
    assert payload["stops"][0]["action"] == "pickup"


def _unreachable_db(*_args, **_kwargs):
    """DB 연결 실패를 흉내낸다."""
    raise RuntimeError("database is down")


def test_healthz_stays_ok_while_database_is_down(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """생존 신호는 DB를 조회하지 않는다.

    조회하면 RDS 순단이 컨테이너 재시작 루프로 번진다.
    """
    monkeypatch.setattr(main, "fetch_one", _unreachable_db)

    response = client.get("/healthz")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_readyz_ok_when_database_reachable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DB에 연결되면 준비 완료를 반환한다."""
    monkeypatch.setattr(main, "fetch_one", lambda *_args, **_kwargs: {"ok": 1})

    response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json() == {"status": "ready"}


def test_readyz_503_when_database_unreachable(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """DB에 연결할 수 없으면 503과 명시적 사유를 반환한다."""
    monkeypatch.setattr(main, "fetch_one", _unreachable_db)

    response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["detail"] == "database_unavailable"


def test_route_response_exposes_dismiss_and_restore_fields(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """route 응답은 삭제 시각과 복제 원본을 그대로 노출한다."""
    route = _route("cancelled")
    route["dismissed_at"] = NOW
    route["restored_from_route_id"] = "44444444-4444-4444-8444-444444444444"
    monkeypatch.setattr(queries, "fetch_route", lambda _route_id: route)

    response = client.get(f"/routes/{ROUTE_ID}")

    assert response.status_code == 200
    assert response.json()["dismissed_at"] == "2026-08-20T01:05:00Z"
    assert response.json()["restored_from_route_id"] == "44444444-4444-4444-8444-444444444444"


def test_dismiss_returns_dismissed_route(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """dismiss는 종료된 작업에 삭제 시각을 채운 응답을 돌려준다."""
    route = _route("completed")
    route["dismissed_at"] = NOW
    monkeypatch.setattr(queries, "dismiss_route", lambda _route_id, _now: route)

    response = client.post(f"/routes/{ROUTE_ID}/dismiss")

    assert response.status_code == 200
    assert response.json()["dismissed_at"] == "2026-08-20T01:05:00Z"


@pytest.mark.parametrize(
    "result,status_code,detail",
    [
        (queries.RouteTransitionResult.NOT_FOUND, 404, f"route {ROUTE_ID} not found"),
        (
            queries.RouteTransitionResult.WRONG_STATUS,
            409,
            f"route {ROUTE_ID} is not in completed or cancelled status",
        ),
        (
            queries.RouteTransitionResult.ALREADY_DISMISSED,
            409,
            f"route {ROUTE_ID} is already dismissed",
        ),
    ],
)
def test_dismiss_maps_not_found_wrong_status_and_duplicate(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    result: queries.RouteTransitionResult,
    status_code: int,
    detail: str,
) -> None:
    """dismiss는 404·상태 409·중복 409를 명시적으로 매핑한다."""
    monkeypatch.setattr(queries, "dismiss_route", lambda _route_id, _now: result)

    response = client.post(f"/routes/{ROUTE_ID}/dismiss")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail


def test_restore_returns_new_proposed_route(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """restore가 실제로 복제했으면 새 후보를 201로 돌려준다."""
    restored = _route("proposed")
    restored["restored_from_route_id"] = str(ROUTE_ID)

    def _restore(_route_id: UUID, _now: datetime, new_route_id: UUID) -> dict:
        return {**restored, "route_id": str(new_route_id)}

    monkeypatch.setattr(queries, "restore_route", _restore)

    response = client.post(f"/routes/{ROUTE_ID}/restore")

    assert response.status_code == 201
    assert response.json()["route_id"] != str(ROUTE_ID)
    assert response.json()["status"] == "proposed"
    assert response.json()["restored_from_route_id"] == str(ROUTE_ID)


def test_restore_returns_existing_candidate_with_200(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """이미 대기 중인 후보를 재사용했으면 201이 아니라 200으로 알린다."""
    reused = _route("proposed")
    reused["route_id"] = "55555555-5555-4555-8555-555555555555"
    reused["restored_from_route_id"] = str(ROUTE_ID)
    monkeypatch.setattr(
        queries,
        "restore_route",
        lambda _route_id, _now, _new_route_id: reused,
    )

    first = client.post(f"/routes/{ROUTE_ID}/restore")
    second = client.post(f"/routes/{ROUTE_ID}/restore")

    assert first.status_code == 200
    assert second.status_code == 200
    assert first.json() == second.json()
    assert first.json()["route_id"] == "55555555-5555-4555-8555-555555555555"


@pytest.mark.parametrize(
    "result,status_code,detail",
    [
        (queries.RouteTransitionResult.NOT_FOUND, 404, f"route {ROUTE_ID} not found"),
        (
            queries.RouteTransitionResult.WRONG_STATUS,
            409,
            f"route {ROUTE_ID} is not in cancelled status",
        ),
        (
            queries.RouteTransitionResult.ALREADY_DISMISSED,
            409,
            f"route {ROUTE_ID} is already dismissed",
        ),
        (
            queries.RouteTransitionResult.CONSTRAINT_CONFLICT,
            409,
            "route_restore_conflict",
        ),
    ],
)
def test_restore_maps_not_found_wrong_status_and_conflicts(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    result: queries.RouteTransitionResult,
    status_code: int,
    detail: str,
) -> None:
    """restore는 404·상태 409·삭제 409·constraint 409를 명시적으로 매핑한다."""
    monkeypatch.setattr(
        queries,
        "restore_route",
        lambda _route_id, _now, _new_route_id: result,
    )

    response = client.post(f"/routes/{ROUTE_ID}/restore")

    assert response.status_code == status_code
    assert response.json()["detail"] == detail
