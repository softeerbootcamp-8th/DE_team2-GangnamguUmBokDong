"""queries.py 테스트: 대여소 조회(fetch_stations), 행사 거리 계산(_haversine_km, #102),
라우트 관련 함수(#110). 이 프로젝트에 TestClient/httpx 선례가 없어서 API 레벨 테스트
대신, main.py가 얇게 위임하는 실제 로직(404/409 판별, N+1 방지 그룹핑)을
core.db(fetch_all/fetch_one)를 monkeypatch로 대체해 DB 연결 없이 검증한다.
"""

from datetime import UTC, datetime, timedelta

import queries
from queries import _haversine_km

NOW = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)


def test_fetch_stations_does_not_require_forecasts(monkeypatch):
    """미래 예측이 없어도 대여소와 최신 재고를 조회한다."""
    expected = [{"sta_id": "ST-1", "parking_bike_tot_cnt": 3}]
    captured = {}

    def fake_fetch_all(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return expected

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert queries.fetch_stations() == expected
    assert "FROM stations s" in captured["query"]
    assert "FROM station_stock" in captured["query"]
    assert "forecast_points" not in captured["query"]
    assert captured["params"] is None


def test_fetch_alerts_excludes_stale_batches(monkeypatch):
    """station_urgency는 sta_id당 최신 1건만 upsert되므로(#124), 배치가 멈춘
    대여소의 낡은 값이 조회 시점에 걸러져야 한다."""
    captured = {}
    now = datetime(2026, 8, 19, 14, 5, tzinfo=UTC)
    stored = [
        {"sta_id": "A", "batch_run_at": now, "urgency_score": 90.0},
        {"sta_id": "B", "batch_run_at": now - timedelta(minutes=queries.ALERTS_FRESHNESS_WINDOW_MIN + 5), "urgency_score": 80.0},
    ]

    def fake_fetch_all(query, params=None):
        captured["query"] = query
        captured["params"] = params
        return [row for row in stored if row["batch_run_at"] >= params["cutoff"]]

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert [row["sta_id"] for row in queries.fetch_alerts(now)] == ["A"]
    assert captured["params"]["cutoff"] == now - timedelta(minutes=queries.ALERTS_FRESHNESS_WINDOW_MIN)
    normalized = " ".join(captured["query"].split()).upper()
    assert "WHERE U.BATCH_RUN_AT >= %(CUTOFF)S" in normalized


def test_same_point_is_zero_distance():
    assert _haversine_km(37.5, 127.0, 37.5, 127.0) == 0.0


def test_one_degree_latitude_matches_known_value():
    # 경도가 같을 때 위도 1도 차이는 지구 반지름(6371km) 기준 정확히 R*radians(1)이다.
    assert round(_haversine_km(37.0, 127.0, 38.0, 127.0), 6) == 111.194927


def test_symmetric_regardless_of_argument_order():
    a = _haversine_km(37.5, 127.0, 37.6, 127.1)
    b = _haversine_km(37.6, 127.1, 37.5, 127.0)
    assert a == b


class TestFetchStopsForRoutes:
    def test_empty_route_ids_returns_empty_without_query(self, monkeypatch):
        def fail(*_args, **_kwargs):
            raise AssertionError("route_ids가 비었으면 쿼리를 아예 날리면 안 된다")

        monkeypatch.setattr(queries, "fetch_all", fail)
        assert queries._fetch_stops_for_routes([]) == {}

    def test_groups_by_route_id(self, monkeypatch):
        rows = [
            {
                "route_id": "r1",
                "visit_order": 1,
                "sta_id": "101",
                "sta_nm": "역삼역",
                "lat": 1.0,
                "lon": 2.0,
                "action": "pickup",
                "bike_cnt": 8,
            },
            {
                "route_id": "r1",
                "visit_order": 2,
                "sta_id": "102",
                "sta_nm": "강남역",
                "lat": 3.0,
                "lon": 4.0,
                "action": "dropoff",
                "bike_cnt": 8,
            },
            {
                "route_id": "r2",
                "visit_order": 1,
                "sta_id": "103",
                "sta_nm": "선릉역",
                "lat": 5.0,
                "lon": 6.0,
                "action": "pickup",
                "bike_cnt": 5,
            },
        ]
        monkeypatch.setattr(queries, "fetch_all", lambda query, params: rows)

        result = queries._fetch_stops_for_routes(["r1", "r2"])

        assert result["r1"] == [
            {"visit_order": 1, "sta_id": "101", "sta_nm": "역삼역", "lat": 1.0, "lon": 2.0, "action": "pickup", "bike_cnt": 8},
            {"visit_order": 2, "sta_id": "102", "sta_nm": "강남역", "lat": 3.0, "lon": 4.0, "action": "dropoff", "bike_cnt": 8},
        ]
        assert result["r2"] == [
            {"visit_order": 1, "sta_id": "103", "sta_nm": "선릉역", "lat": 5.0, "lon": 6.0, "action": "pickup", "bike_cnt": 5},
        ]


class TestFetchRoutes:
    def test_builds_no_filter_when_region_and_status_omitted(self, monkeypatch):
        captured = {}

        def fake_fetch_all(query, params):
            captured["query"] = query
            captured["params"] = params
            return []

        monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

        queries.fetch_routes()

        assert "WHERE" not in captured["query"]
        assert captured["params"] == {"limit": 100, "offset": 0}

    def test_filters_by_region_and_status(self, monkeypatch):
        captured = {}

        def fake_fetch_all(query, params):
            captured["query"] = query
            captured["params"] = params
            return []

        monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

        queries.fetch_routes(region="세종로", status="proposed")

        assert "region = %(region)s" in captured["query"]
        assert "status = %(status)s" in captured["query"]
        assert captured["params"] == {"region": "세종로", "status": "proposed", "limit": 100, "offset": 0}

    def test_limit_and_offset_are_passed_to_the_query(self, monkeypatch):
        """compute_routes가 5분마다 여러 권역에 걸쳐 라우트를 새로 만들기 때문에
        (#114), limit/offset 없이 전부 반환하면 응답이 무한정 커질 수 있다."""
        captured = {}

        def fake_fetch_all(query, params):
            captured["query"] = query
            captured["params"] = params
            return []

        monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

        queries.fetch_routes(limit=20, offset=40)

        assert "LIMIT %(limit)s OFFSET %(offset)s" in captured["query"]
        assert captured["params"]["limit"] == 20
        assert captured["params"]["offset"] == 40

    def test_attaches_stops_to_each_route(self, monkeypatch):
        routes = [{"route_id": "r1", "region": "세종로", "status": "proposed"}]
        monkeypatch.setattr(queries, "fetch_all", lambda query, params: routes)
        monkeypatch.setattr(queries, "_fetch_stops_for_routes", lambda route_ids: {"r1": ["stub"]})

        result = queries.fetch_routes()

        assert result[0]["stops"] == ["stub"]


class TestFetchRoute:
    def test_returns_none_when_not_found(self, monkeypatch):
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: None)
        assert queries.fetch_route("missing") is None

    def test_attaches_stops_when_found(self, monkeypatch):
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: {"route_id": "r1", "region": "세종로"})
        monkeypatch.setattr(queries, "_fetch_stops_for_routes", lambda route_ids: {"r1": ["stub"]})

        result = queries.fetch_route("r1")

        assert result["stops"] == ["stub"]


class TestDispatchRoute:
    """dispatch_route/complete_route는 UPDATE ... RETURNING으로 전이된 행을 그
    자리에서 받는다 — UPDATE와 조회를 분리하면(별도 fetch_route 호출) 그 사이에
    다른 요청이 상태를 또 바꿔서 응답이 실제로 일어난 일과 달라질 수 있다."""

    def test_returns_the_updated_route_with_stops_when_update_succeeds(self, monkeypatch):
        monkeypatch.setattr(
            queries, "fetch_one", lambda query, params: {"route_id": "r1", "status": "dispatched"}
        )
        monkeypatch.setattr(queries, "_fetch_stops_for_routes", lambda route_ids: {"r1": ["stub"]})

        result = queries.dispatch_route("r1", NOW)

        assert result == {"route_id": "r1", "status": "dispatched", "stops": ["stub"]}

    def test_returns_not_found_when_route_does_not_exist(self, monkeypatch):
        # UPDATE...RETURNING이 매치 없이 None을 반환 -> fetch_route로 원인 판별.
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: None)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: None)
        assert queries.dispatch_route("missing", NOW) == "not_found"

    def test_returns_wrong_status_when_route_exists_but_not_proposed(self, monkeypatch):
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: None)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: {"route_id": route_id, "status": "dispatched"})
        assert queries.dispatch_route("r1", NOW) == "wrong_status"


class TestCompleteRoute:
    def test_returns_the_updated_route_with_stops_when_update_succeeds(self, monkeypatch):
        monkeypatch.setattr(
            queries, "fetch_one", lambda query, params: {"route_id": "r1", "status": "completed"}
        )
        monkeypatch.setattr(queries, "_fetch_stops_for_routes", lambda route_ids: {"r1": ["stub"]})

        result = queries.complete_route("r1", NOW)

        assert result == {"route_id": "r1", "status": "completed", "stops": ["stub"]}

    def test_returns_not_found_when_route_does_not_exist(self, monkeypatch):
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: None)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: None)
        assert queries.complete_route("missing", NOW) == "not_found"

    def test_returns_wrong_status_when_route_exists_but_not_dispatched(self, monkeypatch):
        monkeypatch.setattr(queries, "fetch_one", lambda query, params: None)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: {"route_id": route_id, "status": "proposed"})
        assert queries.complete_route("r1", NOW) == "wrong_status"
