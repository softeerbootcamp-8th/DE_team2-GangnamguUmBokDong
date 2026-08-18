"""queries.py의 라우트 관련 함수 테스트. 이 프로젝트에 TestClient/httpx 선례가
없어서 API 레벨 테스트 대신, main.py가 얇게 위임하는 실제 로직(404/409 판별,
N+1 방지 그룹핑)을 core.db(fetch_all/fetch_one/execute)를 monkeypatch로 대체해
DB 연결 없이 검증한다.
"""

from datetime import UTC, datetime

import queries

NOW = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)


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
        assert captured["params"] == {}

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
        assert captured["params"] == {"region": "세종로", "status": "proposed"}

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
    def test_returns_dispatched_when_update_succeeds(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 1)
        assert queries.dispatch_route("r1", NOW) == "dispatched"

    def test_returns_not_found_when_route_does_not_exist(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 0)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: None)
        assert queries.dispatch_route("missing", NOW) == "not_found"

    def test_returns_wrong_status_when_route_exists_but_not_proposed(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 0)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: {"route_id": route_id, "status": "dispatched"})
        assert queries.dispatch_route("r1", NOW) == "wrong_status"


class TestCompleteRoute:
    def test_returns_completed_when_update_succeeds(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 1)
        assert queries.complete_route("r1", NOW) == "completed"

    def test_returns_not_found_when_route_does_not_exist(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 0)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: None)
        assert queries.complete_route("missing", NOW) == "not_found"

    def test_returns_wrong_status_when_route_exists_but_not_dispatched(self, monkeypatch):
        monkeypatch.setattr(queries, "execute", lambda query, params: 0)
        monkeypatch.setattr(queries, "fetch_route", lambda route_id: {"route_id": route_id, "status": "proposed"})
        assert queries.complete_route("r1", NOW) == "wrong_status"
