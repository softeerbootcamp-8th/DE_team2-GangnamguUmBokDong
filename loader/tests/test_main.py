"""loader CLI의 테이블별 적재 전 계약을 검증한다."""

from main import _only_known_stations, _retire_stale_proposed_routes


def test_station_urgency_filters_rows_without_station_fk():
    rows = [
        {"sta_id": "A", "urgency_score": 10.0},
        {"sta_id": "OUTSIDE", "urgency_score": 20.0},
    ]

    assert _only_known_stations(rows, {"A"}) == [{"sta_id": "A", "urgency_score": 10.0}]


class _FakeCursor:
    def __init__(self):
        self.executed: list[str] = []

    def execute(self, query, params=None):
        self.executed.append(" ".join(query.split()))

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class _FakeConnection:
    def __init__(self):
        self.cursor_obj = _FakeCursor()

    def cursor(self):
        return self.cursor_obj


def test_retire_stale_proposed_routes_deletes_stops_before_routes():
    """rebalance_route_stops.route_id가 rebalance_routes.route_id를 FK 참조하므로,
    자식(stops)을 먼저 지우지 않으면 부모(routes) 삭제가 FK 위반으로 실패한다."""
    conn = _FakeConnection()

    _retire_stale_proposed_routes(conn)

    [stops_delete, routes_delete] = conn.cursor_obj.executed
    assert "DELETE FROM rebalance_route_stops" in stops_delete
    assert "status = 'proposed'" in stops_delete
    assert "DELETE FROM rebalance_routes WHERE status = 'proposed'" in routes_delete
