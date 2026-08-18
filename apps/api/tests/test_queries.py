"""queries.py 테스트: 대여소 조회(fetch_stations), 행사 거리 계산(_haversine_km, #102)."""

import queries
from queries import _haversine_km


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


def test_same_point_is_zero_distance():
    assert _haversine_km(37.5, 127.0, 37.5, 127.0) == 0.0


def test_one_degree_latitude_matches_known_value():
    # 경도가 같을 때 위도 1도 차이는 지구 반지름(6371km) 기준 정확히 R*radians(1)이다.
    assert round(_haversine_km(37.0, 127.0, 38.0, 127.0), 6) == 111.194927


def test_symmetric_regardless_of_argument_order():
    a = _haversine_km(37.5, 127.0, 37.6, 127.1)
    b = _haversine_km(37.6, 127.1, 37.5, 127.0)
    assert a == b
