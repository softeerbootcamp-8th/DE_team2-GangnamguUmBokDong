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


def test_fetch_alerts_reads_only_latest_batch(monkeypatch):
    """이전 batch에만 있던 station이 최신 snapshot 조회에 섞이지 않는다."""
    captured = {}
    stored = [
        {"sta_id": "A", "batch_run_at": "14:00", "urgency_score": 70.0},
        {"sta_id": "B", "batch_run_at": "14:00", "urgency_score": 60.0},
        {"sta_id": "C", "batch_run_at": "14:00", "urgency_score": 50.0},
        {"sta_id": "A", "batch_run_at": "14:05", "urgency_score": 90.0},
        {"sta_id": "B", "batch_run_at": "14:05", "urgency_score": 80.0},
    ]

    def fake_fetch_all(query, params=None):
        captured["query"] = query
        latest_batch = max(row["batch_run_at"] for row in stored)
        return [row for row in stored if row["batch_run_at"] == latest_batch]

    monkeypatch.setattr(queries, "fetch_all", fake_fetch_all)

    assert [row["sta_id"] for row in queries.fetch_alerts()] == ["A", "B"]
    normalized = " ".join(captured["query"].split()).upper()
    assert "WHERE U.BATCH_RUN_AT = ( SELECT MAX(BATCH_RUN_AT) FROM STATION_URGENCY )" in normalized


def test_same_point_is_zero_distance():
    assert _haversine_km(37.5, 127.0, 37.5, 127.0) == 0.0


def test_one_degree_latitude_matches_known_value():
    # 경도가 같을 때 위도 1도 차이는 지구 반지름(6371km) 기준 정확히 R*radians(1)이다.
    assert round(_haversine_km(37.0, 127.0, 38.0, 127.0), 6) == 111.194927


def test_symmetric_regardless_of_argument_order():
    a = _haversine_km(37.5, 127.0, 37.6, 127.1)
    b = _haversine_km(37.6, 127.1, 37.5, 127.0)
    assert a == b
