import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import queries


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
