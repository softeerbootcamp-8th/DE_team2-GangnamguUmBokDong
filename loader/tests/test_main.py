"""loader CLI의 테이블별 적재 전 계약을 검증한다."""

from main import _only_known_stations


def test_station_urgency_filters_rows_without_station_fk():
    rows = [
        {"sta_id": "A", "urgency_score": 10.0},
        {"sta_id": "OUTSIDE", "urgency_score": 20.0},
    ]

    assert _only_known_stations(rows, {"A"}) == [{"sta_id": "A", "urgency_score": 10.0}]
