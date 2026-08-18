"""TABLE_SPECS 레지스트리, 특히 신규 추가된 forecast_points 항목을 검증한다."""

from datetime import UTC, datetime

import pandas as pd
import pyarrow as pa

from config import TABLE_SPECS


def test_forecast_points_table_spec_registered():
    spec = TABLE_SPECS["forecast_points"]

    assert spec.conflict_cols == ["sta_id", "predicted_dttm"]
    assert spec.update_cols == ["predicted_rent_cnt", "predicted_return_cnt", "batch_run_at"]
    assert spec.reader is not None


def test_forecast_points_reader_delegates_to_read_predictions(monkeypatch):
    """forecast_points는 다른 테이블과 달리 read_silver(source_id, ...)가 아니라
    별도 키 컨벤션(read_predictions)으로 읽혀야 한다."""
    captured = {}

    def fake_read_predictions(window_start):
        captured["window_start"] = window_start
        return pa.table({"station_id": ["101"]})

    monkeypatch.setattr("config.reader.read_predictions", fake_read_predictions)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["forecast_points"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["station_id"].tolist() == ["101"]


def test_station_urgency_table_spec_registered():
    spec = TABLE_SPECS["station_urgency"]

    assert spec.conflict_cols == ["sta_id"]
    assert spec.update_cols == ["urgency_score", "minutes_until_critical", "action_type", "bike_qty", "batch_run_at"]
    assert spec.reader is not None


def test_station_urgency_reader_delegates_to_read_urgency(monkeypatch):
    """station_urgency도 forecast_points와 마찬가지로 read_silver(source_id, ...)가
    아니라 별도 키 컨벤션(read_urgency)으로 읽혀야 한다."""
    captured = {}

    def fake_read_urgency(window_start):
        captured["window_start"] = window_start
        return pa.table({"sta_id": ["101"]})

    monkeypatch.setattr("config.reader.read_urgency", fake_read_urgency)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["station_urgency"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["sta_id"].tolist() == ["101"]


def test_rebalance_routes_table_spec_registered():
    spec = TABLE_SPECS["rebalance_routes"]

    assert spec.conflict_cols == ["route_id"]
    assert spec.update_cols == []  # 운영자가 바꾼 status를 배치 재실행이 덮어쓰지 않도록 DO NOTHING
    assert spec.reader is not None


def test_rebalance_routes_reader_delegates_to_read_routes(monkeypatch):
    captured = {}

    def fake_read_routes(window_start):
        captured["window_start"] = window_start
        return pa.table({"route_id": ["r1"]})

    monkeypatch.setattr("config.reader.read_routes", fake_read_routes)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["rebalance_routes"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["route_id"].tolist() == ["r1"]


def test_rebalance_route_stops_table_spec_registered():
    spec = TABLE_SPECS["rebalance_route_stops"]

    assert spec.conflict_cols == ["route_id", "visit_order"]
    assert spec.update_cols == []
    assert spec.reader is not None


def test_rebalance_route_stops_reader_delegates_to_read_route_stops(monkeypatch):
    captured = {}

    def fake_read_route_stops(window_start):
        captured["window_start"] = window_start
        return pa.table({"route_id": ["r1"]})

    monkeypatch.setattr("config.reader.read_route_stops", fake_read_route_stops)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["rebalance_route_stops"].read(window_start)

    assert captured["window_start"] == window_start
    assert isinstance(result, pd.DataFrame)
    assert result["route_id"].tolist() == ["r1"]


def test_default_reader_still_uses_read_silver(monkeypatch):
    """reader를 지정하지 않은 기존 테이블(예: stations)은 여전히 read_silver를 쓴다."""
    captured = {}

    def fake_read_silver(source_id, window_start):
        captured["source_id"] = source_id
        captured["window_start"] = window_start
        return pa.table({"stationId": ["101"]})

    monkeypatch.setattr("config.reader.read_silver", fake_read_silver)

    window_start = datetime(2026, 8, 16, 14, 5, tzinfo=UTC)
    result = TABLE_SPECS["stations"].read(window_start)

    assert captured["source_id"] == "bike_station_realtime"
    assert isinstance(result, pd.DataFrame)
