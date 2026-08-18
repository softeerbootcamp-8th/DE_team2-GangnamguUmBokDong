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


def test_expire_col_set_for_sliding_window_and_expiry_tables():
    """지난 시각/만료된 행이 계속 쌓이는 테이블(#116/#117)만 expire_col이 있어야 한다."""
    assert TABLE_SPECS["weather_forecast"].expire_col == "forecast_dttm"
    assert TABLE_SPECS["weather_forecast_ultra"].expire_col == "forecast_dttm"
    assert TABLE_SPECS["forecast_points"].expire_col == "predicted_dttm"
    assert TABLE_SPECS["cultural_events"].expire_col == "end_date"
    assert TABLE_SPECS["cultural_events_performance"].expire_col == "end_date"


def test_expire_col_absent_for_master_and_latest_only_tables():
    """마스터 데이터(stations)나 최신 1건만 유지하는 테이블(station_stock,
    weather_current)은 정리 대상이 아니다."""
    assert TABLE_SPECS["stations"].expire_col is None
    assert TABLE_SPECS["station_stock"].expire_col is None
    assert TABLE_SPECS["weather_current"].expire_col is None


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
