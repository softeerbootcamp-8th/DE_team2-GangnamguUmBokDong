"""Normalizer Silver와 predict_single.py 인구 입력 계약을 검증한다."""

import pandas as pd
import pytest
from ml_core import silver_schema

from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _reset_population_cache():
    ps._recent_population_by_ts = {}
    yield
    ps._recent_population_by_ts = {}


def _normalized_row(spop: float) -> pd.DataFrame:
    return pd.DataFrame({"CELL_ID": ["다사53815262"], "SPOP": [spop]})


def test_recent_population_reads_normalized_source_not_raw(monkeypatch):
    requested_keys = []

    def fake_read_many(keys, columns=None):
        requested_keys.extend(keys)
        return [None] * len(keys)

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    ps._get_recent_population(pd.Timestamp("2026-08-17 14:12"))

    assert requested_keys
    assert all(f"silver/{silver_schema.NORMALIZED_POPULATION_SOURCE_ID}/" in key for key in requested_keys)
    assert all(silver_schema.POPULATION_SOURCE_ID not in key for key in requested_keys)


def test_recent_population_reads_exact_target_tick(monkeypatch):
    target_ts = pd.Timestamp("2026-08-17 14:10")

    def fake_read_many(keys, columns=None):
        assert keys[-1] == silver_schema.silver_key(silver_schema.NORMALIZED_POPULATION_SOURCE_ID, target_ts)
        return [None] * (len(keys) - 1) + [_normalized_row(3210)]

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    result = ps._get_recent_population(target_ts)

    assert result.loc["다사53815262"].to_dict() == {
        "pop_resd": 3210.0,
        "pop_long_foreign": 0.0,
        "pop_short_foreign": 0.0,
        "pop_total": 3210.0,
    }


def test_recent_population_reads_latest_previous_tick(monkeypatch):
    target_ts = pd.Timestamp("2026-08-17 14:12")

    def fake_read_many(keys, columns=None):
        values = [None] * len(keys)
        values[-2] = _normalized_row(3000)
        return values

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    result = ps._get_recent_population(target_ts)

    assert result.loc["다사53815262", "pop_total"] == 3000


def test_recent_population_never_falls_back_to_raw_population(monkeypatch):
    """보정 결과가 없으면 원본 Silver로 우회하지 않고 빈 값을 반환한다."""
    requested_keys = []

    def fake_read_many(keys, columns=None):
        requested_keys.extend(keys)
        return [None] * len(keys)

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    result = ps._get_recent_population(pd.Timestamp("2026-08-17 14:12"))

    assert result.empty
    assert len(requested_keys) == 13
    assert requested_keys[0].endswith("/1310.parquet")
    assert requested_keys[-1].endswith("/1410.parquet")


def test_recent_population_caches_by_target_ts(monkeypatch):
    call_count = 0

    def fake_read_many(keys, columns=None):
        nonlocal call_count
        call_count += 1
        return [None] * (len(keys) - 1) + [_normalized_row(3210)]

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    target_ts = pd.Timestamp("2026-08-17 14:10")

    ps._get_recent_population(target_ts)
    ps._get_recent_population(target_ts)

    assert call_count == 1


def test_recent_population_never_requests_future_tick(monkeypatch):
    requested_keys = []

    def fake_read_many(keys, columns=None):
        requested_keys.extend(keys)
        return [None] * len(keys)

    monkeypatch.setattr(ps.s3_io, "read_parquet_many", fake_read_many)
    ps._get_recent_population(pd.Timestamp("2026-08-17 14:12"))

    assert requested_keys[-1].endswith("/1410.parquet")
    assert not any(key.endswith("/1415.parquet") for key in requested_keys)


def test_station_master_uses_latest_daily_master_and_realtime_capacity(monkeypatch):
    """CELL_ID 보강 master에 최신 실시간 이름과 capacity를 보강한다."""
    master = pd.DataFrame(
        [{
            "station_id": "ST-10", "station_no": "427", "station_name": "서울시 마포구",
            "capacity": 10, "lat": 37.55, "lon": 126.91, "grid_id": "다사53815262",
        }]
    )
    realtime = pd.DataFrame(
        [{"stationId": "ST-10", "stationName": "서교동 사거리", "rackTotCnt": 15}]
    )

    def list_keys(prefix):
        if prefix == ps.silver_schema.STATION_MASTER_ENRICHED_PREFIX:
            return [f"{prefix}dt=2026-08-17/hh=03/0300.parquet"]
        return [f"{prefix}dt=2026-08-17/hh=15/1505.parquet"]

    monkeypatch.setattr(ps.s3_io, "list_keys", list_keys)
    monkeypatch.setattr(
        ps.s3_io,
        "read_parquet",
        lambda key: master if "station_master_enriched" in key else realtime,
    )
    ps._station_master = None

    result = ps._get_station_master()

    assert result.loc["ST-10", "station_name"] == "서교동 사거리"
    assert result.loc["ST-10", "capacity"] == 15
    assert result.loc["ST-10", "grid_id"] == "다사53815262"


def test_missing_population_profile_uses_nan_fallback(monkeypatch):
    """선택 산출물인 인구 profile이 없어도 NaN 피처로 추론을 계속한다."""
    monkeypatch.setattr(ps.s3_io, "read_parquet", lambda key: None)
    ps._population_profile = None

    result = ps._population_fallback("unknown-grid", pd.Timestamp("2026-08-17 19:30"))

    assert all(pd.isna(value) for value in result.values())
    assert ps._population_profile == {}


def test_missing_analysis_summary_uses_korean_holiday_calendar(monkeypatch):
    """선택 산출물이 없어도 2026년 공휴일 feature를 계산한다."""
    monkeypatch.setattr(ps.config, "load_holidays_2025", lambda: (_ for _ in ()).throw(FileNotFoundError))
    ps._holidays = None

    result = ps._get_holidays()

    assert "2026-08-15" in result
