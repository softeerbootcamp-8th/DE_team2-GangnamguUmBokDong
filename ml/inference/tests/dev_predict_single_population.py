"""Normalizer Silver와 predict_single.py 인구 입력 계약을 검증한다."""

import pandas as pd

from inference import predict_single as ps


def test_recent_population_reads_latest_normalized_snapshot_before_target(monkeypatch):
    """예측 시각 이전의 가장 최근 보정 스냅샷을 읽는다."""
    ps._recent_population_by_ts = {}
    read_keys = []

    monkeypatch.setattr(
        ps.s3_io,
        "list_keys",
        lambda prefix: [
            "silver/living_population_normalized/dt=2026-08-17/hh=14/1405.parquet",
            "silver/living_population_normalized/dt=2026-08-17/hh=14/1410.parquet",
            "silver/living_population_normalized/dt=2026-08-17/hh=15/1500.parquet",
        ],
    )

    def fake_read_parquet(key):
        read_keys.append(key)
        return pd.DataFrame({"CELL_ID": ["다사53815262"], "SPOP": [3210]})

    monkeypatch.setattr(ps.s3_io, "read_parquet", fake_read_parquet)
    result = ps._get_recent_population(pd.Timestamp("2026-08-17 14:12"))

    assert read_keys == ["silver/living_population_normalized/dt=2026-08-17/hh=14/1410.parquet"]
    assert result.loc["다사53815262", "pop_total"] == 3210


def test_recent_population_never_falls_back_to_raw_population(monkeypatch):
    """보정 결과가 없으면 원본 Silver로 우회하지 않고 빈 값을 반환한다."""
    ps._recent_population_by_ts = {}
    prefixes = []
    monkeypatch.setattr(ps.s3_io, "list_keys", lambda prefix: prefixes.append(prefix) or [])

    result = ps._get_recent_population(pd.Timestamp("2026-08-17 14:12"), lookback_days=1)

    assert result.empty
    assert all("living_population_normalized" in prefix for prefix in prefixes)


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
