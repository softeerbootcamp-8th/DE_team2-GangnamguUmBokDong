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
