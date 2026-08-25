"""`_get_recent_population()`이 원본(`living_population_grid`)이 아니라 normalizer가
보정한 `living_population_normalized`를 읽는지, 5분 tick 소스답게 최근 값으로
거슬러 올라가는지 검증한다.

학습/평가(`feature_engine/spark/silver_source.py`의 `read_population()`)는 이
파일과 무관하게 여전히 원본을 그대로 읽는다 — 그쪽은 건드리지 않았다.
"""

import pandas as pd
import pytest
from ml_core import silver_schema

from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _reset_population_cache():
    ps._recent_population_by_ts = {}
    yield
    ps._recent_population_by_ts = {}


def _normalized_row(grid_id: str, spop: float) -> pd.DataFrame:
    # 실제 normalizer 출력 스키마(CELL_ID/H_DNG_CD/SPOP + 나이대x성별) 그대로 —
    # YMD/TT는 없다(시각이 이미 S3 키 경로에 있음).
    return pd.DataFrame([{"CELL_ID": grid_id, "H_DNG_CD": "1111051500", "SPOP": spop}])


def test_reads_from_normalized_source_id_not_raw(monkeypatch):
    """조회 키가 POPULATION_NORMALIZED_SOURCE_ID(living_population_normalized)를
    가리켜야 한다 — 원본(living_population_grid) 키가 아님."""
    requested_keys = []

    def _fake_read(key, columns=None):
        requested_keys.append(key)
        return None

    monkeypatch.setattr(ps.s3_io, "read_parquet", _fake_read)

    ps._get_recent_population(pd.Timestamp("2026-08-17 10:00:00"))

    assert requested_keys, "조회 키가 하나도 안 만들어짐"
    assert all(f"silver/{silver_schema.POPULATION_NORMALIZED_SOURCE_ID}/" in k for k in requested_keys)
    assert all(silver_schema.POPULATION_SOURCE_ID not in k for k in requested_keys)


def test_returns_target_tick_value_when_present(monkeypatch):
    target_ts = pd.Timestamp("2026-08-17 10:00:00")

    exact_key = silver_schema.silver_key(
        silver_schema.POPULATION_NORMALIZED_SOURCE_ID, target_ts
    )

    def _fake_read(key, columns=None):
        assert key == exact_key
        return _normalized_row("다사00000000", 1234.0)

    monkeypatch.setattr(ps.s3_io, "read_parquet", _fake_read)

    result = ps._get_recent_population(target_ts)

    assert list(result.columns) == ["pop_total"]
    assert result.loc["다사00000000", "pop_total"] == 1234.0


def test_falls_back_to_earlier_tick_when_exact_tick_missing(monkeypatch):
    """직접 호출·미래 예보 누락이면 제한된 lookback 안의 최근 값을 대신 쓴다."""
    target_ts = pd.Timestamp("2026-08-17 10:00:00")

    requested_keys = []

    def _fake_read(key, columns=None):
        requested_keys.append(key)
        if key.endswith("/0955.parquet"):
            return _normalized_row("다사00000000", 500.0)
        return None

    monkeypatch.setattr(ps.s3_io, "read_parquet", _fake_read)

    result = ps._get_recent_population(target_ts)

    assert result.loc["다사00000000", "pop_total"] == 500.0
    assert len(requested_keys) == 2


def test_returns_empty_dataframe_when_nothing_in_lookback_window(monkeypatch):
    monkeypatch.setattr(ps.s3_io, "read_parquet", lambda key, columns=None: None)

    result = ps._get_recent_population(pd.Timestamp("2026-08-17 10:00:00"))

    assert result.empty
    assert list(result.columns) == ["pop_total"]


def test_caches_result_per_target_ts(monkeypatch):
    call_count = 0

    def _fake_read(key, columns=None):
        nonlocal call_count
        call_count += 1
        return _normalized_row("다사00000000", 1.0)

    monkeypatch.setattr(ps.s3_io, "read_parquet", _fake_read)

    target_ts = pd.Timestamp("2026-08-17 10:00:00")
    ps._get_recent_population(target_ts)
    ps._get_recent_population(target_ts)

    assert call_count == 1  # 두 번째 호출은 캐시에서 바로 반환
