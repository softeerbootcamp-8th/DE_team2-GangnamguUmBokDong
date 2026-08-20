"""실시간 station master의 인구 격자 계약을 검증한다."""

import pandas as pd
import pytest

from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _reset_station_master_cache():
    """각 테스트가 독립적으로 S3 station master를 읽도록 캐시를 비운다."""
    previous = ps._station_master
    ps._station_master = None
    yield
    ps._station_master = previous


def _station_master_rows(count: int = 10) -> pd.DataFrame:
    """normalizer의 실제 보강 station master 스키마 fixture를 만든다."""
    return pd.DataFrame({
        "station_id": [f"ST-{index + 1}" for index in range(count)],
        "station_no": list(range(1, count + 1)),
        "station_name": [f"station-{index + 1}" for index in range(count)],
        "capacity": [10] * count,
        "lat": [37.5] * count,
        "lon": [127.0] * count,
        "grid_id": [f"GRID-{index:04d}" for index in range(count)],
    })


def _stub_master_listing(monkeypatch) -> None:
    """보강 station master의 운영 prefix에 parquet 하나가 있는 것으로 대체한다."""
    monkeypatch.setattr(
        ps.s3_io,
        "list_keys",
        lambda prefix: [f"{prefix}dt=2026-08-20/hh=00/0000.parquet"],
    )


def test_station_master_reads_latest_enriched_partition(monkeypatch):
    """실제 list_keys(prefix) 계약으로 최신 보강 parquet만 선택해 읽는다."""
    prefix = ps.silver_schema.STATION_MASTER_ENRICHED_PREFIX
    older = f"{prefix}dt=2026-08-19/hh=00/0000.parquet"
    latest = f"{prefix}dt=2026-08-20/hh=00/0000.parquet"
    list_calls = []
    read_calls = []
    raw = _station_master_rows()

    def _list_keys(requested_prefix):
        list_calls.append(requested_prefix)
        return [latest, f"{prefix}_manifest.json", older, f"{prefix}_SUCCESS"]

    def _read_parquet(key):
        read_calls.append(key)
        return raw

    monkeypatch.setattr(ps.s3_io, "list_keys", _list_keys)
    monkeypatch.setattr(ps.s3_io, "read_parquet", _read_parquet)

    master = ps._get_station_master()

    assert list_calls == [prefix]
    assert read_calls == [latest]
    assert list(master.index) == list(raw["station_id"])


def test_station_master_requires_enriched_parquet(monkeypatch):
    """보강 prefix에 parquet이 없으면 예전 고정 station 파일로 몰래 대체하지 않는다."""
    monkeypatch.setattr(ps.s3_io, "list_keys", lambda prefix: [f"{prefix}_manifest.json"])

    with pytest.raises(FileNotFoundError, match="station_master_enriched"):
        ps._get_station_master()


def test_station_master_requires_grid_id_column(monkeypatch):
    """grid_id 컬럼 자체가 없으면 인구 feature 전체를 NaN으로 만들기 전에 실패한다."""
    raw = _station_master_rows().drop(columns="grid_id")
    _stub_master_listing(monkeypatch)
    monkeypatch.setattr(ps.s3_io, "read_parquet", lambda key: raw)

    with pytest.raises(ValueError, match="grid_id 컬럼이 없음"):
        ps._get_station_master()


def test_station_master_rejects_low_grid_id_coverage(monkeypatch):
    """grid_id 매핑률이 95% 미만이면 조용히 결측 인구로 서빙하지 않는다."""
    raw = _station_master_rows()
    raw.loc[0, "grid_id"] = None
    _stub_master_listing(monkeypatch)
    monkeypatch.setattr(ps.s3_io, "read_parquet", lambda key: raw)

    with pytest.raises(ValueError, match="grid_id 매핑률이 기준 미달"):
        ps._get_station_master()
