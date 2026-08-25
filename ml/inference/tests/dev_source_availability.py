"""Inference Collector 입력이 공통 5단계의 앞 세 단계를 지키는지 검증한다."""

from types import SimpleNamespace

import pyarrow as pa
from core.source_snapshot_io import SourceSnapshotNotFoundError

from inference import predict_single as ps


def test_current_partial_is_selected_before_past_complete(monkeypatch):
    """현재 완전이 없으면 현재 부분 성공을 과거 완전 성공보다 먼저 둔다."""
    keys = [
        "silver/weather_ultra_short_live/dt=2026-08-20/hh=13/1345.parquet",
        "silver/weather_ultra_short_live/dt=2026-08-20/hh=13/1350.parquet",
    ]

    def read_exact(_source, logical, *, columns=None):
        del columns
        if logical.minute == 45:
            return SimpleNamespace(table=pa.table({"value": [1]}))
        raise SourceSnapshotNotFoundError("current complete missing")

    monkeypatch.setattr(ps, "read_exact_source_snapshot", read_exact)
    monkeypatch.setattr(
        ps,
        "read_partial_source_snapshot",
        lambda *_args, **_kwargs: pa.table({"value": [2]}),
    )

    selected = ps._read_authoritative_collector_many(keys)

    assert selected[0]["value"].tolist() == [1]
    assert selected[1]["value"].tolist() == [2]


def test_historical_partial_is_not_considered_past_success(monkeypatch):
    """현재 PARTIAL이 없을 때 과거 후보는 완전 authority만 사용한다."""
    keys = [
        "silver/weather_ultra_short_live/dt=2026-08-20/hh=13/1345.parquet",
        "silver/weather_ultra_short_live/dt=2026-08-20/hh=13/1350.parquet",
    ]

    monkeypatch.setattr(
        ps,
        "read_exact_source_snapshot",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            SourceSnapshotNotFoundError("complete missing")
        ),
    )
    partial_calls = []

    def read_partial(_source, logical, *, columns=None):
        del columns
        partial_calls.append(logical.minute)
        raise SourceSnapshotNotFoundError("current partial missing")

    monkeypatch.setattr(ps, "read_partial_source_snapshot", read_partial)

    assert ps._read_authoritative_collector_many(keys) == [None, None]
    assert partial_calls == [50]
