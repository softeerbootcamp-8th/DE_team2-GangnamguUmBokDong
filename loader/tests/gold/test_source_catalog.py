"""source authority S3 catalog의 revision·checksum 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from io import BytesIO
from typing import Any

import pytest
from core.gold_publication import ContractViolation, S3ImmutableObjectStore, sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold.source_catalog import S3SourceSnapshotCatalog

BUCKET = "fixture-bucket"
LOGICAL = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


class _FakeS3:
    """catalog list/get을 제공하는 in-memory S3 fake이다."""

    def __init__(self, objects: dict[str, bytes]) -> None:
        """object key·bytes 사전을 복사한다."""
        self.objects = dict(objects)
        self.get_calls: list[str] = []
        self.list_prefixes: list[str] = []

    def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
        """prefix에 맞는 key를 정렬해 반환한다."""
        prefix = kwargs["Prefix"]
        self.list_prefixes.append(prefix)
        keys = [key for key in sorted(self.objects) if key.startswith(prefix)]
        max_keys = kwargs.get("MaxKeys")
        if max_keys is not None:
            keys = keys[:max_keys]
        return {
            "Contents": [{"Key": key} for key in keys],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: Any) -> dict[str, Any]:
        """exact key bytes와 ContentLength를 반환한다."""
        self.get_calls.append(kwargs["Key"])
        payload = self.objects[kwargs["Key"]]
        return {"Body": BytesIO(payload), "ContentLength": len(payload)}

    def put_object(self, **kwargs: Any) -> dict[str, Any]:
        """이 read-only fake에서 put 호출을 거부한다."""
        raise AssertionError("put_object must not be called")


def _manifest(
    logical: datetime,
    revision: int,
    *,
    source_id: str = "bike_station_realtime",
) -> bytes:
    """유효한 SUCCEEDED source manifest bytes를 반환한다."""
    silver_payload_sha = sha256_hex(f"silver-{logical}-{revision}".encode())
    return build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="config-v1",
        silver_uri=(
            f"s3://{BUCKET}/source_snapshot_silver/{source_id}/"
            f"sha256={silver_payload_sha}.parquet"
        ),
        silver_byte_sha256=silver_payload_sha,
        counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
        planned_parts=("page-1",),
        completed_parts=("page-1",),
    ).canonical_bytes


def _key(source_id: str, logical: datetime, revision: int) -> str:
    """fixture source authority key를 반환한다."""
    return (
        f"source_snapshot_manifest/{source_id}/dt={logical:%Y-%m-%d}/"
        f"hh={logical:%H}/logical={logical:%Y%m%dT%H%M%S}"
        f"{logical.microsecond:06d}Z/revision={revision:010d}.json"
    )


def _catalog(objects: dict[str, bytes]) -> S3SourceSnapshotCatalog:
    """in-memory S3와 exact immutable store를 결합한 catalog를 반환한다."""
    client = _FakeS3(objects)
    return S3SourceSnapshotCatalog(
        client,
        S3ImmutableObjectStore(client),
        bucket=BUCKET,
    )


def test_exact_window_returns_highest_contiguous_correction() -> None:
    """exact logical window의 최대 correction revision을 선택한다."""
    source = "bike_station_realtime"
    objects = {
        _key(source, LOGICAL, revision): _manifest(LOGICAL, revision)
        for revision in (0, 1)
    }
    artifact = _catalog(objects).exact_window(source, LOGICAL)
    assert artifact.manifest.revision_no == 1
    assert artifact.byte_sha256 == sha256_hex(artifact.payload)


def test_optional_exact_window_returns_none_only_for_empty_prefix() -> None:
    """선택 API는 exact authority prefix가 실제로 비었을 때만 None을 반환한다."""
    assert _catalog({}).exact_window_or_none("cultural_event", LOGICAL) is None


def test_optional_exact_window_does_not_hide_revision_gap() -> None:
    """비어 있지 않은 손상 authority를 PARTIAL fallback 대상으로 축소하지 않는다."""
    source = "cultural_event"
    objects = {
        _key(source, LOGICAL, revision): _manifest(
            LOGICAL,
            revision,
            source_id=source,
        )
        for revision in (0, 2)
    }

    with pytest.raises(ContractViolation, match="빈틈없이"):
        _catalog(objects).exact_window_or_none(source, LOGICAL)


def test_latest_at_or_before_selects_latest_logical_then_revision() -> None:
    """기준 이전 최신 logical·correction을 두 단계로 선택한다."""
    source = "bike_station_realtime"
    earlier = LOGICAL - timedelta(minutes=5)
    objects = {
        _key(source, earlier, 0): _manifest(earlier, 0),
        _key(source, LOGICAL, 0): _manifest(LOGICAL, 0),
        _key(source, LOGICAL, 1): _manifest(LOGICAL, 1),
    }
    artifact = _catalog(objects).latest_at_or_before(
        source,
        LOGICAL,
        lookback=timedelta(hours=1),
    )
    assert artifact.manifest.logical_dttm == LOGICAL
    assert artifact.manifest.revision_no == 1


def test_recent_windows_keeps_one_highest_revision_per_logical_time() -> None:
    """station lifecycle 입력에 latest distinct window만 DESC로 제공한다."""
    source = "bike_station_realtime"
    objects: dict[str, bytes] = {}
    for offset in range(4):
        logical = LOGICAL - timedelta(minutes=5 * offset)
        objects[_key(source, logical, 0)] = _manifest(logical, 0)
    objects[_key(source, LOGICAL, 1)] = _manifest(LOGICAL, 1)
    artifacts = _catalog(objects).recent_windows(
        source,
        LOGICAL,
        limit=3,
        lookback=timedelta(hours=1),
    )
    assert [item.manifest.logical_dttm for item in artifacts] == [
        LOGICAL,
        LOGICAL - timedelta(minutes=5),
        LOGICAL - timedelta(minutes=10),
    ]
    assert artifacts[0].manifest.revision_no == 1


def test_revision_gap_fails_closed() -> None:
    """0,2로 빈 revision chain을 최신으로 오인하지 않는다."""
    source = "bike_station_realtime"
    objects = {
        _key(source, LOGICAL, revision): _manifest(LOGICAL, revision)
        for revision in (0, 2)
    }
    with pytest.raises(ContractViolation, match="빈틈없이"):
        _catalog(objects).list_source(source)


def test_path_manifest_identity_mismatch_fails_closed() -> None:
    """list key의 logical identity와 actual canonical manifest가 다르면 거부한다."""
    source = "bike_station_realtime"
    objects = {_key(source, LOGICAL, 0): _manifest(LOGICAL - timedelta(minutes=5), 0)}
    with pytest.raises(ContractViolation, match="identity"):
        _catalog(objects).list_source(source)


def test_empty_catalog_does_not_open_downstream_authority() -> None:
    """authority manifest가 하나도 없으면 latest publisher 입력을 열지 않는다."""
    with pytest.raises(ContractViolation, match="없습니다"):
        _catalog({}).latest_at_or_before(
            "cultural_event",
            LOGICAL,
            lookback=timedelta(hours=1),
        )


def test_latest_uses_bounded_hour_prefix_and_gets_only_selected_manifest() -> None:
    """많은 과거 object가 있어도 latest가 전체 history를 GET하지 않는다."""
    source = "bike_station_realtime"
    objects = {
        _key(source, LOGICAL - timedelta(days=day), 0): _manifest(
            LOGICAL - timedelta(days=day),
            0,
        )
        for day in range(100)
    }
    client = _FakeS3(objects)
    catalog = S3SourceSnapshotCatalog(
        client,
        S3ImmutableObjectStore(client),
        bucket=BUCKET,
    )

    selected = catalog.latest_at_or_before(
        source,
        LOGICAL,
        lookback=timedelta(hours=1),
    )

    assert selected.manifest.logical_dttm == LOGICAL
    assert len(client.get_calls) == 2
    assert len(client.list_prefixes) == 1
    assert client.list_prefixes[0].endswith("dt=2026-08-20/hh=00/")


def test_latest_requires_explicit_positive_lookback() -> None:
    """publisher가 무한 history scan이나 암묵적 freshness를 선택하지 못하게 한다."""
    with pytest.raises(ContractViolation, match="양수 timedelta"):
        _catalog({}).latest_at_or_before(
            "bike_station_realtime",
            LOGICAL,
            lookback=timedelta(0),
        )


def test_recent_fails_when_bounded_scan_would_omit_older_lifecycle_window() -> None:
    """3개보다 적게 찾았을 때 lookback 밖 history를 초기 상태처럼 오인하지 않는다."""
    source = "bike_station_realtime"
    old = LOGICAL - timedelta(days=2)
    objects = {
        _key(source, old, 0): _manifest(old, 0),
        _key(source, LOGICAL, 0): _manifest(LOGICAL, 0),
    }

    with pytest.raises(ContractViolation, match="완전성"):
        _catalog(objects).recent_windows(
            source,
            LOGICAL,
            limit=3,
            lookback=timedelta(hours=1),
        )
