"""Authoritative source snapshot S3 reader를 검증한다."""

import io
import json
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core.gold_publication.canonical import sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from core.source_snapshot_io import (
    PartialConsumptionPolicy,
    SourceDataStatus,
    SourceFreshness,
    SourceSnapshotNotFoundError,
    SourceSnapshotReadError,
    read_available_source_snapshot,
    read_exact_source_snapshot,
    read_latest_source_snapshot,
    read_partial_source_snapshot,
)

KST = ZoneInfo("Asia/Seoul")
TEST_BUCKET = "test-bucket"


def _put_snapshot(
    logical: datetime,
    rows: list[dict],
    *,
    revision: int = 0,
    source_id: str = "population_realtime",
) -> None:
    """테스트용 content-addressed Parquet과 authority manifest를 쓴다."""
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        f"silver/{source_id}/dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"{logical:%H%M}/sha256={checksum}.parquet"
    )
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="fixture-v1",
        silver_uri=f"s3://{TEST_BUCKET}/{silver_key}",
        silver_byte_sha256=checksum,
        counts=SourceSnapshotCounts(len(rows), len(rows), len(rows), 0, 0),
        planned_parts=("page=1",),
        completed_parts=("page=1",),
    )
    utc = logical.astimezone(ZoneInfo("UTC"))
    manifest_key = (
        f"source_snapshot_manifest/{source_id}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
        f"revision={revision:010d}.json"
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest.canonical_bytes
    )


def test_exact_window_selects_latest_contiguous_correction() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical, [{"value": 1}], revision=0)
    _put_snapshot(logical, [{"value": 2}], revision=1)

    result = read_exact_source_snapshot("population_realtime", logical)

    assert result.manifest.revision_no == 1
    assert result.table is not None
    assert result.table.column("value").to_pylist() == [2]


def test_exact_window_fails_when_content_bytes_change() -> None:
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical, [{"value": 1}])
    manifest = read_exact_source_snapshot("population_realtime", logical).manifest
    assert manifest.silver_uri is not None
    key = manifest.silver_uri.split(f"s3://{TEST_BUCKET}/", 1)[1]
    boto3.client("s3", region_name="us-east-1").put_object(
        Bucket=TEST_BUCKET,
        Key=key,
        Body=b"changed",
    )

    with pytest.raises(SourceSnapshotReadError, match="checksum"):
        read_exact_source_snapshot("population_realtime", logical)


def test_latest_window_is_bounded_and_ignores_future() -> None:
    cutoff = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    prior = cutoff - timedelta(minutes=5)
    _put_snapshot(prior, [{"value": 1}])
    _put_snapshot(cutoff + timedelta(minutes=5), [{"value": 2}])

    result = read_latest_source_snapshot(
        "population_realtime",
        cutoff,
        lookback=timedelta(hours=1),
    )

    assert result.manifest.logical_dttm == prior


def test_exact_window_requires_timezone_and_authority() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        read_exact_source_snapshot(
            "population_realtime",
            datetime(2026, 8, 20, 13, 50, tzinfo=UTC).replace(tzinfo=None),
        )
    with pytest.raises(SourceSnapshotNotFoundError):
        read_exact_source_snapshot(
            "population_realtime",
            datetime(2026, 8, 20, 13, 50, tzinfo=KST),
        )


def test_partial_window_uses_kst_path_and_validates_content_address() -> None:
    """UTC 인자를 받아도 KST diagnostic 경로를 찾고 artifact hash를 검증한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([{"value": 7}]), buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        f"silver/population_realtime/dt=2026-08-20/hh=13/1350/sha256={checksum}.parquet"
    )
    document = {
        "source_id": "population_realtime",
        "window_start": logical.isoformat(),
        "status": "partial",
        "stage": "completed",
        "failure_reason": None,
        "artifacts": {"silver": silver_key},
        "counts": {"kept": 1},
    }
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET,
        Key="_manifest/population_realtime/dt=2026-08-20/hh=13/1350.json",
        Body=json.dumps(document).encode(),
    )

    table = read_partial_source_snapshot(
        "population_realtime",
        logical.astimezone(UTC),
    )

    assert table.column("value").to_pylist() == [7]

    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=b"mutated")
    with pytest.raises(SourceSnapshotReadError, match="checksum"):
        read_partial_source_snapshot("population_realtime", logical)


def test_available_prefers_current_partial_to_past_complete() -> None:
    """현재 완전이 없으면 현재 PARTIAL을 과거 완전 성공보다 먼저 선택한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical - timedelta(minutes=5), [{"value": 1}])
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist([{"value": 2}]), buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        "silver/population_realtime/dt=2026-08-20/hh=13/"
        f"1350/sha256={checksum}.parquet"
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET,
        Key="_manifest/population_realtime/dt=2026-08-20/hh=13/1350.json",
        Body=json.dumps(
            {
                "source_id": "population_realtime",
                "window_start": logical.isoformat(),
                "status": "partial",
                "stage": "completed",
                "failure_reason": None,
                "artifacts": {"silver": silver_key},
                "counts": {"kept": 1},
            }
        ).encode(),
    )

    selected = read_available_source_snapshot(
        "population_realtime",
        logical,
        lookback=timedelta(hours=1),
        partial_policy=PartialConsumptionPolicy.REPAIR,
    )

    assert selected.status is SourceDataStatus.PARTIAL
    assert selected.freshness is SourceFreshness.CURRENT
    assert selected.table.column("value").to_pylist() == [2]
    metadata = selected.selection_metadata(
        "population_realtime",
        logical,
        partial_policy=PartialConsumptionPolicy.REPAIR,
    )
    assert metadata.as_dict() == {
        "freshness": "current",
        "partial_policy": "repair",
        "requested_dttm": logical.astimezone(UTC).isoformat(),
        "resolution": "repaired",
        "selected_dttm": logical.astimezone(UTC).isoformat(),
        "source_id": "population_realtime",
        "status": "partial",
    }

    rejected = read_available_source_snapshot(
        "population_realtime", logical, lookback=timedelta(hours=1)
    )
    assert rejected.status is SourceDataStatus.SUCCESS
    assert rejected.freshness is SourceFreshness.STALE
    assert rejected.table.column("value").to_pylist() == [1]


def test_available_uses_past_complete_after_current_is_missing() -> None:
    """현재 완전·부분이 모두 없으면 freshness 안의 과거 성공을 선택한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    prior = logical - timedelta(minutes=5)
    _put_snapshot(prior, [{"value": 1}])

    selected = read_available_source_snapshot(
        "population_realtime", logical, lookback=timedelta(hours=1)
    )

    assert selected.status is SourceDataStatus.SUCCESS
    assert selected.freshness is SourceFreshness.STALE
    assert selected.logical_dttm == prior.astimezone(UTC)


def test_available_fails_when_current_and_bounded_past_are_unavailable() -> None:
    """현재 및 허용 lookback 안의 과거 성공이 없으면 명시적으로 실패한다."""
    logical = datetime(2026, 8, 20, 13, 50, tzinfo=KST)
    _put_snapshot(logical - timedelta(hours=2), [{"value": 1}])

    with pytest.raises(SourceSnapshotNotFoundError):
        read_available_source_snapshot(
            "population_realtime", logical, lookback=timedelta(hours=1)
        )
