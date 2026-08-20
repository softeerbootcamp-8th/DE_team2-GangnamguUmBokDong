"""Immutable source snapshot manifest의 canonical bytes와 권한 경계를 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime

import pytest
from core.gold_publication.errors import CanonicalParseError
from core.source_snapshot import (
    SOURCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    SourceSnapshotContractError,
    SourceSnapshotCounts,
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
    parse_source_snapshot_manifest,
    same_source_snapshot_content,
)

LOGICAL_DTTM = datetime(2026, 8, 19, 7, 0, tzinfo=UTC)
SILVER_SHA256 = "a" * 64
SILVER_URI = f"s3://fixture/silver/source/sha256={SILVER_SHA256}.parquet"


def _succeeded(**overrides: object) -> SourceSnapshotManifest:
    """테스트용 정상 SUCCEEDED manifest를 만든다."""
    fields: dict[str, object] = {
        "source_id": "bike_station_realtime",
        "logical_dttm": LOGICAL_DTTM,
        "revision_no": 0,
        "status": SourceSnapshotStatus.SUCCEEDED,
        "config_version": "sha256:config-v1",
        "silver_uri": SILVER_URI,
        "silver_byte_sha256": SILVER_SHA256,
        "counts": SourceSnapshotCounts(
            expected=2,
            fetched=2,
            kept=2,
            repaired=1,
            dropped=0,
        ),
        "planned_parts": ("page-00001-00002",),
        "completed_parts": ("page-00001-00002",),
    }
    fields.update(overrides)
    return build_source_snapshot_manifest(**fields)  # type: ignore[arg-type]


def _empty(**overrides: object) -> SourceSnapshotManifest:
    """테스트용 confirmed EMPTY manifest를 만든다."""
    fields: dict[str, object] = {
        "source_id": "cultural_event",
        "logical_dttm": LOGICAL_DTTM,
        "revision_no": 0,
        "status": SourceSnapshotStatus.EMPTY,
        "config_version": "sha256:config-v1",
        "silver_uri": None,
        "silver_byte_sha256": None,
        "counts": SourceSnapshotCounts(0, 0, 0, 0, 0),
        "planned_parts": ("page-00001-01000",),
        "completed_parts": ("page-00001-01000",),
    }
    fields.update(overrides)
    return build_source_snapshot_manifest(**fields)  # type: ignore[arg-type]


def test_succeeded_manifest_has_stable_exact_canonical_bytes() -> None:
    """11개 field와 UTC microsecond 표현을 golden bytes로 고정한다."""
    manifest = _succeeded()

    assert manifest.canonical_bytes == (
        b'{"completed_parts":["page-00001-00002"],'
        b'"config_version":"sha256:config-v1",'
        b'"counts":{"dropped":0,"expected":2,"fetched":2,"kept":2,"repaired":1},'
        b'"logical_dttm":"2026-08-19T07:00:00.000000Z",'
        b'"planned_parts":["page-00001-00002"],"revision_no":0,'
        b'"schema_version":"source-snapshot-manifest-v1",'
        b'"silver_byte_sha256":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"silver_uri":"s3://fixture/silver/source/'
        b'sha256=aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.parquet",'
        b'"source_id":"bike_station_realtime","status":"succeeded"}'
    )
    assert parse_source_snapshot_manifest(manifest.canonical_bytes) == manifest


def test_empty_manifest_round_trips_without_silver() -> None:
    """Confirmed EMPTY는 expected=0과 pagination 완료만으로 round trip한다."""
    manifest = _empty()

    assert parse_source_snapshot_manifest(manifest.canonical_bytes) == manifest
    assert manifest.silver_uri is None


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"status": "succeeded"}, "SourceSnapshotStatus"),
        ({"planned_parts": ("b", "a")}, "정렬"),
        ({"completed_parts": ("page-2",)}, "planned_parts"),
        ({"silver_uri": "s3://fixture/silver/current.parquet"}, "content-addressed"),
        ({"counts": SourceSnapshotCounts(2, 2, 1, 0, 1)}, "dropped=0"),
    ],
)
def test_succeeded_rejects_non_authoritative_values(
    overrides: dict[str, object],
    message: str,
) -> None:
    """SUCCEEDED authority가 partial·mutable·비정렬 근거를 받지 않는다."""
    with pytest.raises(SourceSnapshotContractError, match=message):
        _succeeded(**overrides)


@pytest.mark.parametrize(
    "overrides",
    [
        {"counts": SourceSnapshotCounts(None, 0, 0, 0, 0)},
        {"counts": SourceSnapshotCounts(1, 0, 0, 0, 0)},
        {"silver_uri": SILVER_URI, "silver_byte_sha256": SILVER_SHA256},
        {"completed_parts": ()},
    ],
)
def test_empty_rejects_unknown_or_unconfirmed_results(
    overrides: dict[str, object],
) -> None:
    """Unknown-total·산출물·미완료 pagination은 EMPTY authority가 아니다."""
    with pytest.raises(SourceSnapshotContractError):
        _empty(**overrides)


def test_counts_reject_bool_and_inconsistent_totals() -> None:
    """Bool과 fetched != kept+dropped인 count를 거부한다."""
    with pytest.raises(SourceSnapshotContractError):
        SourceSnapshotCounts(None, True, 1, 0, 0)  # type: ignore[arg-type]
    with pytest.raises(SourceSnapshotContractError):
        SourceSnapshotCounts(None, 3, 2, 0, 0)


def test_parser_rejects_unknown_field_and_noncanonical_bytes() -> None:
    """Parser가 schema 확장과 공백 JSON을 fail closed한다."""
    payload = _succeeded().canonical_bytes

    with pytest.raises(CanonicalParseError):
        parse_source_snapshot_manifest(payload[:-1] + b',"extra":1}')
    with pytest.raises(CanonicalParseError):
        parse_source_snapshot_manifest(payload.replace(b'":', b'": ', 1))


def test_same_content_ignores_only_revision() -> None:
    """Replay 판정은 revision만 제외하고 status·artifact·counts를 모두 비교한다."""
    first = _succeeded(revision_no=0)

    assert same_source_snapshot_content(first, replace(first, revision_no=7))
    assert not same_source_snapshot_content(
        first,
        _succeeded(config_version="sha256:config-v2", revision_no=7),
    )


def test_schema_version_constant_is_disk_contract() -> None:
    """Source manifest schema version 문자열을 회귀 고정한다."""
    assert SOURCE_SNAPSHOT_MANIFEST_SCHEMA_VERSION == "source-snapshot-manifest-v1"
