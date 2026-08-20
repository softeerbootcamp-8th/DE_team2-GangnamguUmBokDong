"""권위 manifest를 따라 immutable source Silver snapshot을 읽는다."""

from __future__ import annotations

import io
import json
import os
import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from urllib.parse import urlsplit
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq

from .gold_publication.canonical import format_utc_dttm, sha256_hex
from .s3 import get_object_bytes, list_keys
from .source_snapshot import (
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    parse_source_snapshot_manifest,
)

_SOURCE_ID = re.compile(r"[a-z][a-z0-9_]*\Z")
_REVISION_KEY = re.compile(r"revision=([0-9]{10})\.json\Z")


class SourceSnapshotReadError(RuntimeError):
    """Source authority 또는 연결된 Silver artifact가 올바르지 않다."""


class SourceSnapshotNotFoundError(SourceSnapshotReadError):
    """요청 범위에 authoritative source snapshot이 없다."""


@dataclass(frozen=True, slots=True)
class SourceSnapshotData:
    """검증한 source manifest와 선택적인 Parquet table을 결합한다."""

    manifest: SourceSnapshotManifest
    table: pa.Table | None


def read_exact_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    columns: list[str] | None = None,
) -> SourceSnapshotData:
    """한 logical window의 최신 correction과 연결된 Parquet을 읽는다."""
    source = _validated_source_id(source_id)
    logical = _aware_utc(logical_dttm)
    keys = tuple(sorted(list_keys(_logical_prefix(source, logical))))
    if not keys:
        raise SourceSnapshotNotFoundError(
            f"{source} exact source authority window가 없습니다: {format_utc_dttm(logical)}"
        )

    revisions: list[int] = []
    manifests: list[SourceSnapshotManifest] = []
    for key in keys:
        match = _REVISION_KEY.search(key)
        if match is None:
            raise SourceSnapshotReadError(
                f"source authority revision key가 잘못됐습니다: {key}"
            )
        revision = int(match.group(1))
        payload = get_object_bytes(key)
        if payload is None:
            raise SourceSnapshotReadError(
                f"LIST된 source authority를 읽을 수 없습니다: {key}"
            )
        manifest = parse_source_snapshot_manifest(payload)
        if (
            manifest.source_id != source
            or manifest.logical_dttm.astimezone(UTC) != logical
            or manifest.revision_no != revision
            or key != _manifest_key(manifest)
        ):
            raise SourceSnapshotReadError(
                f"source authority key와 manifest identity가 다릅니다: {key}"
            )
        revisions.append(revision)
        manifests.append(manifest)

    if revisions != list(range(len(revisions))):
        raise SourceSnapshotReadError(
            f"source authority revision chain이 0부터 연속되지 않습니다: {revisions}"
        )
    manifest = manifests[-1]
    if manifest.status is SourceSnapshotStatus.EMPTY:
        return SourceSnapshotData(manifest=manifest, table=None)
    return SourceSnapshotData(
        manifest=manifest,
        table=_read_manifest_parquet(manifest, columns=columns),
    )


def read_latest_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    lookback: timedelta,
    columns: list[str] | None = None,
) -> SourceSnapshotData:
    """기준 시각 이전 bounded 범위의 최신 SUCCEEDED snapshot을 읽는다."""
    if type(lookback) is not timedelta or lookback <= timedelta(0):
        raise ValueError("lookback은 양수 timedelta여야 합니다.")
    source = _validated_source_id(source_id)
    cutoff = _aware_utc(logical_dttm)
    lower = cutoff - lookback
    current = cutoff.replace(minute=0, second=0, microsecond=0)
    final = lower.replace(minute=0, second=0, microsecond=0)
    while current >= final:
        keys = list_keys(_hour_prefix(source, current))
        logicals = sorted(
            {
                parsed
                for key in keys
                if (parsed := _logical_from_key(key, source)) is not None
                and lower <= parsed <= cutoff
            },
            reverse=True,
        )
        for logical in logicals:
            snapshot = read_exact_source_snapshot(source, logical, columns=columns)
            if snapshot.table is not None:
                return snapshot
        current -= timedelta(hours=1)
    raise SourceSnapshotNotFoundError(
        f"{source} SUCCEEDED source authority가 lookback 안에 없습니다."
    )


def read_partial_source_snapshot(
    source_id: str,
    logical_dttm: datetime,
    *,
    columns: list[str] | None = None,
) -> pa.Table:
    """명시적으로 허용된 PARTIAL 실행의 content-addressed Silver를 읽는다.

    PARTIAL은 Gold authority를 열 수 없지만 실시간 운영 소비자는 source별 정책에
    따라 제한적으로 사용할 수 있다. 호출자가 그 정책을 결정하고, 이 함수는 exact
    diagnostic manifest가 completed/partial인지와 artifact checksum·행 수를 검증한다.
    """
    source = _validated_source_id(source_id)
    logical = _aware_utc(logical_dttm)
    local = logical.astimezone(ZoneInfo("Asia/Seoul"))
    manifest_key = (
        f"_manifest/{source}/dt={local:%Y-%m-%d}/hh={local:%H}/{local:%H%M}.json"
    )
    payload = get_object_bytes(manifest_key)
    if payload is None:
        raise SourceSnapshotNotFoundError(
            f"{source} partial diagnostic manifest가 없습니다: {manifest_key}"
        )
    try:
        document = json.loads(payload)
        manifest_logical = datetime.fromisoformat(document["window_start"])
        artifacts = document["artifacts"]
        counts = document["counts"]
        silver_key = artifacts["silver"]
        kept = counts["kept"]
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise SourceSnapshotReadError(
            f"partial diagnostic manifest 형식이 잘못됐습니다: {manifest_key}"
        ) from exc
    if (
        type(document) is not dict
        or document.get("source_id") != source
        or manifest_logical.tzinfo is None
        or manifest_logical.astimezone(UTC) != logical
        or document.get("status") != "partial"
        or document.get("stage") != "completed"
        or document.get("failure_reason") is not None
        or type(artifacts) is not dict
        or type(counts) is not dict
        or type(silver_key) is not str
        or type(kept) is not int
        or kept <= 0
    ):
        raise SourceSnapshotReadError(
            f"partial diagnostic manifest가 completed PARTIAL 계약을 위반했습니다: {manifest_key}"
        )
    match = re.fullmatch(
        rf"silver/{re.escape(source)}/dt={local:%Y-%m-%d}/hh={local:%H}/"
        rf"{local:%H%M}/sha256=([0-9a-f]{{64}})\.parquet",
        silver_key,
    )
    if match is None:
        raise SourceSnapshotReadError(
            f"partial Silver key가 logical window 또는 content address와 다릅니다: {silver_key}"
        )
    return _read_content_addressed_parquet(
        silver_key,
        checksum=match.group(1),
        expected_rows=kept,
        columns=columns,
    )


def _read_manifest_parquet(
    manifest: SourceSnapshotManifest,
    *,
    columns: list[str] | None,
) -> pa.Table:
    """Manifest URI의 content-addressed bytes와 행 수를 검증해 읽는다."""
    if manifest.silver_uri is None or manifest.silver_byte_sha256 is None:
        raise SourceSnapshotReadError(
            "SUCCEEDED manifest에 Silver artifact가 없습니다."
        )
    parsed = urlsplit(manifest.silver_uri)
    bucket = os.environ.get("S3_BUCKET", "gangnamgu")
    if parsed.scheme != "s3" or parsed.netloc != bucket:
        raise SourceSnapshotReadError(
            f"source Silver bucket이 현재 런타임과 다릅니다: {manifest.silver_uri}"
        )
    key = parsed.path.removeprefix("/")
    return _read_content_addressed_parquet(
        key,
        checksum=manifest.silver_byte_sha256,
        expected_rows=manifest.counts.kept,
        columns=columns,
    )


def _read_content_addressed_parquet(
    key: str,
    *,
    checksum: str,
    expected_rows: int,
    columns: list[str] | None,
) -> pa.Table:
    """Exact key의 checksum·Parquet·행 수를 검증한다."""
    body = get_object_bytes(key)
    if body is None:
        raise SourceSnapshotReadError(f"source Silver가 없습니다: {key}")
    if sha256_hex(body) != checksum:
        raise SourceSnapshotReadError(
            f"source Silver checksum이 manifest와 다릅니다: {key}"
        )
    try:
        table = pq.read_table(io.BytesIO(body), columns=columns)
    except (pa.ArrowException, OSError) as exc:
        raise SourceSnapshotReadError(
            f"source Silver Parquet을 읽을 수 없습니다: {key}"
        ) from exc
    if table.num_rows != expected_rows:
        raise SourceSnapshotReadError(
            f"source Silver 행 수가 manifest와 다릅니다: key={key}, "
            f"actual={table.num_rows}, expected={expected_rows}"
        )
    return table


def _validated_source_id(value: str) -> str:
    """Authority 경로에 쓸 source ID를 검증한다."""
    if type(value) is not str or _SOURCE_ID.fullmatch(value) is None:
        raise ValueError("source_id는 lowercase snake_case여야 합니다.")
    return value


def _aware_utc(value: datetime) -> datetime:
    """Aware datetime을 같은 UTC instant로 정규화한다."""
    if type(value) is not datetime:
        raise TypeError("logical_dttm은 datetime이어야 합니다.")
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("logical_dttm은 timezone-aware datetime이어야 합니다.")
    format_utc_dttm(value)
    return value.astimezone(UTC)


def _hour_prefix(source_id: str, value: datetime) -> str:
    """UTC hour authority prefix를 반환한다."""
    utc = value.astimezone(UTC)
    return f"source_snapshot_manifest/{source_id}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"


def _logical_prefix(source_id: str, value: datetime) -> str:
    """UTC logical window authority prefix를 반환한다."""
    utc = value.astimezone(UTC)
    return (
        f"{_hour_prefix(source_id, utc)}"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
    )


def _manifest_key(manifest: SourceSnapshotManifest) -> str:
    """Manifest identity의 canonical authority key를 반환한다."""
    return (
        f"{_logical_prefix(manifest.source_id, manifest.logical_dttm)}"
        f"revision={manifest.revision_no:010d}.json"
    )


def _logical_from_key(key: str, source_id: str) -> datetime | None:
    """해당 source의 canonical authority key에서 logical UTC 시각을 읽는다."""
    prefix = f"source_snapshot_manifest/{source_id}/"
    if not key.startswith(prefix):
        raise SourceSnapshotReadError(f"source authority key가 prefix 밖입니다: {key}")
    try:
        logical_text = key.split("/logical=", 1)[1].split("/", 1)[0]
        logical = datetime.strptime(logical_text, "%Y%m%dT%H%M%S%fZ").replace(
            tzinfo=UTC
        )
    except (IndexError, ValueError) as exc:
        raise SourceSnapshotReadError(
            f"source authority key logical 형식이 잘못됐습니다: {key}"
        ) from exc
    if (
        not key.startswith(_logical_prefix(source_id, logical))
        or _REVISION_KEY.search(key) is None
    ):
        raise SourceSnapshotReadError(
            f"source authority key 형식이 잘못됐습니다: {key}"
        )
    return logical
