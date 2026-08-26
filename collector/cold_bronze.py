"""Hot Bronze 원본 객체를 날짜 단위 immutable Cold Bronze로 묶는다.

JSON을 파싱하거나 다시 직렬화하지 않는다. 각 Hot object의 gzip bytes, 원래 key와
checksum을 Parquet binary 컬럼에 넣어 모든 window와 모든 수집 revision을 복원할 수
있게 한다. Cold manifest만 해당 날짜의 최신 bundle을 가리키는 mutable pointer다.
"""

from __future__ import annotations

import gzip
import hashlib
import io
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta

import pyarrow as pa
import pyarrow.parquet as pq
from botocore.exceptions import ClientError
from core.s3 import (
    delete_objects,
    get_object_bytes,
    list_keys,
    list_objects,
    object_exists,
    put_object_bytes,
    read_json,
    write_json,
)

_HOT_KEY = re.compile(
    r"\Abronze/hot/(?P<source>[a-z][a-z0-9_]*)/dt=(?P<day>\d{4}-\d{2}-\d{2})/"
    r"hh=(?P<hour>\d{2})/(?P<hhmm>\d{4})/revision=(?P<revision>\d{10})/"
    r"part=(?P<part>.+)\.json\.gz\Z"
)
_LEGACY_KEY = re.compile(
    r"\Abronze/(?P<source>[a-z][a-z0-9_]*)/dt=(?P<day>\d{4}-\d{2}-\d{2})/"
    r"hh=(?P<hour>\d{2})/(?P<hhmm>\d{4})/part=(?P<part>.+)\.json\.gz\Z"
)
_PENDING_MARKER_KEY = re.compile(
    r"\A_cold_pending/(?P<source>[a-z][a-z0-9_]*)/"
    r"dt=(?P<day>\d{4}-\d{2}-\d{2})/sha256=[0-9a-f]{64}\.json\Z"
)


@dataclass(frozen=True, slots=True)
class ColdBronzeResult:
    """날짜 하나의 Cold Bronze 생성 결과다."""

    status: str
    objects: int
    cold_key: str | None


@dataclass(frozen=True, slots=True)
class ColdRecoveryResult:
    """pending marker에서 처리한 날짜별 Cold recovery 결과다."""

    dates: int
    objects: int
    stale_markers: int


def _pending_marker_key(source_id: str, day: date, hot_key: str) -> str:
    """Hot revision prefix 하나에 대응하는 immutable pending marker key를 만든다."""
    try:
        hot_revision_prefix, _ = hot_key.rsplit("/part=", 1)
    except ValueError as exc:
        raise ValueError(f"pending marker의 Hot key 형식이 잘못됐다: {hot_key}") from exc
    return (
        f"_cold_pending/{source_id}/dt={day.isoformat()}/"
        f"sha256={_sha256(hot_revision_prefix.encode())}.json"
    )


def write_pending_marker(source_id: str, day: date, hot_key: str) -> None:
    """Hot 저장 전에 날짜별 Cold 작업 marker를 put-once 방식으로 기록한다."""
    key = _pending_marker_key(source_id, day, hot_key)
    payload = {
        "source_id": source_id,
        "date": day.isoformat(),
        "hot_revision_prefix": hot_key.rsplit("/part=", 1)[0],
    }
    previous = read_json(key)
    if previous is not None and previous != payload:
        raise RuntimeError(f"Cold pending marker key 충돌: {key}")
    if previous is None:
        write_json(key, payload)


def _sha256(payload: bytes) -> str:
    """Bytes의 lowercase SHA-256을 반환한다."""
    return hashlib.sha256(payload).hexdigest()


def _input_objects(source_id: str, day: date):
    """새 Hot 경로와 이관 전 legacy 경로의 해당 날짜 원본을 모두 나열한다."""
    objects = list_objects(f"bronze/hot/{source_id}/dt={day.isoformat()}/")
    objects += list_objects(f"bronze/{source_id}/dt={day.isoformat()}/")
    return sorted(objects, key=lambda item: item.key.encode("utf-8"))


def _input_signature(objects) -> str:
    """Cold 입력 object 목록의 결정적인 signature를 만든다."""
    rows = [[obj.key, obj.size, obj.last_modified.isoformat()] for obj in objects]
    return _sha256(
        json.dumps(rows, separators=(",", ":"), ensure_ascii=False).encode()
    )


def _marker_revision_prefix(marker_key: str, source_id: str, day: date) -> str:
    """신·구 pending marker payload에서 Hot revision prefix를 읽는다."""
    marker = read_json(marker_key)
    if not isinstance(marker, dict):
        raise TypeError(f"Cold pending marker를 읽을 수 없다: {marker_key}")
    if marker.get("source_id") != source_id or marker.get("date") != day.isoformat():
        raise RuntimeError(f"Cold pending marker identity가 다르다: {marker_key}")
    prefix = marker.get("hot_revision_prefix")
    if isinstance(prefix, str) and prefix:
        return prefix
    hot_key = marker.get("hot_key")
    if isinstance(hot_key, str) and "/part=" in hot_key:
        return hot_key.rsplit("/part=", 1)[0]
    raise TypeError(f"Cold pending marker의 Hot revision prefix가 없다: {marker_key}")


def _parse_key(key: str, source_id: str, day: date) -> dict:
    """Hot/legacy object key를 Cold row identity로 파싱한다."""
    matched = _HOT_KEY.fullmatch(key) or _LEGACY_KEY.fullmatch(key)
    if matched is None:
        raise ValueError(f"Cold Bronze 입력 key 규칙에 맞지 않는다: {key}")
    if matched.group("source") != source_id or matched.group("day") != day.isoformat():
        raise ValueError(f"Cold Bronze 입력 key partition이 요청과 다르다: {key}")
    if matched.group("hour") != matched.group("hhmm")[:2]:
        raise ValueError(f"Cold Bronze hh와 HHMM이 다르다: {key}")
    revision = matched.groupdict().get("revision")
    return {
        "window_start": f"{day.isoformat()}T{matched.group('hhmm')[:2]}:{matched.group('hhmm')[2:]}:00+09:00",
        "revision": -1 if revision is None else int(revision),
        "part_key": matched.group("part"),
    }


def compact_date(source_id: str, day: date) -> ColdBronzeResult:
    """모든 Hot Bronze revision을 원본 bytes 그대로 날짜 파일 하나에 보관한다."""
    objects = _input_objects(source_id, day)
    manifest_key = f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
    previous = read_json(manifest_key)
    previous_rows: list[dict] = []
    if isinstance(previous, dict) and previous.get("verified") is True:
        previous_key = previous.get("cold_key")
        previous_sha256 = previous.get("cold_sha256")
        if isinstance(previous_key, str) and isinstance(previous_sha256, str):
            previous_payload = get_object_bytes(previous_key)
            if previous_payload is None or _sha256(previous_payload) != previous_sha256:
                raise RuntimeError(f"기존 Cold Bronze 검증에 실패했다: {previous_key}")
            previous_rows = pq.read_table(pa.BufferReader(previous_payload)).to_pylist()

    if not objects and not previous_rows:
        return ColdBronzeResult(status="empty", objects=0, cold_key=None)

    rows_by_key = {row["original_key"]: row for row in previous_rows}
    for obj in objects:
        identity = _parse_key(obj.key, source_id, day)
        stored = get_object_bytes(obj.key)
        if stored is None:
            raise RuntimeError(f"LIST 뒤 Hot Bronze object가 사라졌다: {obj.key}")
        try:
            raw = gzip.decompress(stored)
        except gzip.BadGzipFile as exc:
            raise ValueError(f"Hot Bronze가 gzip이 아니다: {obj.key}") from exc
        row = {
            "source_id": source_id,
            **identity,
            "original_key": obj.key,
            "collected_at": obj.last_modified.isoformat(),
            "stored_sha256": _sha256(stored),
            "payload_sha256": _sha256(raw),
            "stored_bytes": stored,
        }
        previous_row = rows_by_key.get(obj.key)
        if (
            previous_row is not None
            and previous_row["stored_sha256"] != row["stored_sha256"]
        ):
            raise RuntimeError(f"immutable Hot Bronze key 내용이 변경됐다: {obj.key}")
        rows_by_key[obj.key] = row

    rows = [rows_by_key[key] for key in sorted(rows_by_key, key=str.encode)]
    inventory_keys = [row["original_key"] for row in rows]
    inventory_signature = _sha256("\n".join(inventory_keys).encode())
    if (
        isinstance(previous, dict)
        and previous.get("inventory_keys") == inventory_keys
        and previous.get("verified") is True
    ):
        cold_key = previous.get("cold_key")
        if isinstance(cold_key, str) and object_exists(cold_key):
            return ColdBronzeResult("skipped", len(rows), cold_key)

    schema = pa.schema(
        [
            ("source_id", pa.string()),
            ("window_start", pa.string()),
            ("revision", pa.int64()),
            ("part_key", pa.string()),
            ("original_key", pa.string()),
            ("collected_at", pa.string()),
            ("stored_sha256", pa.string()),
            ("payload_sha256", pa.string()),
            ("stored_bytes", pa.binary()),
        ]
    )
    buffer = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), buffer, compression="zstd")
    cold_payload = buffer.getvalue()
    cold_sha256 = _sha256(cold_payload)
    cold_key = (
        f"bronze/cold/{source_id}/dt={day.isoformat()}/"
        f"sha256={cold_sha256}.parquet"
    )
    existing = get_object_bytes(cold_key)
    if existing is not None and existing != cold_payload:
        raise RuntimeError(f"immutable Cold Bronze key 충돌: {cold_key}")
    if existing is None:
        put_object_bytes(cold_key, cold_payload)

    readback = get_object_bytes(cold_key)
    if readback is None or _sha256(readback) != cold_sha256:
        raise RuntimeError(f"Cold Bronze readback checksum 검증에 실패했다: {cold_key}")
    if pq.ParquetFile(pa.BufferReader(readback)).metadata.num_rows != len(rows):
        raise RuntimeError(f"Cold Bronze readback row 수가 입력 object 수와 다르다: {cold_key}")

    write_json(
        manifest_key,
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "input_signature": inventory_signature,
            "inventory_signature": inventory_signature,
            "input_objects": len(rows),
            "inventory_keys": inventory_keys,
            "cold_key": cold_key,
            "cold_sha256": cold_sha256,
            "stored_bytes": sum(len(row["stored_bytes"]) for row in rows),
            "verified": True,
        },
    )
    return ColdBronzeResult("compacted", len(rows), cold_key)


def recover_pending(
    source_id: str,
    *,
    today: date,
    delay_days: int = 6,
) -> ColdRecoveryResult:
    """보존 대기 marker 중 안정화 기간이 지난 날짜만 Cold로 묶는다."""
    if delay_days < 0:
        raise ValueError("Cold recovery delay_days는 0 이상이어야 한다.")
    marker_keys = list_keys(f"_cold_pending/{source_id}/")
    markers_by_day: dict[date, list[str]] = {}
    for marker_key in marker_keys:
        matched = _PENDING_MARKER_KEY.fullmatch(marker_key)
        if matched is None:
            raise RuntimeError(f"Cold pending marker key 형식이 잘못됐다: {marker_key}")
        if matched.group("source") != source_id:
            raise RuntimeError(f"Cold pending marker source가 다르다: {marker_key}")
        try:
            day = date.fromisoformat(matched.group("day"))
        except ValueError as exc:
            raise RuntimeError(f"Cold pending marker 날짜가 잘못됐다: {marker_key}") from exc
        if day + timedelta(days=delay_days) <= today:
            markers_by_day.setdefault(day, []).append(marker_key)

    failures = []
    object_count = 0
    stale_count = 0
    for day in sorted(markers_by_day):
        try:
            prefixes = {
                marker_key: _marker_revision_prefix(marker_key, source_id, day)
                for marker_key in markers_by_day[day]
            }
            result = compact_date(source_id, day)
            object_count += result.objects
            current = _input_objects(source_id, day)
            manifest = read_json(
                f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
            )
            current_signature = _input_signature(current)
            inventory_keys = (
                set(manifest.get("inventory_keys", []))
                if isinstance(manifest, dict)
                else set()
            )
            if current and (
                not isinstance(manifest, dict)
                or manifest.get("verified") is not True
                or not {obj.key for obj in current}.issubset(inventory_keys)
            ):
                raise RuntimeError(
                    f"Cold 생성 중 Hot 입력이 변경됐다: source={source_id}, day={day}"
                )

            current_keys = {obj.key for obj in current}
            stale = [
                marker_key
                for marker_key, prefix in prefixes.items()
                if not any(key.startswith(f"{prefix}/") for key in current_keys)
            ]
            stale_count += len(stale)
            # 시작 뒤 생긴 marker는 이 목록에 없으므로 late revision 작업으로 남는다.
            delete_objects(sorted(set(markers_by_day[day])))

            after_delete = _input_objects(source_id, day)
            if _input_signature(after_delete) != current_signature:
                for obj in after_delete:
                    if obj.key.startswith("bronze/hot/"):
                        write_pending_marker(source_id, day, obj.key)
                raise RuntimeError(
                    f"Cold marker 정리 중 Hot 입력이 변경됐다: source={source_id}, day={day}"
                )
        except (ClientError, OSError, RuntimeError, ValueError) as exc:
            failures.append((day, exc))
    if failures:
        detail = ", ".join(f"{day}: {exc}" for day, exc in failures)
        raise RuntimeError(f"Cold pending recovery 일부 날짜가 실패했다: {detail}")
    return ColdRecoveryResult(
        dates=len(markers_by_day),
        objects=object_count,
        stale_markers=stale_count,
    )


def read_revision(
    source_id: str,
    window_start: datetime,
    revision: int,
    parts: tuple[str, ...],
) -> dict[str, bytes]:
    """Cold manifest가 가리키는 bundle에서 window revision의 원본 payload를 복원한다."""
    day = window_start.date()
    manifest_key = f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
    manifest = read_json(manifest_key)
    if not manifest:
        return {}
    cold_key = manifest.get("cold_key")
    expected_sha256 = manifest.get("cold_sha256")
    if not isinstance(cold_key, str) or not isinstance(expected_sha256, str):
        raise TypeError(f"Cold Bronze manifest가 불완전하다: {manifest_key}")
    payload = get_object_bytes(cold_key)
    if payload is None:
        raise RuntimeError(f"Cold Bronze manifest 대상이 없다: {cold_key}")
    if _sha256(payload) != expected_sha256:
        raise RuntimeError(f"Cold Bronze checksum이 manifest와 다르다: {cold_key}")

    expected_window = window_start.isoformat()
    requested = set(parts)
    restored = {}
    table = pq.read_table(
        pa.BufferReader(payload),
        columns=["window_start", "revision", "part_key", "stored_bytes"],
    )
    for row in table.to_pylist():
        if (
            row["window_start"] == expected_window
            and row["revision"] == revision
            and row["part_key"] in requested
        ):
            restored[row["part_key"]] = gzip.decompress(row["stored_bytes"])
    return restored
