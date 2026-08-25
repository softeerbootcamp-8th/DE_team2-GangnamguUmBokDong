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
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
from core.s3 import (
    get_object_bytes,
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


@dataclass(frozen=True, slots=True)
class ColdBronzeResult:
    """날짜 하나의 Cold Bronze 생성 결과다."""

    status: str
    objects: int
    cold_key: str | None


def _sha256(payload: bytes) -> str:
    """Bytes의 lowercase SHA-256을 반환한다."""
    return hashlib.sha256(payload).hexdigest()


def _input_objects(source_id: str, day: date):
    """새 Hot 경로와 이관 전 legacy 경로의 해당 날짜 원본을 모두 나열한다."""
    objects = list_objects(f"bronze/hot/{source_id}/dt={day.isoformat()}/")
    objects += list_objects(f"bronze/{source_id}/dt={day.isoformat()}/")
    return sorted(objects, key=lambda item: item.key.encode("utf-8"))


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
    if not objects:
        return ColdBronzeResult(status="empty", objects=0, cold_key=None)

    signature_rows = [
        [obj.key, obj.size, obj.last_modified.isoformat()] for obj in objects
    ]
    input_signature = _sha256(
        json.dumps(signature_rows, separators=(",", ":"), ensure_ascii=False).encode()
    )
    manifest_key = f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
    previous = read_json(manifest_key)
    if (
        previous
        and previous.get("input_signature") == input_signature
        and previous.get("verified") is True
    ):
        cold_key = previous.get("cold_key")
        if isinstance(cold_key, str) and object_exists(cold_key):
            return ColdBronzeResult("skipped", len(objects), cold_key)

    rows = []
    for obj in objects:
        identity = _parse_key(obj.key, source_id, day)
        stored = get_object_bytes(obj.key)
        if stored is None:
            raise RuntimeError(f"LIST 뒤 Hot Bronze object가 사라졌다: {obj.key}")
        try:
            raw = gzip.decompress(stored)
        except gzip.BadGzipFile as exc:
            raise ValueError(f"Hot Bronze가 gzip이 아니다: {obj.key}") from exc
        rows.append(
            {
                "source_id": source_id,
                **identity,
                "original_key": obj.key,
                "collected_at": obj.last_modified.isoformat(),
                "stored_sha256": _sha256(stored),
                "payload_sha256": _sha256(raw),
                "stored_bytes": stored,
            }
        )

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
    if pq.ParquetFile(pa.BufferReader(readback)).metadata.num_rows != len(objects):
        raise RuntimeError(f"Cold Bronze readback row 수가 입력 object 수와 다르다: {cold_key}")

    write_json(
        manifest_key,
        {
            "source_id": source_id,
            "date": day.isoformat(),
            "input_signature": input_signature,
            "input_objects": len(objects),
            "cold_key": cold_key,
            "cold_sha256": cold_sha256,
            "stored_bytes": sum(obj.size for obj in objects),
            "verified": True,
        },
    )
    return ColdBronzeResult("compacted", len(objects), cold_key)


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
