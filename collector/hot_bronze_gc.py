"""검증된 Cold inventory에 포함된 30일 지난 Hot Bronze만 명시적으로 삭제한다."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import pyarrow as pa
import pyarrow.parquet as pq
from core.s3 import (
    delete_objects,
    get_object_bytes,
    list_keys,
    list_objects,
    read_json,
    write_json,
)

_KST = ZoneInfo("Asia/Seoul")
_MANIFEST_KEY = re.compile(
    r"\Abronze/cold_manifest/(?P<source>[a-z][a-z0-9_]*)/"
    r"dt=(?P<day>\d{4}-\d{2}-\d{2})\.json\Z"
)


@dataclass(frozen=True, slots=True)
class HotGcResult:
    """Hot Bronze GC sweep 결과다."""

    dates: int
    deleted: int
    retained: int


def _gc_manifest_key(source_id: str, day: date) -> str:
    return f"_hot_gc_manifest/{source_id}/dt={day.isoformat()}.json"


def _inventory(manifest_key: str, manifest: dict) -> tuple[list[str], str]:
    """신규 manifest inventory를 읽거나 검증된 구버전 Cold Parquet에서 승격한다."""
    cold_key = manifest.get("cold_key")
    cold_sha256 = manifest.get("cold_sha256")
    if not isinstance(cold_key, str) or not isinstance(cold_sha256, str):
        raise TypeError(f"Cold manifest가 불완전하다: {manifest_key}")
    payload = get_object_bytes(cold_key)
    if payload is None or hashlib.sha256(payload).hexdigest() != cold_sha256:
        raise RuntimeError(f"Cold object checksum 검증에 실패했다: {cold_key}")
    inventory = manifest.get("inventory_keys")
    if not isinstance(inventory, list) or not all(
        isinstance(key, str) for key in inventory
    ):
        table = pq.read_table(pa.BufferReader(payload), columns=["original_key"])
        inventory = sorted(table["original_key"].to_pylist(), key=str.encode)
        if len(inventory) != manifest.get("input_objects"):
            raise RuntimeError(f"구버전 Cold row 수가 manifest와 다르다: {manifest_key}")
        signature = hashlib.sha256("\n".join(inventory).encode()).hexdigest()
        write_json(
            manifest_key,
            {
                **manifest,
                "inventory_keys": inventory,
                "inventory_signature": signature,
            },
        )
    signature = hashlib.sha256("\n".join(inventory).encode()).hexdigest()
    return inventory, signature


def collect_date(
    source_id: str,
    day: date,
    *,
    now: datetime | None = None,
    retention_days: int = 30,
) -> HotGcResult:
    """Cold inventory와 일치하고 보존기간이 지난 Hot 객체만 삭제한다."""
    if retention_days < 0:
        raise ValueError("Hot Bronze retention_days는 0 이상이어야 한다.")
    manifest_key = f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
    manifest = read_json(manifest_key)
    if not isinstance(manifest, dict) or manifest.get("verified") is not True:
        raise RuntimeError(f"검증된 Cold manifest가 없다: {manifest_key}")
    cold_key = manifest.get("cold_key")
    inventory, inventory_signature = _inventory(manifest_key, manifest)

    hot_objects = list_objects(f"bronze/hot/{source_id}/dt={day.isoformat()}/")
    hot_keys = {obj.key for obj in hot_objects}
    unknown = sorted(hot_keys - set(inventory))
    if unknown:
        raise RuntimeError(f"Cold에 포함되지 않은 Hot object가 있다: {unknown[:5]}")

    cutoff = (now or datetime.now(_KST)) - timedelta(days=retention_days)
    candidates = sorted(obj.key for obj in hot_objects if obj.last_modified <= cutoff)
    retained = len(hot_objects) - len(candidates)
    base = {
        "source_id": source_id,
        "date": day.isoformat(),
        "cold_key": cold_key,
        "cold_inventory_signature": inventory_signature,
        "retention_days": retention_days,
        "cutoff": cutoff.isoformat(),
        "deletion_candidates": candidates,
    }
    gc_key = _gc_manifest_key(source_id, day)
    if candidates:
        write_json(gc_key, {**base, "status": "prepared"})
        delete_objects(candidates)
        remaining = [key for key in candidates if get_object_bytes(key) is not None]
        if remaining:
            write_json(
                gc_key,
                {**base, "status": "failed", "remaining_keys": remaining},
            )
            raise RuntimeError(f"Hot Bronze 삭제 확인에 실패했다: {remaining}")
    status = "completed" if retained == 0 else "retained_recent"
    write_json(
        gc_key,
        {
            **base,
            "status": status,
            "deleted_keys": candidates,
            "retained": retained,
            "completed_at": datetime.now(_KST).isoformat(),
        },
    )
    return HotGcResult(dates=1, deleted=len(candidates), retained=retained)


def recover_due(
    source_id: str,
    *,
    today: date,
    retention_days: int = 30,
) -> HotGcResult:
    """Cold manifest 목록에서 미완료된 보존기한 도래 날짜만 복구한다."""
    total_dates = total_deleted = total_retained = 0
    cutoff_day = today - timedelta(days=retention_days)
    for key in list_keys(f"bronze/cold_manifest/{source_id}/"):
        matched = _MANIFEST_KEY.fullmatch(key)
        if matched is None or matched.group("source") != source_id:
            raise RuntimeError(f"Cold manifest key 형식이 잘못됐다: {key}")
        day = date.fromisoformat(matched.group("day"))
        if day > cutoff_day:
            continue
        cold_manifest = read_json(key)
        completed = read_json(_gc_manifest_key(source_id, day))
        inventory_signature = None
        if isinstance(cold_manifest, dict) and cold_manifest.get("verified") is True:
            stored_signature = cold_manifest.get("inventory_signature")
            if isinstance(stored_signature, str):
                inventory_signature = stored_signature
            else:
                _, inventory_signature = _inventory(key, cold_manifest)
        if (
            isinstance(cold_manifest, dict)
            and isinstance(completed, dict)
            and completed.get("status") == "completed"
            and completed.get("cold_inventory_signature") == inventory_signature
        ):
            continue
        result = collect_date(source_id, day, retention_days=retention_days)
        total_dates += result.dates
        total_deleted += result.deleted
        total_retained += result.retained
    return HotGcResult(total_dates, total_deleted, total_retained)
