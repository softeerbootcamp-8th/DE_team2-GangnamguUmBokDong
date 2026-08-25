"""검증된 날짜에서 최신 authority가 아닌 Silver object를 정리한다.

삭제는 Cold Bronze와 일 단위 Archive가 모두 현재 authority에 맞게 검증된 뒤에만
허용한다. 과거 Source Snapshot manifest는 immutable 감사 기록으로 남지만 삭제된
Silver URI는 더 이상 직접 읽을 수 있으므로, 삭제 목록과 복구 근거를 GC manifest에
기록한다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime, timedelta
from zoneinfo import ZoneInfo

import compaction
import storage
from config.schema import SourceConfig
from core.s3 import get_object_bytes, object_exists, read_json, write_json

_KST = ZoneInfo("Asia/Seoul")
NON_AUTHORITY_RETENTION_DAYS = 30


@dataclass(frozen=True, slots=True)
class SilverGcResult:
    """날짜 하나의 non-authority Silver 정리 결과다."""

    status: str
    deleted: int
    retained: int
    reason: str | None = None


def _cold_manifest(source_id: str, day: date) -> dict | None:
    """검증된 Cold Bronze manifest를 읽고 exact object 존재를 확인한다."""
    key = f"bronze/cold_manifest/{source_id}/dt={day.isoformat()}.json"
    manifest = read_json(key)
    if not manifest or manifest.get("verified") is not True:
        return None
    cold_key = manifest.get("cold_key")
    if not isinstance(cold_key, str) or not object_exists(cold_key):
        return None
    return manifest


def collect_date(
    config: SourceConfig,
    day: date,
    *,
    now: datetime | None = None,
) -> SilverGcResult:
    """최신 authority와 생성 후 30일이 지나지 않은 Silver를 보존한다."""
    objects = storage.list_silver_objects(config.source_id, day)
    if not objects:
        return SilverGcResult("skipped", deleted=0, retained=0, reason="no_silver")

    archive_manifest = storage.read_archive_manifest(config.source_id, day)
    if not archive_manifest:
        return SilverGcResult(
            "skipped", 0, len(objects), "archive_manifest_missing"
        )
    archive_key = archive_manifest.get("archive_key")
    if not isinstance(archive_key, str) or not object_exists(archive_key):
        return SilverGcResult("skipped", 0, len(objects), "archive_missing")
    cold_manifest = _cold_manifest(config.source_id, day)
    if cold_manifest is None:
        return SilverGcResult("skipped", 0, len(objects), "cold_unverified")

    authority, signature = compaction.resolve_date_authority(config, day)
    if archive_manifest.get("silver_signature") != signature:
        return SilverGcResult(
            "skipped", 0, len(objects), "archive_authority_stale"
        )
    authority_keys = {item.object.key for item in authority.selected}
    cutoff = (now or datetime.now(_KST)) - timedelta(
        days=NON_AUTHORITY_RETENTION_DAYS
    )
    candidates = [
        obj
        for obj in objects
        if obj.key not in authority_keys and obj.last_modified <= cutoff
    ]
    retained_keys = {obj.key for obj in objects if obj not in candidates}
    if not candidates:
        return SilverGcResult("skipped", 0, len(retained_keys), "nothing_to_delete")

    manifest_key = f"_silver_gc_manifest/{config.source_id}/dt={day.isoformat()}.json"
    base = {
        "source_id": config.source_id,
        "date": day.isoformat(),
        "archive_key": archive_key,
        "archive_silver_signature": signature,
        "cold_key": cold_manifest["cold_key"],
        "retained_authority_keys": sorted(authority_keys),
        "retained_keys": sorted(retained_keys),
        "retention_days": NON_AUTHORITY_RETENTION_DAYS,
        "cutoff": cutoff.isoformat(),
        "deletion_candidates": [
            {
                "key": obj.key,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat(),
            }
            for obj in candidates
        ],
    }
    write_json(
        manifest_key,
        {**base, "status": "prepared", "prepared_at": datetime.now(_KST).isoformat()},
    )
    deleted_keys = [obj.key for obj in candidates]
    storage.delete_silver_objects(deleted_keys)
    remaining = [key for key in deleted_keys if get_object_bytes(key) is not None]
    if remaining:
        write_json(
            manifest_key,
            {**base, "status": "failed", "remaining_keys": remaining},
        )
        raise RuntimeError(f"non-authority Silver 삭제 확인에 실패했다: {remaining}")
    write_json(
        manifest_key,
        {
            **base,
            "status": "completed",
            "deleted_keys": deleted_keys,
            "completed_at": datetime.now(_KST).isoformat(),
        },
    )
    return SilverGcResult("completed", len(deleted_keys), len(retained_keys))
