"""manifest 스키마와 읽기/쓰기, 상태 어휘(RunStatus / Stage / FailureReason).

한 번의 실행이 무엇을 했고 어디까지 갔는지를 남긴다. 실제 S3 접근은 storage.py에
위임하고, 이 모듈은 dict ↔ 모델 변환(직렬화)과 상태 어휘만 담당한다.

설계 근거: docs/superpowers/specs/2026-08-13-collector-storage-manifest-design.md
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum, IntEnum
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, PlainSerializer

import storage


class RunStatus(str, Enum):
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    EMPTY = "empty"
    SKIPPED = "skipped"


class Stage(IntEnum):
    BRONZE_WRITTEN = 1
    VALIDATED = 2
    COMPLETED = 3


def _stage_from_json(value: object) -> Stage:
    if isinstance(value, Stage):
        return value
    return Stage[str(value).upper()]


StageField = Annotated[
    Stage,
    BeforeValidator(_stage_from_json),
    PlainSerializer(lambda s: s.name.lower(), return_type=str),
]


class FailureReason(str, Enum):
    FETCH_ERROR = "fetch_error"
    STORAGE_ERROR = "storage_error"
    QUALITY_GATE = "quality_gate"
    CONFIG_ERROR = "config_error"


class BronzeArtifacts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    prefix: str
    parts: tuple[str, ...] = ()


class Artifacts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    bronze: BronzeArtifacts | None = None
    silver: str | None = None
    quarantine: str | None = None


class Counts(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    expected: int | None = None
    fetched: int = 0
    kept: int = 0
    repaired: int = 0
    dropped: int = 0


class Missing(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    parts: tuple[str, ...] = ()
    rows: int | None = None
    basis: Literal["rows", "parts"] = "rows"


class ColumnIssueCount(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    missing: int = 0
    outlier: int = 0
    type_error: int = 0


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    window_start: datetime
    window_end: datetime
    status: RunStatus
    stage: StageField
    failure_reason: FailureReason | None = None
    attempt: int = 1
    revision: int = 0
    started_at: datetime
    ended_at: datetime | None = None
    duration_ms: int | None = None
    artifacts: Artifacts = Artifacts()
    counts: Counts = Counts()
    missing: Missing = Missing()
    drop_ratio: float | None = None
    completeness: float | None = None
    backfill_status: Literal["pending", "expired"] | None = None
    column_issues: dict[str, ColumnIssueCount] = {}
    policy_actions: dict[str, int] = {}
    config_version: str


def load(source_id: str, window_start: datetime) -> Manifest | None:
    data = storage.read_manifest(source_id, window_start)
    return None if data is None else Manifest.model_validate(data)


def save(manifest: Manifest) -> None:
    storage.write_manifest(
        manifest.source_id, manifest.window_start, manifest.model_dump(mode="json")
    )


class RetryMarker(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    window_start: datetime
    missing_parts: tuple[str, ...]
    first_failed_at: datetime
    expires_at: datetime
    attempts: int = 1


def save_retry_marker(marker: RetryMarker) -> None:
    storage.write_retry_marker(
        marker.source_id, marker.window_start, marker.model_dump(mode="json")
    )


def load_retry_markers(source_id: str) -> list[RetryMarker]:
    return [RetryMarker.model_validate(d) for d in storage.list_retry_markers(source_id)]


def clear_retry_marker(source_id: str, window_start: datetime) -> None:
    storage.delete_retry_marker(source_id, window_start)
