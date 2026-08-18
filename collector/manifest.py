"""manifest 스키마와 읽기 쓰기, 상태 어휘.

한 번의 실행이 무엇을 했고 어디까지 갔는지를 남긴다. 
실제 S3 접근은 storage.py에 위임하고, 이 모듈은 dict ↔ 모델 변환과 상태 어휘만 담당한다.
"""

from __future__ import annotations

from datetime import datetime
from enum import Enum, IntEnum
from typing import Annotated, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, PlainSerializer

import storage


class RunStatus(str, Enum):
    """한 번의 실행이 최종적으로 도달한 상태."""

    RUNNING = "running"
    SUCCEEDED = "succeeded"
    PARTIAL = "partial"
    FAILED = "failed"
    EMPTY = "empty"
    SKIPPED = "skipped"


class Stage(IntEnum):
    """실행이 어디까지 진행됐는지를 나타내는 단계. 값 순서가 진행 순서다."""

    BRONZE_WRITTEN = 1
    VALIDATED = 2
    COMPLETED = 3


def _stage_from_json(value: object) -> Stage:
    """저장된 Stage 문자열을 Stage 객체로 변환한다."""

    if isinstance(value, Stage):
        return value
    return Stage[str(value).upper()]


StageField = Annotated[
    Stage,
    BeforeValidator(_stage_from_json),
    PlainSerializer(lambda s: s.name.lower(), return_type=str),
]


class FailureReason(str, Enum):
    """실행이 FAILED 상태로 끝났을 때, 어느 단계에서 실패했는지 구분한다."""

    FETCH_ERROR = "fetch_error"
    STORAGE_ERROR = "storage_error"
    QUALITY_GATE = "quality_gate"
    CONFIG_ERROR = "config_error"


class BronzeArtifacts(BaseModel):
    """bronze 계층에 쓰인 조각들의 위치 정보."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    prefix: str
    parts: tuple[str, ...] = ()


class Artifacts(BaseModel):
    """실행 중 각 계층에 남긴 산출물 위치."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    bronze: BronzeArtifacts | None = None
    silver: str | None = None
    quarantine: str | None = None


class Counts(BaseModel):
    """수집·검증 단계를 거치며 집계한 row 개수."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    expected: int | None = None
    fetched: int = 0
    kept: int = 0
    repaired: int = 0
    dropped: int = 0


class Missing(BaseModel):
    """부분 실패로 채우지 못한 부분에 대한 정보."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    parts: tuple[str, ...] = ()
    rows: int | None = None
    basis: Literal["rows", "parts"] = "rows"


class ColumnIssueCount(BaseModel):
    """컬럼 하나에서 발견된 이상치 유형별 개수."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    missing: int = 0
    outlier: int = 0
    type_error: int = 0


class Manifest(BaseModel):
    """한 번의 실행이 남기는 실행 기록 전체.
    
    source_id: 소스 ID
    window_start: 윈도우 시작 시간
    window_end: 윈도우 종료 시간
    status: 실행 상태
    stage: 실행 단계
    failure_reason: 실패 이유
    attempt: 재시도 횟수
    revision: 수정 횟수
    started_at: 시작 시간
    ended_at: 종료 시간
    duration_ms: 실행 시간
    artifacts: 실행 결과물
    counts: 개수 정보
    missing: 누락 정보
    drop_ratio: 드롭 비율
    completeness: 완료 비율
    backfill_status: 백필 상태
    column_issues: 컬럼 이슈
    policy_actions: 정책 실행
    config_version: 설정 버전
    """

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
    """해당 윈도우의 manifest를 읽어 모델로 변환한다. 없으면 None을 반환한다."""

    data = storage.read_manifest(source_id, window_start)
    return None if data is None else Manifest.model_validate(data)


def save(manifest: Manifest) -> None:
    """manifest를 dict로 직렬화해 저장한다."""

    storage.write_manifest(
        manifest.source_id, manifest.window_start, manifest.model_dump(mode="json")
    )


class RetryMarker(BaseModel):
    """부분 실패로 남은 조각을 나중에 재시도하기 위해 큐에 남기는 마커."""

    model_config = ConfigDict(extra="forbid", frozen=True)
    source_id: str
    window_start: datetime
    missing_parts: tuple[str, ...]
    first_failed_at: datetime
    expires_at: datetime
    attempts: int = 1


def save_retry_marker(marker: RetryMarker) -> None:
    """retry marker를 dict로 직렬화해 저장한다."""

    storage.write_retry_marker(
        marker.source_id, marker.window_start, marker.model_dump(mode="json")
    )


def load_retry_markers(source_id: str) -> list[RetryMarker]:
    """해당 소스에 쌓인 retry marker를 모두 읽어 모델로 변환한다."""

    return [RetryMarker.model_validate(d) for d in storage.list_retry_markers(source_id)]


def clear_retry_marker(source_id: str, window_start: datetime) -> None:
    """해당 윈도우의 retry marker를 큐에서 제거한다."""
    
    storage.delete_retry_marker(source_id, window_start)
