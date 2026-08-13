"""manifest 스키마와 읽기/쓰기, 상태 어휘(RunStatus / Stage / FailureReason).

한 번의 실행이 무엇을 했고 어디까지 갔는지를 남긴다. 실제 S3 접근은 storage.py에
위임하고, 이 모듈은 dict ↔ 모델 변환(직렬화)과 상태 어휘만 담당한다.

설계 근거: docs/superpowers/specs/2026-08-13-collector-storage-manifest-design.md
"""

from __future__ import annotations

from enum import Enum, IntEnum
from typing import Annotated

from pydantic import BeforeValidator, PlainSerializer

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
