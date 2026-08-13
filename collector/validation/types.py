"""
collector에서 이 파일은 검증 계층의 어휘(공용 타입)를 정의하는 최하단 기반 모듈입니다. 
코드를 직접 실행하지 않고, 다른 모듈들이 서로 대화할 때 쓰는 공통 언어만 제공합니다.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from config.schema import ColumnSpec


class Action(Enum):
    """컬럼 정책이 엔진에 돌려주는 처리 지시."""

    KEEP = "keep"
    DROP_ROW = "drop_row"
    FAIL_BATCH = "fail_batch"


class IssueKind(Enum):
    """판정 이슈의 종류."""

    MISSING = "missing"
    TYPE_ERROR = "type_error"
    OUTLIER = "outlier"


class RowVerdict(Enum):
    """행 정책의 최종 판정."""

    KEEP = "keep"
    DROP = "drop"


@dataclass(frozen=True, slots=True)
class Issue:
    """컬럼 하나의 판정 결과가 무엇으로 문제인지, 
    원시값·필수여부·스펙을 담아 정책 함수에 전달하는 불변 데이터 객체

    엔진이 정책에 넘기는 `value`와 이 객체의 `raw_value`는 다르다.

    | kind | 정책이 받는 value |
    | --- | --- |
    | MISSING | 정규화된 `None` |
    | TYPE_ERROR | 캐스팅에 실패한 원시값 |
    | OUTLIER | 캐스팅에 성공한 값 |

    `MISSING`에서 원시값을 넘기지 않는 이유는 `optional_missing`의 기본값이
    `keep_null`이기 때문이다.
    """

    column: str
    kind: IssueKind
    required: bool
    raw_value: Any  # 캐스팅 전 원시값 — quarantine에 남길 때 필요하다
    spec: ColumnSpec


@dataclass(frozen=True, slots=True)
class RunContext:
    """정책이 참조할 수 있는 실행 맥락."""

    source_id: str
    window_start: datetime
    window_end: datetime
    attempt: int
