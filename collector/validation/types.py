"""데이터 검증 과정에서 여러 모듈들의 공통 언어를 정의해 둔 곳"""

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
    """컬럼 하나에 문제가 있을 때, 그 상황에 대한 페이로드를 만들어
    정책 함수로 전달하기 위한 객체

    column: 문제가 생긴 컬럼명
    kind: 문제 종류
    required: 필수 여부
    raw_value: 캐스팅 전 원시값
    spec: 컬럼 스펙

    엔진이 정책에 넘기는 value 
    MISSING: 정규화된 None
    TYPE_ERROR: 캐스팅에 실패한 원시값
    OUTLIER: 캐스팅에 성공한 값
    """

    column: str
    kind: IssueKind
    required: bool
    raw_value: Any  # 캐스팅 전 원시값 — quarantine에 남길 때 필요하다
    spec: ColumnSpec


@dataclass(frozen=True, slots=True)
class RunContext:
    """정책이 참조할 수 있는 실행 맥락.
    
    source_id: 데이터 소스 식별자
    window_start: 배치 시작 시각
    window_end: 배치 종료 시각
    attempt: 재시도 횟수
    """

    source_id: str
    window_start: datetime
    window_end: datetime
    attempt: int
