"""정책 · 엔진 · loader가 공유하는 계약 타입.

구현 예정: docs/collector/implementation-issues.md #3
설계 근거: docs/collector/implementation-plan.md 5절 (정책 계약)

## 이 모듈의 역할

`Action` · `Issue` · `RowVerdict` · `RunContext`는 registry의 의존물이 아니라
**registry · policies · engine · config/loader 네 곳이 공유하는 어휘**다. 한곳에 모아
두면 import 방향이 일직선이 된다.

    types ← registry ← policies ← engine
          ↖ config/loader

타입만 필요한 모듈이 레지스트리 전체를 끌어오지 않고, 나중에 `libs/core`로 승격할 때
타입만 먼저 옮길 수 있다.

## 정의된 타입

- `Action` — 컬럼 정책의 반환 액션
  - `KEEP` — 반환값으로 치환하고 행을 유지한다
  - `DROP_ROW` — 이 행을 silver에서 제외하고 quarantine으로 보낸다
  - `FAIL_BATCH` — 배치 전체를 실패로 만든다
- `IssueKind` — `MISSING` · `TYPE_ERROR` · `OUTLIER`
- `Issue(column, kind, required, raw_value, spec)` — 판정 결과 하나. 컬럼 정책의 두 번째 인자로 그대로 전달된다.
  `raw_value`에는 **캐스팅 전 원시값**을 보관한다. quarantine에 "무엇이 왜 폐기됐는지"
  남기려면 이 값이 필요하다.
- `RowVerdict` — 행 정책의 최종 판정(유지 / 폐기)
- `RunContext` — 정책이 참조하는 실행 맥락(`source_id` · `window` · `attempt` 등)

## 주의

- 이 모듈은 collector의 다른 모듈을 import하지 않는다(pydantic과 표준 라이브러리는
  예외). 의존 방향의 최하단이라 여기서 위를 참조하면 순환이 생긴다.
- `ColumnSpec`을 타입 힌트로 쓰려면 `config.schema`를 참조해야 하는데, 그러면 config가
  validation을 다시 참조할 때 순환이 된다. `TYPE_CHECKING` 가드나 전방 참조로 처리한다.
- `Issue.kind`가 `TYPE_ERROR`일 때의 디스패치 규칙은 계획서 5절에 있다. outlier 계열
  정책을 쓰되 컬럼별 `on_outlier` 오버라이드는 적용하지 않는다. 컬럼 정책은 `issue.kind`를
  보고 자신의 호출 이유를 파악하고 그에 따라 방어한다.
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
    """판정 3단계가 내는 이슈의 종류."""

    MISSING = "missing"
    TYPE_ERROR = "type_error"
    OUTLIER = "outlier"


class RowVerdict(Enum):
    """행 정책의 최종 판정."""

    KEEP = "keep"
    DROP = "drop"


@dataclass(frozen=True, slots=True)
class Issue:
    """컬럼 하나의 판정 결과. 컬럼 정책의 두 번째 인자로 그대로 전달된다."""

    column: str
    kind: IssueKind
    required: bool
    raw_value: Any  # 캐스팅 전 원시값 — quarantine에 남길 때 필요하다
    spec: ColumnSpec


@dataclass(frozen=True, slots=True)
class RunContext:
    """정책이 참조할 수 있는 실행 맥락.

    `Window` 타입을 만들지 않고 datetime 두 개로 푼다. `Window`는 storage(#4) ·
    어댑터(#6) · pipeline(#7)이 함께 쓰는 개념이라 검증 계층 최하단에 두지 않는다.
    """

    source_id: str
    window_start: datetime
    window_end: datetime
    attempt: int
