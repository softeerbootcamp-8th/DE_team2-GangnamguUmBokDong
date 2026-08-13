"""SourceConfig / ColumnSpec / Policies pydantic 모델.

`sources/{source_id}.yaml`이 가질 수 있는 모양을 못박는다. 구조적으로 말이 안 되는
조합(부분 range, range+enum 동시 선언, backfill 조합 오류)은 여기서 pydantic
validator로 막는다. 레지스트리 조회가 필요한 검증(정책 이름 존재, row_params)은
`config/loader.py`가 맡는다.

설계 근거: docs/superpowers/specs/2026-08-13-collector-config-loader-design.md
"""

from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

_DURATION_RE = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def _parse_duration(value: object) -> timedelta:
    """`"5m"`·`"3h"`·`"1d"`·`"2m30s"` 같은 문자열을 timedelta로 바꾼다."""
    if isinstance(value, timedelta):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"duration은 비어있지 않은 문자열이어야 한다: {value!r}")
    match = _DURATION_RE.fullmatch(value)
    if match is None or not any(match.groups()):
        raise ValueError(
            f"duration 형식이 아니다(예: '5m', '3h', '1d', '2m30s'): {value!r}"
        )
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


Duration = Annotated[timedelta, BeforeValidator(_parse_duration)]


class Range(BaseModel):
    """컬럼의 정상 범위. `min`·`max` 둘 다 필수다 — 부분 선언은 loader가 아니라
    필드 정의 자체로 거부한다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float
    max: float


class ColumnSpec(BaseModel):
    """컬럼 하나의 스펙. `range`와 `enum`은 배타적으로만 쓴다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    types: tuple[Literal["str", "int", "float", "bool"], ...] = Field(min_length=1)
    required: bool = False
    range: Range | None = None
    enum: tuple[Any, ...] | None = None
    on_missing: str | None = None
    on_outlier: str | None = None
    default: Any = None

    @model_validator(mode="after")
    def _range_xor_enum(self) -> ColumnSpec:
        if self.range is not None and self.enum is not None:
            raise ValueError("range와 enum을 동시에 선언할 수 없다")
        return self
