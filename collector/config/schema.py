"""데이터 소스 설정 파일(sources/{source_id}.yaml)의 구조를 정의하고 검증(Validation)하는 Pydantic 모델.
SourceConfig / ColumnSpec / Policies pydantic 모델.

`sources/{source_id}.yaml`이 가질 수 있는 모양을 못박는다. 구조적으로 말이 안 되는
조합(부분 range, range+enum 동시 선언, backfill 조합 오류)은 여기서 pydantic
validator로 막는다. 레지스트리 조회가 필요한 검증(정책 이름 존재, row_params)은
`config/loader.py`가 맡는다.
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
    """컬럼의 정상 범위. `min`·`max` 둘 다 필수
    부분 선언은 loader가 아니라 필드 정의 자체로 거부"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float
    max: float

    @model_validator(mode="after")
    def _min_le_max(self) -> Range:
        """`min`이 `max`보다 큰 range를 막는다."""
        if self.min > self.max:
            raise ValueError(f"range.min({self.min})이 range.max({self.max})보다 클 수 없다")
        return self


class ColumnSpec(BaseModel):
    """컬럼 하나의 스펙. `range`와 `enum`은 배타적으로만 사용"""

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
        """`range`와 `enum`을 동시에 선언한 spec을 막는다."""
        if self.range is not None and self.enum is not None:
            raise ValueError("range와 enum을 동시에 선언할 수 없다")
        return self


class Schedule(BaseModel):
    """수집 주기."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    interval: Duration


class Storage(BaseModel):
    """bronze·silver 저장 형식과 파티션 키."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bronze_format: str
    silver_format: str
    partition: tuple[str, ...] = Field(min_length=1)


class Quality(BaseModel):
    """배치를 버릴지 판단하는 완결도 기준."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_drop_ratio: float = Field(ge=0, le=1)
    max_missing_ratio: float = Field(default=0.0, ge=0, le=1)
    allow_empty: bool = False


class Fetch(BaseModel):
    """API 호출 예산. `budget`이 없으면 `SourceConfig.effective_fetch_budget`이 기본값을 계산한다."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    budget: Duration | None = None


class Backfill(BaseModel):
    """`_retry_queue` 백필 대상 여부와 만료 기준."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    enabled: bool = False
    max_age: Duration | None = None

    @model_validator(mode="after")
    def _max_age_required_when_enabled(self) -> Backfill:
        """`enabled=true`인데 `max_age`가 없는 조합을 막는다."""
        if self.enabled and self.max_age is None:
            raise ValueError("backfill.enabled=true면 max_age가 필수다")
        return self


class Policies(BaseModel):
    """4분면 기본 정책 이름과, 선택적인 행 정책·그 params."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    required_missing: str
    required_outlier: str
    optional_missing: str
    optional_outlier: str
    row: str | None = None
    row_params: dict[str, Any] | None = None


class SourceConfig(BaseModel):
    """`sources/{source_id}.yaml` 한 개에 대응하는 검증된 설정."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    source_id: str
    description: str
    adapter: str
    adapter_params: dict[str, Any] = {}
    schedule: Schedule
    storage: Storage
    quality: Quality
    fetch: Fetch | None = None
    backfill: Backfill | None = None
    policies: Policies
    columns: dict[str, ColumnSpec]
    config_version: str = ""

    def effective_fetch_budget(self) -> timedelta:
        """`fetch.budget`이 없으면 주기의 절반과 30분 중 작은 값을 기본 예산으로 쓴다."""
        if self.fetch and self.fetch.budget:
            return self.fetch.budget
        return min(self.schedule.interval / 2, timedelta(minutes=30))
