"""데이터 수집 설정 파일의 명세를 정의하고, 
Pydantic을 활용해 사용자의 잘못된 입력을 차단하는 안전장치."""



from __future__ import annotations

import re
from datetime import timedelta
from typing import Annotated, Any, Literal

from pydantic import BaseModel, BeforeValidator, ConfigDict, Field, model_validator

# 예: 1d, 12h, 5m, 30s, 1d12h, 2h30m, 1d2h3m4s
_DURATION_RE = re.compile(r"(?:(\d+)d)?(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?")


def _parse_duration(value: object) -> timedelta:
    """YAML 파일에서 문자열로 입력한 값을 파이썬이 이해할 수 있는 시간 간격 객체로 변환한다."""
    
    if isinstance(value, timedelta):
        return value
    if not isinstance(value, str) or not value:
        raise ValueError(f"duration은 비어있지 않은 문자열이어야 합니다: {value!r}")

    match = _DURATION_RE.fullmatch(value)
    # 정규식의 모든 단위가 ?로 생략 가능하기 때문에 단위 값이 하나도 없는 경우를 확인한다.
    if match is None or not any(match.groups()):
        raise ValueError(
            f"duration 형식이 틀렸습니다.: {value!r}"
        )
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)

# Annotated는 기본 타입에 추가적인 메타데이터나 동작을 붙일 때 사용한다.
# BeforeValidator는 Pydantic이 값을 검증하기 전에 _parse_duration 함수를 실행하여 값을 변환한다.
Duration = Annotated[timedelta, BeforeValidator(_parse_duration)]


class Range(BaseModel):
    """컬럼의 정상 범위이다. min, max 둘 다 필수이다."""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float
    max: float

    @model_validator(mode="after")
    def _min_le_max(self) -> Range:
        """min이 max보다 큰 range를 막는다."""

        if self.min > self.max:
            raise ValueError(f"range.min({self.min})이 range.max({self.max})보다 클 수 없습니다.")
        return self


class ColumnSpec(BaseModel):
    """컬럼 하나의 스펙. range와 enum은 배타적으로만 사용할 수 있다."""

    # Pydantic 모델 configuration
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
        """range와 enum을 동시에 선언한 spec을 막는다."""
        if self.range is not None and self.enum is not None:
            raise ValueError("range와 enum을 동시에 선언할 수 없습니다.")
        return self


class Schedule(BaseModel):
    """수집 주기."""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    interval: Duration


class Storage(BaseModel):
    """bronze·silver 저장 형식과 파티션 키."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    bronze_format: str
    silver_format: str
    partition: tuple[str, ...] = Field(min_length=1)


class Quality(BaseModel):
    """배치를 버릴지 판단하는 완결성 기준."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_drop_ratio: float = Field(ge=0, le=1)
    max_missing_ratio: float = Field(default=0.0, ge=0, le=1)
    allow_empty: bool = False


class Fetch(BaseModel):
    """API 호출할 때, 얼마나 오랫동안 시도할지 설정하는 모델"""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    # API 호출에 사용할 수 있는 최대 시간
    # None으로 두어도, SourceConfig 모델에서 자동 설정됨.
    budget: Duration | None = None


class Backfill(BaseModel):
    """누락된 과거 데이터를 나중에 채워넣는 백필 작업에 대한 설정을 담당."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # 백필 기능을 켤지 끌지 결정하는 스위치
    enabled: bool = False

    # 과거 며칠 전의 데이터까지만 재수집할 것인지 설정
    # None으로 두어도, SourceConfig 모델에서 schedule.interval로 자동 설정됨.
    max_age: Duration | None = None

    @model_validator(mode="after")
    def _max_age_required_when_enabled(self) -> Backfill:
        """enabled=true인데 max_age가 없는 조합을 막는다."""
        
        # 무한정 과거로 거슬러 올라가며 재수집을 시도하여 과부하를 일으킬 가능성을 막기 위함
        if self.enabled and self.max_age is None:
            raise ValueError("backfill.enabled=true면 max_age가 필수다")
        return self


class Policies(BaseModel):
    """컬럼 정책과 행 정책을 정의하는 모델"""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # 컬럼 정책 (필수)
    required_missing: str
    required_outlier: str
    optional_missing: str
    optional_outlier: str

    # 행 정책 (선택)
    row: str | None = None
    row_params: dict[str, Any] | None = None


class SourceConfig(BaseModel):
    """모든 설정 모델들을 하나로 조립하는 최종 완성본.

    source_id: 소스 ID
    description: 소스에 대한 설명
    adapter: 어댑터 이름
    adapter_params: 어댑터 파라미터 (어댑터가 작동할 때 필요한 추가옵션들, 기본값은 {})
    schedule: 주기
    storage: 저장 방식
    quality: 품질 기준
    fetch: API 호출에 사용할 수 있는 최대 시간
    backfill: 백필
    policies: 컬럼 정책과 행 정책
    columns: 데이터 스키마(key=컬럼명, value=컬럼 스펙)
    config_version: 설정 버전
    """

    # Pydantic 모델 configuration
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
        """fetch.budget이 없으면 주기의 절반과 30분 중 작은 값을 기본 예산으로 쓴다."""

        if self.fetch and self.fetch.budget:
            return self.fetch.budget
        return min(self.schedule.interval / 2, timedelta(minutes=30))
