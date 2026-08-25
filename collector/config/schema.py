"""데이터 수집 설정 파일의 명세를 정의하고, Pydantic을 활용해 사용자의 잘못된 입력을 차단하는 안전장치."""

from __future__ import annotations

import math
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
        raise ValueError(f"duration 형식이 틀렸습니다.: {value!r}")
    days, hours, minutes, seconds = (int(g) if g else 0 for g in match.groups())
    return timedelta(days=days, hours=hours, minutes=minutes, seconds=seconds)


# Annotated는 기본 타입에 추가적인 메타데이터나 동작을 붙일 때 사용한다.
# BeforeValidator는 Pydantic이 값을 검증하기 전에 _parse_duration 함수를 실행하여 값을 변환한다.
Duration = Annotated[timedelta, BeforeValidator(_parse_duration)]


class Range(BaseModel):
    """컬럼의 정상 범위이다. 한쪽 경계만 선언한 열린 범위도 허용한다."""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    min: float = -math.inf
    max: float = math.inf

    @model_validator(mode="after")
    def _min_le_max(self) -> Range:
        """경계가 하나도 없거나 min이 max보다 큰 range를 막는다."""

        if not self.model_fields_set.intersection({"min", "max"}):
            raise ValueError("range는 min 또는 max 중 하나 이상을 선언해야 합니다.")
        if self.min > self.max:
            raise ValueError(
                f"range.min({self.min})이 range.max({self.max})보다 클 수 없습니다."
            )
        return self


class ColumnSpec(BaseModel):
    """컬럼 하나의 스펙. range와 enum은 배타적으로만 사용할 수 있다."""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    # "precip"은 기상청 강수량 범주 표기("강수없음", "30.0~50.0mm" 등)를 mm 실수로
    # 바꾸는 전용 캐스터다. 규칙은 `core.precip`에 있고 loader와 공유한다.
    # "snow"는 적설 표기("적설없음", "1.0~4.9cm" 등)를 cm 실수로 바꾼다 — 형태는
    # 같지만 단위가 달라 서로의 표기를 받지 않는다(`core._amount` 참고).
    # "masked_float"는 생활인구의 비식별 마스킹(`*`)을 결측으로 판정시킨다
    # (`core.masked` 참고).
    types: tuple[
        Literal["str", "int", "float", "bool", "precip", "snow", "masked_float"], ...
    ] = Field(min_length=1)
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


class Compaction(BaseModel):
    """하루치 silver를 archive로 묶을 때의 소스별 옵션."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    # 윈도우마다 같은 구간을 다시 받는 소스에서 켠다. path_suffix가 날짜 단위인데
    # 주기가 그보다 짧으면 윈도우끼리 같은 기록을 중복 수집한다(bike_rental_history).
    #
    # 스냅샷 소스에는 켜면 안 된다 — 재고가 안 변하면 연속 윈도우가 같은 값을 내는
    # 것이 정상인데, 그걸 지우면 시계열이 무너진다.
    dedup: bool = False


class Quality(BaseModel):
    """배치를 버릴지 판단하는 완결성 기준."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    max_drop_ratio: float = Field(ge=0, le=1)
    max_missing_ratio: float = Field(default=0.0, ge=0, le=1)
    allow_empty: bool = False

    # 소스가 알려준 전체 건수보다 실제 받은 행이 많을 때 용인할 비율.
    #
    # 진행 중인 window를 조회하면 API가 같은 본문 안에서 list_total_count보다 많은
    # row를 주는 일이 있다(실측 2026-08-23: 대여이력 rows=989 expected=988, 4시간에
    # 5회). 카운트 계산과 직렬화 사이에 레코드가 들어온 경우이고, 그 초과분은 누락이
    # 아니라 실제 데이터다. 이걸 실패로 확정하면 window 하나가 통째로 버려지고 그
    # tick의 서빙 게시까지 함께 죽는다.
    #
    # 그래서 기본값을 0.1로 둔다 — 막아야 할 사고(페이지 중복 병합)는 최소 +100%라
    # 이 값으로도 계속 걸린다. 대신 10% 안쪽의 초과는 어떤 소스에서든 통과하므로,
    # snapshot 성격 소스에서 옛 payload와 새 total이 섞인 상태가 그 범위에 들면
    # 잡아내지 못한다는 것을 감수한다.
    max_overfetch_ratio: float = Field(default=0.1, ge=0, le=1)


class Fetch(BaseModel):
    """API 호출 예산과 fetch 실패 뒤 재수집 방식을 설정하는 모델."""

    # Pydantic 모델 configuration
    model_config = ConfigDict(extra="forbid", frozen=True)

    # API 호출에 사용할 수 있는 최대 시간
    # None으로 두어도, SourceConfig 모델에서 자동 설정됨.
    budget: Duration | None = None

    # 한 실행 안의 transient round와 Airflow가 같은 window를 다시 실행할 때 적용한다.
    # 현재값·페이지 경계가 바뀌는 API는 전체를 다시 받고, logical window로 발표본과
    # 조각 key가 고정되는 API만 누락 조각을 이어 받는다.
    retry_mode: Literal["refetch_all", "retry_missing"] = "refetch_all"


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
    compaction: 하루치 압축 옵션
    policies: 컬럼 정책과 행 정책
    columns: 데이터 스키마(key=컬럼명, value=컬럼 스펙)
    natural_key: 원천 snapshot에서 행별 identity를 이루는 컬럼
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
    compaction: Compaction | None = None
    policies: Policies
    columns: dict[str, ColumnSpec]
    natural_key: tuple[str, ...] | None = Field(default=None, min_length=1)
    config_version: str = ""

    def effective_fetch_budget(self) -> timedelta:
        """fetch.budget이 없으면 주기의 절반과 30분 중 작은 값을 기본 예산으로 쓴다."""

        if self.fetch and self.fetch.budget:
            return self.fetch.budget
        return min(self.schedule.interval / 2, timedelta(minutes=30))

    def effective_fetch_retry_mode(self) -> Literal["refetch_all", "retry_missing"]:
        """fetch 설정이 없으면 안전한 전체 재수집 정책을 반환한다."""

        return self.fetch.retry_mode if self.fetch else "refetch_all"
