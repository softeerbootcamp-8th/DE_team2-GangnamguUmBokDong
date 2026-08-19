"""bootstrap 매핑 설정 스키마와 로더.

운영 설정(`collector/sources/*.yaml`)과 파일을 나눈 이유는 **수명**이다. bootstrap
설정은 한 번 쓰고 버리는데, 운영 yaml은 5분마다 읽히고 오래 유지된다. 다 쓴 25줄이
운영 파일에 영구히 남으면 나중에 읽는 사람이 "이건 지금도 쓰이나"를 매번 판단해야 한다.

검증에 필요한 `columns`·`policies`는 여기 없다 — 그건 운영 yaml의 것이고,
`config.loader.load()`로 따로 가져와 합쳐 쓴다.
"""

from __future__ import annotations

from pathlib import Path
from typing import Literal

import yaml
from pydantic import BaseModel, ConfigDict, Field, model_validator

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


class WindowSpec(BaseModel):
    """행이 속한 시간대를 어느 컬럼에서 어떻게 읽을지."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_column: str
    format: str


class DerivedTimeSpec(BaseModel):
    """시각 컬럼 하나를 여러 물리 컬럼으로 분해하는 규칙.

    `column_map`은 CSV 헤더 하나를 물리 컬럼 하나로 옮기므로, 원본이 시각을 한 컬럼에
    담고 collector가 날짜·시각을 나눠 갖는 경우를 표현할 수 없다. 기상청 ASOS는
    `일시`(`2026-06-01 00:00`) 하나인데 `weather_ultra_short_live`는 `baseDate`·
    `baseTime`을 각각 required로 요구한다.

    `parse`로 읽어 `into`의 각 형식으로 다시 찍는다. 임의 표현식이 아니라 strftime
    재포맷만 허용한다 — 설정 파일이 코드가 되는 것을 막기 위해서다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    parse: str
    into: dict[str, str] = Field(min_length=1)


class DerivedWindSpec(BaseModel):
    """풍속·풍향에서 동서(`UUU`)·남북(`VVV`) 성분을 계산하는 규칙.

    운영 수집(`getUltraSrtNcst`)은 두 성분을 이미 계산해서 주는데 과거 CSV(ASOS
    시간자료)에는 없다. 같은 공식으로 채워 컬럼을 맞춘다 — 발명이 아니라 가진 값의
    무손실 변환이고, 실API 대조 검증 결과가 `core.wind` docstring에 있다.

    `derived_time`과 마찬가지로 임의 표현식이 아니라 **이 변환 하나만** 허용한다.
    설정 파일이 코드가 되면 값의 유래를 코드에서 찾을 수 없게 된다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    # 입력 물리 컬럼명(매핑 후 이름).
    speed: str
    direction: str
    # 결과를 담을 물리 컬럼명.
    u: str
    v: str


class BootstrapConfig(BaseModel):
    """소스 하나의 초기 로드 설정.

    `kind`에 따라 쓰는 필드가 갈린다. 한 모델에 둔 이유는 공통 필드(`window`)가 있고
    파일 하나로 읽히는 게 자연스럽기 때문이다. 필수 여부는 validator가 가른다.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["csv", "history_api"]
    window: WindowSpec
    dedup: bool = False

    # kind: csv
    encoding: str = "utf-8"
    # 이 소스의 CSV만 고르는 glob 패턴. 한 디렉터리에 여러 소스의 CSV가 섞여 있을 수
    # 있는데(실제로 `data/`에 따릉이 대여이력과 ASOS 기상자료가 함께 있었다), 파일명
    # YYMM이 요청 범위와 겹치면 다른 소스의 파일을 열어 시각 컬럼 파싱에서 죽는다.
    # 기본값은 기존 동작(디렉터리의 모든 CSV)을 유지한다.
    file_pattern: str = "*.csv"
    na_values: tuple[str, ...] = ()
    column_map: dict[str, str] = {}
    value_map: dict[str, dict[str, str]] = {}
    # CSV에 아예 없는 물리 컬럼을 고정값으로 채운다. 원본이 단일 관측 지점인데
    # collector가 격자 좌표(nx·ny)를 required로 요구하는 경우에 쓴다.
    # 값을 문자열로 두는 이유는 csv_source가 전 컬럼을 문자열로 넘기고 캐스팅은
    # 검증 엔진의 `types`가 맡는다는 규약을 지키기 위해서다.
    constants: dict[str, str] = {}
    # 시각 컬럼 하나를 여러 물리 컬럼으로 분해한다(키는 CSV 헤더).
    derived_time: dict[str, DerivedTimeSpec] = {}
    # 풍속·풍향에서 UUU·VVV를 계산한다.
    derived_wind: DerivedWindSpec | None = None

    # kind: history_api
    service: str | None = None
    time_format: str | None = None
    page_size: int = 1000

    @model_validator(mode="after")
    def _require_kind_fields(self) -> BootstrapConfig:
        if self.kind == "csv" and not self.column_map:
            raise ValueError("kind=csv면 column_map이 필수다")
        if self.kind == "history_api":
            if not self.service:
                raise ValueError("kind=history_api면 service가 필수다")
            if not self.time_format:
                raise ValueError("kind=history_api면 time_format이 필수다")
        return self

    @model_validator(mode="after")
    def _no_duplicate_targets(self) -> BootstrapConfig:
        """한 물리 컬럼을 두 경로가 채우는 설정을 막는다.

        column_map·constants·derived_time이 같은 컬럼을 노리면 어느 값이 남는지가
        적용 순서에 달리게 된다. 조용히 이기는 쪽이 생기는 대신 설정 오류로 끊는다.
        """
        seen: dict[str, str] = {}
        sources = [
            ("column_map", self.column_map.values()),
            ("constants", self.constants.keys()),
            *(
                (f"derived_time[{header}]", spec.into.keys())
                for header, spec in self.derived_time.items()
            ),
            *(
                [("derived_wind", (self.derived_wind.u, self.derived_wind.v))]
                if self.derived_wind is not None
                else []
            ),
        ]
        for origin, targets in sources:
            for target in targets:
                if target in seen:
                    raise ValueError(
                        f"물리 컬럼 '{target}'을 {seen[target]}과 {origin}이 함께 채운다"
                    )
                seen[target] = origin
        return self


def load(source_id: str) -> BootstrapConfig:
    """해당 소스의 bootstrap 설정을 읽는다.

    args:
        source_id: 소스 id. `mappings/{source_id}.yaml`을 찾는다.
    returns:
        검증된 설정
    raises:
        FileNotFoundError: 그 소스의 매핑 파일이 없을 때. 초기 로드 대상이 아니라는 뜻이다.
    """
    path = _MAPPINGS_DIR / f"{source_id}.yaml"
    if not path.exists():
        raise FileNotFoundError(f"bootstrap 매핑 설정이 없다: {path}")
    return BootstrapConfig.model_validate(yaml.safe_load(path.read_text(encoding="utf-8")))
