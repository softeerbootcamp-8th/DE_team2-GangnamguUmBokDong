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
from pydantic import BaseModel, ConfigDict, model_validator

_MAPPINGS_DIR = Path(__file__).parent / "mappings"


class WindowSpec(BaseModel):
    """행이 속한 시간대를 어느 컬럼에서 어떻게 읽을지."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    from_column: str
    format: str


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
    na_values: tuple[str, ...] = ()
    column_map: dict[str, str] = {}
    value_map: dict[str, dict[str, str]] = {}

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
