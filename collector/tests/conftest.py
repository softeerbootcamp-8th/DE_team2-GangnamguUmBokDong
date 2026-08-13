"""#3 테스트 공용 픽스처.

`ColumnSpecStub`은 #2가 만들 `config.schema.ColumnSpec`의 자리를 메우는 스텁이다.
**#2에서 `ColumnSpec`이 확정되면 이 스텁을 실제 모델로 교체한다** — 방치하면 정책
테스트가 실물과 어긋난 채 초록불이 된다.
"""

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pytest

from validation import registry
from validation.types import Issue, RunContext


@dataclass(frozen=True, slots=True)
class RangeStub:
    min: Any
    max: Any


@dataclass(frozen=True, slots=True)
class ColumnSpecStub:
    types: tuple[str, ...] = ("int",)
    required: bool = False
    range: RangeStub | None = None
    enum: tuple[Any, ...] | None = None
    default: Any = None


@pytest.fixture
def make_spec():
    """ColumnSpec 스텁을 만든다. `range=(0, 200)` 처럼 튜플로 주면 RangeStub으로 감싼다."""

    def _make(types=("int",), required=False, range=None, enum=None, default=None):
        bounds = RangeStub(*range) if isinstance(range, tuple) else range
        return ColumnSpecStub(types=types, required=required, range=bounds, enum=enum, default=default)

    return _make


@pytest.fixture
def make_issue(make_spec):
    """Issue를 만든다. `required`를 생략하면 spec의 값을 따른다."""

    def _make(kind, spec=None, column="col", raw=None, required=None):
        spec = make_spec() if spec is None else spec
        return Issue(
            column=column,
            kind=kind,
            required=spec.required if required is None else required,
            raw_value=raw,
            spec=spec,
        )

    return _make


@pytest.fixture
def ctx():
    return RunContext(
        source_id="bike_station_realtime",
        window_start=datetime(2026, 8, 12, 14, 10, tzinfo=UTC),
        window_end=datetime(2026, 8, 12, 14, 15, tzinfo=UTC),
        attempt=1,
    )


@pytest.fixture
def clean_registry():
    """레지스트리는 전역 상태다. 테스트가 등록한 이름이 다른 테스트로 새지 않게 복원한다."""
    saved_policies = dict(registry._POLICIES)
    saved_row_policies = dict(registry._ROW_POLICIES)
    yield registry
    registry._POLICIES.clear()
    registry._POLICIES.update(saved_policies)
    registry._ROW_POLICIES.clear()
    registry._ROW_POLICIES.update(saved_row_policies)
