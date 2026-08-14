"""YAML 설정 파일에 명시된 정책 이름을 실제 파이썬 검증 함수로 연결해 주는 레지스트리이다.

데이터 수집 시 문자열로 작성된 정책 이름을 파이썬 함수 객체로 매핑합니다.
컬럼 정책(`@policy`)과 파라미터가 필요한 행 정책(`@row_policy`)을 분리하여 관리하며,
외부 모듈이 오타를 검증하거나 요구 파라미터를 조회할 수 있도록 돕는 역할을 합니다.

주의:
- 중복 방지: 같은 이름의 정책을 중복 등록하면 예외가 발생하여 조용한 덮어쓰기를 방지합니다.
- 부수 효과: 함수 등록은 데코레이터의 import 부수효과로 동작하므로, 사전에 함수들이 import 되어야 합니다.
- 영구 기록: 정책 이름은 manifest에 영구 기록되므로 정책 이름을 바꿀 때는 신중해야 합니다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from validation.types import Action, Issue, RowVerdict, RunContext

if TYPE_CHECKING:
    from pydantic import BaseModel

PolicyFn = Callable[[Any, Issue, dict, RunContext], tuple[Any, Action]]
RowPolicyFn = Callable[[dict, list[Issue], RunContext, Any], RowVerdict]


class UnknownPolicyError(ValueError):
    """등록되지 않은 정책 이름을 조회했다."""


class DuplicatePolicyError(ValueError):
    """같은 이름을 두 번 등록했다. 조용히 덮어쓰면 어느 함수가 돌았는지 알 수 없다."""


@dataclass(frozen=True, slots=True)
class RowPolicyEntry:
    """파라미터가 필요한 정책을 등록할 때에는 모델을 넣어두고, 
    파라미터가 필요 없는 정책을 등록할 때는 param_model에 None을 넣는다."""

    fn: RowPolicyFn
    params_model: type[BaseModel] | None


_POLICIES: dict[str, PolicyFn] = {}
_ROW_POLICIES: dict[str, RowPolicyEntry] = {}


def policy(name: str) -> Callable[[PolicyFn], PolicyFn]:
    """컬럼 정책을 등록한다. 원본 함수를 그대로 반환한다."""

    def register(fn: PolicyFn) -> PolicyFn:
        if name in _POLICIES:
            raise DuplicatePolicyError(
                f"컬럼 정책 '{name}'이 이미 등록돼 있습니다: {_POLICIES[name].__qualname__}"
            )
        _POLICIES[name] = fn
        return fn

    return register


def row_policy(
    name: str, *, params: type[BaseModel] | None = None
) -> Callable[[RowPolicyFn], RowPolicyFn]:
    """행 정책을 등록한다. 원본 함수를 그대로 반환한다."""

    def register(fn: RowPolicyFn) -> RowPolicyFn:
        if name in _ROW_POLICIES:
            raise DuplicatePolicyError(
                f"행 정책 '{name}'이 이미 등록돼 있습니다: {_ROW_POLICIES[name].fn.__qualname__}"
            )
        _ROW_POLICIES[name] = RowPolicyEntry(fn=fn, params_model=params)
        return fn

    return register


def get_policy(name: str) -> PolicyFn:
    """등록된 컬럼 정책을 이름으로 조회한다."""
    try:
        return _POLICIES[name]
    except KeyError:
        raise UnknownPolicyError(_unknown_message("컬럼 정책", name, policy_names())) from None


def get_row_policy(name: str) -> RowPolicyFn:
    """등록된 행 정책을 이름으로 조회한다."""
    return _row_entry(name).fn


def get_row_policy_params_model(name: str) -> type[BaseModel] | None:
    """행 정책이 파라미터를 필요로 하는지, 필요하다면 어떤 Pydantic 모델을 써야하는가를 반환해준다."""
    return _row_entry(name).params_model


def is_policy_registered(name: str) -> bool:
    """컬럼 정책 이름이 등록돼 있는지 본다."""
    return name in _POLICIES


def is_row_policy_registered(name: str) -> bool:
    """행 정책 이름이 등록돼 있는지 본다."""
    return name in _ROW_POLICIES


def policy_names() -> tuple[str, ...]:
    """등록된 컬럼 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_POLICIES))


def row_policy_names() -> tuple[str, ...]:
    """등록된 행 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_ROW_POLICIES))


def _row_entry(name: str) -> RowPolicyEntry:
    """이름으로 행 정책 항목을 조회한다."""
    try:
        return _ROW_POLICIES[name]
    except KeyError:
        raise UnknownPolicyError(_unknown_message("행 정책", name, row_policy_names())) from None


def _unknown_message(kind: str, name: str, registered: tuple[str, ...]) -> str:
    """미등록 이름 조회 시 예외 메시지를 만든다."""
    listed = ", ".join(registered) or "(없음)"
    return f"{kind} '{name}'이 등록돼 있지 않습니다. 등록된 이름: {listed}"
