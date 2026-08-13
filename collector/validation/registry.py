"""YAML에 적힌 정책 이름을 @policy/@row_policy로 등록된 실제 함수로 매핑·조회해주는 이름 레지스트리입니다.

@policy / @row_policy 데코레이터와 이름→함수 매핑.

## 이 모듈의 역할

YAML에 적힌 문자열을 실제 함수로 바꾸는 지점이다. 새 정책이 필요하면 함수 하나를
추가하고 YAML에서 이름으로 부른다. 엔진 코드는 건드리지 않는다.

계약 타입(`Action` · `Issue` · `RowVerdict` · `RunContext`)은 `validation/types.py`에
있다. 이 모듈은 등록과 조회만 담당한다.

## 데코레이터 2종
- `@policy(name)` — 컬럼 정책. `(value, issue, row, ctx) -> tuple[Any, Action]`.
   두 번째 인자가 `Issue`인 이유는 정책이 `issue.kind`로 자신이 왜 호출됐는지 알아야 교정형이 캐스팅 실패 값을 방어할 수 있기 때문이다.
- `@row_policy(name, params=None)` — 행 정책.
  `(row, issues, ctx, params) -> RowVerdict`

**두 종류를 별도 매핑에 담는다.** 계약(인자 개수 · 반환 타입)이 다르므로 섞어 조회하면
실행 중에 터진다.

`params`는 그 정책이 config의 `policies.row_params`로 받을 인자의 pydantic 모델이다.
파라미터가 없는 정책은 생략한다.

    class IssueCountParams(BaseModel):
        max_issues: int

    @row_policy("drop_if_issue_count_exceeds", params=IssueCountParams)
    def drop_if_issue_count_exceeds(row, issues, ctx, params): ...

## 조회 API

- `get_policy(name)` · `get_row_policy(name)` — 엔진이 디스패치할 때 쓴다.
- 존재 확인 함수 — loader가 기동 시 config의 정책 이름을 검증할 때 쓴다. 함수를
  실행하지 않고 등록 여부만 본다.
- `get_row_policy_params_model(name)` — 그 정책에 등록된 params 모델(없으면 None).
  loader가 `row_params`를 검증할 때 쓴다.

## 주의

- 같은 이름을 두 번 등록하면 예외로 막는다. 조용히 덮어쓰면 어느 함수가 실제로 돌았는지
  알 수 없게 된다.
- 등록은 **import 부수효과**다. `validation.policies`가 import되지 않으면 레지스트리가
  비어 있다. 이 보장은 `validation/__init__.py`가 맡는다.
- 정책 이름은 manifest의 `policy_actions` 키로 남는다. 이름을 바꾸면 과거 manifest와
  대조가 끊기므로 개명은 신중히 한다.
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
    """행 정책과 그 정책이 받는 params 모델. `params_model`이 None이면 인자를 받지 않는다."""

    fn: RowPolicyFn
    params_model: type[BaseModel] | None


_POLICIES: dict[str, PolicyFn] = {}
_ROW_POLICIES: dict[str, RowPolicyEntry] = {}


def policy(name: str) -> Callable[[PolicyFn], PolicyFn]:
    """컬럼 정책을 등록한다. 원본 함수를 그대로 반환한다(래핑하지 않는다)."""

    def register(fn: PolicyFn) -> PolicyFn:
        if name in _POLICIES:
            raise DuplicatePolicyError(
                f"컬럼 정책 '{name}'이 이미 등록돼 있다: {_POLICIES[name].__qualname__}"
            )
        _POLICIES[name] = fn
        return fn

    return register


def row_policy(
    name: str, *, params: type[BaseModel] | None = None
) -> Callable[[RowPolicyFn], RowPolicyFn]:
    """행 정책을 등록한다. `params`는 config의 `policies.row_params`를 검증할 모델이다."""

    def register(fn: RowPolicyFn) -> RowPolicyFn:
        if name in _ROW_POLICIES:
            raise DuplicatePolicyError(
                f"행 정책 '{name}'이 이미 등록돼 있다: {_ROW_POLICIES[name].fn.__qualname__}"
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
    """그 정책에 등록된 params 모델. None은 '등록됐고 params를 받지 않는다'는 뜻이다."""
    return _row_entry(name).params_model


def is_policy_registered(name: str) -> bool:
    """컬럼 정책 이름이 등록돼 있는지 본다. 함수를 실행하지 않는다."""
    return name in _POLICIES


def is_row_policy_registered(name: str) -> bool:
    """행 정책 이름이 등록돼 있는지 본다. 함수를 실행하지 않는다."""
    return name in _ROW_POLICIES


def policy_names() -> tuple[str, ...]:
    """등록된 컬럼 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_POLICIES))


def row_policy_names() -> tuple[str, ...]:
    """등록된 행 정책 이름을 정렬된 튜플로 반환한다."""
    return tuple(sorted(_ROW_POLICIES))


def _row_entry(name: str) -> RowPolicyEntry:
    """이름으로 행 정책 항목(함수 + params 모델)을 조회한다."""
    try:
        return _ROW_POLICIES[name]
    except KeyError:
        raise UnknownPolicyError(_unknown_message("행 정책", name, row_policy_names())) from None


def _unknown_message(kind: str, name: str, registered: tuple[str, ...]) -> str:
    """미등록 이름 조회 시 예외 메시지를 만든다."""
    listed = ", ".join(registered) or "(없음)"
    return f"{kind} '{name}'이 등록돼 있지 않다. 등록된 이름: {listed}"
