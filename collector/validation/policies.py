"""데이터 이슈 발견 시 값을 어떻게 처리할지 정의한 공통 정책 함수 모음이다.

특정 데이터 소스에 종속되는 지식을 배제하여, 소스가 늘어나더라도 공통 코드는 유지되도록 설계되었습니다.
소스별 처리 방식의 차이는 모두 YAML 설정 파일(4분면 기본값 + 컬럼별 오버라이드)로 분리하여 제어합니다.

`Action`, `Issue`, `RowVerdict`, `RunContext`는 `validation/types.py`에서,
데코레이터는 `validation/registry.py`에서 가져와 사용합니다.

## 컬럼 정책 7종: `(value, issue, row, ctx) -> tuple[Any, Action]`

엔진이 넘기는 `value`는 `MISSING`이면 정규화된 `None`, `TYPE_ERROR`면 캐스팅 실패
원시값, `OUTLIER`면 캐스팅된 값이다. 원시값은 항상 `issue.raw_value`에 있다.

| 이름 | 반환값 | Action | repaired | 방어 가드 |
| --- | --- | --- | --- | --- |
| `keep_null` | 원래 값 그대로 | KEEP | 아니오 | — |
| `set_null` | None | KEEP | 예 | — |
| `fill_zero` | 0 | KEEP | 예 | `TYPE_ERROR` 제외 |
| `fill_default` | spec에 선언된 기본값 | KEEP | 예 | `TYPE_ERROR` 제외 |
| `clip_to_range` | 정상 범위의 경계로 자른 값 | KEEP | 예 | `TYPE_ERROR` · `MISSING` · `range` 미선언 · `min`/`max` 부분 선언 제외 |
| `drop_row` | — | DROP_ROW | — (행 폐기) | — |
| `fail_batch` | — | FAIL_BATCH | — (배치 실패) | — |

### 교정형 정책은 캐스팅 실패 값을 방어한다

정책은 값의 타입을 되짚지 않고 **`issue.kind`를 본다.** `clip_to_range` · `fill_zero` ·
`fill_default`는 `TYPE_ERROR`일 때 `(None, Action.KEEP)`을 돌려준다. `clip_to_range`는
가드 한 줄로 `TYPE_ERROR` · `MISSING` · `range` 미선언 · `min`/`max` 부분 선언 네 경우를
함께 막는다.

결과적으로 `set_null`과 같은 효과가 되어 Parquet 스키마가 깨지지 않고, 규칙을 새로 만들지
않고 구현 안에서 흡수된다.

## 행 정책 3종 — `(row, issues, ctx, params) -> RowVerdict`

- `drop_if_any_required_issue` — 필수 컬럼에 문제가 하나라도 있으면 행을 폐기한다.
  params 없음.
- `drop_if_issue_count_exceeds` — 이슈 개수가 `params.max_issues`를 넘으면 폐기한다.
  params 모델을 정의해 `@row_policy(..., params=IssueCountParams)`로 등록한다.
- `keep_always` — 항상 유지한다. params 없음.

params가 없는 정책도 시그니처는 4인자로 통일하고 `params`를 무시한다. 엔진이 정책마다
호출 방식을 분기하지 않게 하려는 것이다.

## 주의

- `keep_null`과 `set_null`을 구분한다. 전자는 값을 바꾸지 않으므로 `repaired`가 아니고,
  후자는 값을 바꾸므로 `repaired`다. 이 기준을 엔진의 `_row_status` 판정과 맞춘다.
- `fail_batch`는 최후 수단이다. 4분면 기본값에 걸면 한 행 때문에 배치 전체가 죽는다.
- 정책은 순수 함수로 둔다. 로그를 남기거나 S3에 접근하지 않는다.
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from validation.registry import policy, row_policy
from validation.types import Action, Issue, IssueKind, RowVerdict, RunContext


@policy("keep_null")
def keep_null(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 바꾸지 않고 유지한다."""

    # _row_status: ok
    return value, Action.KEEP


@policy("set_null")
def set_null(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 None으로 교체한다."""

    # _row_status: repaired
    return None, Action.KEEP


@policy("fill_zero")
def fill_zero(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 0으로 채운다. 해석 불가한 값에는 적용하지 않는다."""

    # 무지성으로 0을 넣지 않는다, 타입 에러라면 0으로 채우지 않고 안전하게 None으로 반환.
    if issue.kind is IssueKind.TYPE_ERROR:
        return None, Action.KEEP
    return 0, Action.KEEP


@policy("fill_default")
def fill_default(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """spec에 선언된 기본값으로 채운다.

    `default`를 선언하지 않은 컬럼에서는 None이 되어 `set_null`과 같은 효과다.
    """
    if issue.kind is IssueKind.TYPE_ERROR:
        return None, Action.KEEP
    return issue.spec.default, Action.KEEP


@policy("clip_to_range")
def clip_to_range(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """값을 정상 범위의 경계로 잘라낸다."""
    
    bounds = issue.spec.range
    if (
        issue.kind is not IssueKind.OUTLIER    # 이상치가 아닌 경우
        or bounds is None                      # 범위 스펙이 없는 경우
        or bounds.min is None                  # 최소값이 없는 경우 
        or bounds.max is None                  # 최대값이 없는 경우     
    ):
        return None, Action.KEEP
    return min(max(value, bounds.min), bounds.max), Action.KEEP


@policy("drop_row")
def drop_row(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """이 행을 silver에서 제외하고 quarantine으로 보낸다."""
    return value, Action.DROP_ROW


@policy("fail_batch")
def fail_batch(value: Any, issue: Issue, row: dict, ctx: RunContext) -> tuple[Any, Action]:
    """배치 전체를 실패로 만든다. 스키마 계약 위반처럼 데이터 전체를 의심할 때만 쓴다."""
    return value, Action.FAIL_BATCH


class IssueCountParams(BaseModel):
    """drop_if_issue_count_exceeds의 인자."""
    model_config = ConfigDict(extra="forbid", frozen=True)
    max_issues: int = Field(ge=0)


@row_policy("drop_if_any_required_issue")
def drop_if_any_required_issue(
    row: dict, issues: list[Issue], ctx: RunContext, params: None
) -> RowVerdict:
    """필수 컬럼에 문제가 하나라도 있으면 행을 폐기한다."""
    return RowVerdict.DROP if any(issue.required for issue in issues) else RowVerdict.KEEP


@row_policy("drop_if_issue_count_exceeds", params=IssueCountParams)
def drop_if_issue_count_exceeds(
    row: dict, issues: list[Issue], ctx: RunContext, params: IssueCountParams
) -> RowVerdict:
    """이슈 개수가 임계값을 넘으면 행을 폐기한다."""
    return RowVerdict.DROP if len(issues) > params.max_issues else RowVerdict.KEEP


@row_policy("keep_always")
def keep_always(row: dict, issues: list[Issue], ctx: RunContext, params: None) -> RowVerdict:
    """항상 유지한다."""
    return RowVerdict.KEEP
