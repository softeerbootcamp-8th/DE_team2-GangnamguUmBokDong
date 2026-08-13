"""@policy / @row_policy 데코레이터와 이름→함수 매핑.

구현 예정: docs/collector/implementation-issues.md #3
설계 근거: docs/collector/implementation-plan.md 5절 (정책 계약)

## 이 모듈의 역할

YAML에 적힌 **문자열을 실제 함수로 바꾸는 지점**이다. "새 정책이 필요하면 함수 하나를
추가하고 YAML에서 이름으로 부른다. 엔진 코드는 건드리지 않는다"는 구조가 여기서 나온다.

계약 타입(`Action` · `Issue` · `RowVerdict` · `RunContext`)은 `validation/types.py`에
있다. 이 모듈은 등록과 조회만 담당한다.

## 데코레이터 2종

- `@policy(name)` — 컬럼 정책. `(value, spec, row, ctx) -> tuple[Any, Action]`
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

- `get_policy(name)` · `get_row_policy(name)` — 엔진이 디스패치할 때 쓴다. 미등록
  이름은 예외로 막고, 메시지에 **등록된 이름 목록**을 함께 싣는다.
- 존재 확인 함수 — loader가 기동 시 config의 정책 이름을 검증할 때 쓴다. 함수를
  실행하지 않고 등록 여부만 본다.
- `get_row_policy_params_model(name)` — 그 정책에 등록된 params 모델(없으면 None).
  loader가 `row_params`를 검증할 때 쓴다.

## 주의

- 같은 이름을 두 번 등록하면 예외로 막는다. 조용히 덮어쓰면 어느 함수가 실제로 돌았는지
  알 수 없게 된다.
- 등록은 **import 부수효과**다. `validation.policies`가 import되지 않으면 레지스트리가
  비어 있다. 이 보장은 loader가 맡는다.
- 정책 이름은 manifest의 `policy_actions` 키로 남는다. 이름을 바꾸면 과거 manifest와
  대조가 끊기므로 개명은 신중히 한다.
"""
