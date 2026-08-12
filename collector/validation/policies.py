"""공통 정책 함수 구현체 — 소스와 무관하다.

구현 예정: docs/collector/implementation-issues.md #3
설계 근거: docs/collector/implementation-plan.md 5절 (정책 계약)

## 이 모듈의 역할

이슈가 발견된 값을 **어떻게 처리할지**를 담은 함수 모음이다. 특정 소스에 대한 지식이
들어가는 순간 "소스가 늘어도 공통 코드는 바뀌지 않는다"는 목표가 깨진다. 소스별 차이는
전부 YAML(4분면 기본값 + 컬럼별 오버라이드)로 표현한다.

`Action` · `Issue` · `RowVerdict` · `RunContext`는 `validation/types.py`에서,
데코레이터는 `validation/registry.py`에서 가져온다.

## 컬럼 정책 7종 — `(value, spec, row, ctx) -> tuple[Any, Action]`

| 이름 | 반환값 | Action | repaired |
| --- | --- | --- | --- |
| `keep_null` | 원래 값 그대로 | KEEP | 아니오 |
| `set_null` | None | KEEP | 예 |
| `fill_zero` | 0 | KEEP | 예 |
| `fill_default` | spec에 선언된 기본값 | KEEP | 예 |
| `clip_to_range` | 정상 범위의 경계로 자른 값 | KEEP | 예 |
| `drop_row` | — | DROP_ROW | — (행 폐기) |
| `fail_batch` | — | FAIL_BATCH | — (배치 실패) |

### 교정형 정책은 캐스팅 실패 값을 방어한다

`clip_to_range` · `fill_zero` · `fill_default`는 값이 `spec.types`로 해석되지 않는
경우를 만날 수 있다. 4분면 기본값이 교정형으로 설정된 소스에서 `TYPE_ERROR`가 이쪽으로
디스패치되기 때문이다(계획서 5절).

이때는 `(None, Action.KEEP)`을 반환한다. 결과적으로 `set_null`과 같은 효과가 되어
Parquet 스키마가 깨지지 않고, 규칙을 새로 만들지 않고 구현 안에서 흡수된다.

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
  스키마 계약 위반처럼 **데이터 전체를 의심해야 할 때**만 쓴다.
- 정책은 순수 함수로 둔다. 로그를 남기거나 S3에 접근하지 않는다. 행 단위 로그가 0줄인
  이유가 여기에 있다(계획서 8절).

검증(계획서 11절): 정책 함수 10종을 각각 단위 테스트한다. 교정형 3종은 캐스팅 실패 값을
넣어 `(None, KEEP)`이 나오는지도 함께 확인한다.
"""
