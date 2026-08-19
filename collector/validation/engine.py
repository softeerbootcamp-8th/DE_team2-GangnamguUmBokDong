"""어댑터가 넘긴 행을 config 기준으로 판정·정책 디스패치해 silver·quarantine·집계로 만드는 검증 엔진."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from config.schema import ColumnSpec, Policies, SourceConfig
from core.masked import MaskedValue, parse_masked_float
from core.precip import parse_precip
from core.snow import parse_snow
from validation.registry import get_policy, get_row_policy, get_row_policy_params_model
from validation.types import Action, Issue, IssueKind, RowVerdict, RunContext


def _parse_bool(value: Any) -> bool:
    """문자열 "true"/"false"(대소문자 무관)만 bool로 인정한다.

    args:
        value: 캐스팅할 원시값. 이미 bool이면 그대로 반환한다.
    returns:
        캐스팅된 bool 값.
    raises:
        ValueError: value가 bool도 아니고 "true"/"false" 문자열도 아닐 때.
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in ("true", "false"):
        return value.strip().lower() == "true"
    raise ValueError(f"bool로 해석할 수 없다: {value!r}")


_CASTERS: dict[str, Callable[[Any], Any]] = {
    "str": str,
    "int": int,
    "float": float,
    "bool": _parse_bool,
    # 기상청 강수량 범주 표기 전용. loader도 같은 규칙을 써야 silver와 RDB의 값이
    # 갈리지 않으므로 `core`에 둔다.
    "precip": parse_precip,
    # 적설 범주 표기 전용. `precip`과 형태는 같지만 단위가 cm라 함수를 나눴다
    # (`core._amount` docstring 참고).
    "snow": parse_snow,
    # 생활인구의 비식별 마스킹(`*`) 전용. `*`을 TYPE_ERROR가 아니라 결측으로
    # 판정시킨다 — `_judge_column`이 MaskedValue를 잡아 되돌린다.
    "masked_float": parse_masked_float,
}


def _try_cast(raw_value: Any, types: tuple[str, ...]) -> tuple[Any, bool]:
    """`types` 선언 순서대로 캐스팅을 시도해 첫 성공을 채택한다.

    args:
        raw_value: 캐스팅할 원시값.
        types: 시도할 타입 이름을 선언 순서대로 담은 튜플
    returns:
        (캐스팅된 값, 성공 여부). 전부 실패하면 (raw_value, False)를 반환한다.
    """
    for type_name in types:
        try:
            return _CASTERS[type_name](raw_value), True
        except (ValueError, TypeError):
            continue
    return raw_value, False


def _judge_column(raw_value: Any, column: str, spec: ColumnSpec) -> tuple[Any, Issue | None]:
    """컬럼 하나를 null → type → range·enum 3단계로 판정한다.

    이슈가 없으면 캐스팅된 값이 그대로 silver 값 후보가 된다. 
    이슈가 있으면 (MISSING은 None, TYPE_ERROR는 캐스팅 실패 원시값, OUTLIER는 캐스팅된 값)을 함께 반환한다.

    args:
        raw_value: 원본 raw 값(None 또는 빈 문자열은 결측으로 본다).
        column: 컬럼 이름(Issue에 남긴다).
        spec: 이 컬럼의 타입 · 필수 여부 · range · enum 스펙.
    returns:
        (value, 이슈 | None)
    """
    if raw_value is None or raw_value == "":
        issue = Issue(
            column=column, kind=IssueKind.MISSING, required=spec.required, raw_value=raw_value, spec=spec
        )
        return None, issue

    try:
        casted, ok = _try_cast(raw_value, spec.types)
    except MaskedValue:
        # 비식별 마스킹(`*`)은 "가려진 값"이지 형식 오류가 아니다. 빈 값과 같은
        # 결측으로 판정해 결측 정책이 걸리게 한다(사유는 `core.masked` 참고).
        issue = Issue(
            column=column, kind=IssueKind.MISSING, required=spec.required, raw_value=raw_value, spec=spec
        )
        return None, issue

    if not ok:
        issue = Issue(
            column=column, kind=IssueKind.TYPE_ERROR, required=spec.required, raw_value=raw_value, spec=spec
        )
        return raw_value, issue

    if spec.enum is not None:
        is_outlier = casted not in spec.enum
    elif spec.range is not None:
        is_outlier = not (spec.range.min <= casted <= spec.range.max)
    else:
        is_outlier = False

    if is_outlier:
        issue = Issue(
            column=column, kind=IssueKind.OUTLIER, required=spec.required, raw_value=raw_value, spec=spec
        )
        return casted, issue

    return casted, None


def _resolve_policy_name(issue: Issue, policies: Policies) -> str:
    """이슈 종류 · 필수 여부 · 컬럼 오버라이드로 적용할 정책 이름을 고른다.

    TYPE_ERROR는 컬럼별 `on_outlier` 오버라이드를 무시한다.
    캐스팅되지 않은 값에는 clip_to_range 같은 교정 정책을 적용할 수 없기 때문

    args:
        issue: 판정된 이슈
        policies: SourceConfig.policies
    returns:
        레지스트리에서 조회할 정책 이름.
    """
    if issue.kind is IssueKind.MISSING:
        if issue.spec.on_missing is not None:
            return issue.spec.on_missing
        return policies.required_missing if issue.required else policies.optional_missing

    if issue.kind is IssueKind.OUTLIER and issue.spec.on_outlier is not None:
        return issue.spec.on_outlier
    return policies.required_outlier if issue.required else policies.optional_outlier


class BatchValidationFailed(RuntimeError):
    """FAIL_BATCH 정책이 트리거되어 배치 전체가 실패"""


@dataclass(slots=True)
class _RowResult:
    """한 행의 컬럼 처리 결과.
    validate_batch가 이 결과를 행 정책과 결합해 최종 유지·폐기를 판정한다.
    """

    resolved: dict[str, Any]
    issues: list[Issue]
    issue_entries: list[dict[str, Any]]
    repaired: bool
    dropped_by_column: bool


def _process_columns(
    row: dict,
    config: SourceConfig,
    ctx: RunContext,
    column_issue_counts: dict[str, dict[str, int]],
    policy_action_counts: dict[str, int],
) -> _RowResult:
    """행 하나의 컬럼을 전부 판정 · 디스패치하고 집계를 누적한다.

    컬럼 정책이 DROP_ROW를 반환해도 나머지 컬럼을 계속 판정한다.
    quarantine에 그 행의 이슈 전체를 남겨야 하기 때문이다. 
    정책에 넘기는 `row`는 원본 raw row 그대로다. (부분 교정된 값이 섞이지 않는다)

    args:
        row: 원본 raw 행.
        config: 컬럼 스펙과 정책을 담은 소스 설정.
        ctx: 정책이 참조할 실행 맥락.
        column_issue_counts: 컬럼별 이슈 종류 개수를 누적할 dict
        policy_action_counts: 정책 이름별 호출 횟수를 누적할 dict
    returns:
        이 행의 처리 결과
    raises:
        BatchValidationFailed: 컬럼 정책이 FAIL_BATCH를 반환했을 때.
    """
    resolved: dict[str, Any] = {}
    issues: list[Issue] = []
    issue_entries: list[dict[str, Any]] = []
    repaired = False
    dropped_by_column = False

    for column, spec in config.columns.items():
        raw_value = row.get(column)
        value, issue = _judge_column(raw_value, column, spec)
        if issue is None:
            # 이슈 없음: 캐스팅된 값이 그대로 silver 값 후보가 된다.
            resolved[column] = value
            continue

        issues.append(issue)
        # manifest에 남길 컬럼별 이슈 종류 집계
        counts = column_issue_counts.setdefault(
            column, {"missing": 0, "outlier": 0, "type_error": 0}
        )
        counts[issue.kind.value] += 1

        # 이슈 종류·필수 여부·컬럼 오버라이드로 적용할 정책을 고르고 실행한다.
        policy_name = _resolve_policy_name(issue, config.policies)
        new_value, action = get_policy(policy_name)(value, issue, row, ctx)
        policy_action_counts[policy_name] = policy_action_counts.get(policy_name, 0) + 1

        if action is Action.FAIL_BATCH:
            # 배치 전체를 즉시 중단시켜야 하므로 나머지 컬럼 처리를 진행하지 않는다.
            raise BatchValidationFailed(
                f"컬럼 '{column}'에서 정책 '{policy_name}'이 FAIL_BATCH를 반환했다"
            )

        if action is Action.DROP_ROW:
            # 이 컬럼은 행을 폐기하라고 판정했지만, quarantine에 남길 전체 이슈를
            # 모으기 위해 나머지 컬럼 판정은 계속 진행한다.
            dropped_by_column = True

        if new_value != value:
            # 정책이 원본 판정값을 다른 값으로 교정했다는 뜻
            repaired = True

        resolved[column] = new_value
        issue_entries.append(
            {
                "column": column,
                "kind": issue.kind.value,
                "required": issue.required,
                "action": action.value,
            }
        )

    return _RowResult(
        resolved=resolved,
        issues=issues,
        issue_entries=issue_entries,
        repaired=repaired,
        dropped_by_column=dropped_by_column,
    )


def _resolve_row_params(policies: Policies) -> Any:
    """`policies.row`에 등록된 params 모델로 row_params를 배치당 한 번만 파싱한다.
    행마다 다시 파싱하지 않도록 validate_batch가 배치 시작 시 한 번만 호출한다.

    args:
        policies: 소스의 정책 설정(row,row_params).
    returns:
        파싱된 params 모델 인스턴스. 행 정책이 없거나 params를 받지 않으면 None.
    """
    if policies.row is None:
        return None
    params_model = get_row_policy_params_model(policies.row)
    if params_model is None:
        return None
    return params_model.model_validate(policies.row_params or {})


@dataclass(slots=True)
class BatchOutcome:
    """validate_batch의 결과.

    필드명은 manifest.py의 Counts · ColumnIssueCount와 동일하게 맞춰서
    pipeline이 변환 없이 그대로 대입할 수 있게 한다.
    """

    silver_rows: list[dict]
    quarantine_records: list[dict]
    counts: dict[str, int]
    drop_ratio: float
    column_issues: dict[str, dict[str, int]]
    policy_actions: dict[str, int]


def _build_quarantine_record(row: dict, issue_entries: list[dict], row_index: int) -> dict:
    """폐기된 행의 quarantine 레코드를 만든다.

    MISSING으로 판정된 컬럼만 None으로 정규화하고, TYPE_ERROR · OUTLIER 컬럼은
    원본 raw 값을 그대로 남겨 "무엇이 왜 폐기됐는지"를 보존한다.

    args:
        row: 원본 raw 행.
        issue_entries: 이 행에서 발생한 이슈 목록(column · kind · required · action).
        row_index: 배치 안에서 이 행의 순번.
    returns:
        `_issues` · `_row_index`가 덧붙은 quarantine 레코드.
    """
    record = dict(row)
    for entry in issue_entries:
        if entry["kind"] == IssueKind.MISSING.value:
            record[entry["column"]] = None
    record["_issues"] = issue_entries
    record["_row_index"] = row_index
    return record


def validate_batch(rows: list[dict], config: SourceConfig, ctx: RunContext) -> BatchOutcome:
    """어댑터가 넘긴 list[dict]를 silver 행 · quarantine 행 · 집계로 바꾼다.

    행은 컬럼 정책 중 하나라도 DROP_ROW이거나 행 정책이 DROP을 반환하면 폐기된다.
    컬럼 단계에서 이미 폐기가 확정되면 행 정책은 호출하지 않는다.

    args:
        rows: 검증할 원본 raw 행 목록.
        config: 소스 설정.
        ctx: 정책이 참조할 실행 맥락.
    returns:
        silver 행, quarantine 레코드, 집계를 담은 BatchOutcome.
    raises:
        BatchValidationFailed: 어떤 컬럼 정책이 FAIL_BATCH를 반환했을 때. 그 시점에서
            순회를 즉시 중단하므로 이후 행은 처리되지 않는다.
    """
    # 행 정책 params는 배치당 한 번만 파싱해서 매 행마다 재사용한다.
    row_params = _resolve_row_params(config.policies)
    column_issue_counts: dict[str, dict[str, int]] = {}
    policy_action_counts: dict[str, int] = {}
    silver_rows: list[dict] = []
    quarantine_records: list[dict] = []
    kept = repaired_count = dropped = 0

    for row_index, row in enumerate(rows):
        # 컬럼 단위 판정·정책 디스패치를 먼저 끝낸다.
        result = _process_columns(row, config, ctx, column_issue_counts, policy_action_counts)

        # 컬럼 정책에서 이미 DROP_ROW가 확정됐으면 행 정책은 호출하지 않는다.
        verdict = RowVerdict.DROP if result.dropped_by_column else RowVerdict.KEEP
        if verdict is RowVerdict.KEEP and config.policies.row is not None:
            row_policy = get_row_policy(config.policies.row)
            verdict = row_policy(row, result.issues, ctx, row_params)

        if verdict is RowVerdict.DROP:
            # 폐기된 행은 silver로 보내지 않고, 왜 폐기됐는지 이슈 전체를 quarantine에 남긴다.
            dropped += 1
            quarantine_records.append(
                _build_quarantine_record(row, result.issue_entries, row_index)
            )
            continue

        kept += 1
        if result.repaired:
            repaired_count += 1
        # 다운스트림이 원본 값 그대로인지 교정된 값인지 구분할 수 있도록 상태를 남긴다.
        result.resolved["_row_status"] = "repaired" if result.repaired else "ok"
        silver_rows.append(result.resolved)

    fetched = len(rows)
    return BatchOutcome(
        silver_rows=silver_rows,
        quarantine_records=quarantine_records,
        counts={"fetched": fetched, "kept": kept, "repaired": repaired_count, "dropped": dropped},
        drop_ratio=(dropped / fetched) if fetched else 0.0,
        column_issues=column_issue_counts,
        policy_actions=policy_action_counts,
    )
