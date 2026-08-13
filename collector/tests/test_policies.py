"""정책 함수는 순수 함수다. 로그도 S3도 없다."""

from validation.policies import (
    clip_to_range,
    drop_row,
    fail_batch,
    fill_default,
    fill_zero,
    keep_null,
    set_null,
)
from validation.registry import policy_names
from validation.types import Action, IssueKind


def test_all_column_policies_are_registered():
    assert set(policy_names()) >= {
        "keep_null",
        "set_null",
        "fill_zero",
        "fill_default",
        "clip_to_range",
        "drop_row",
        "fail_batch",
    }


def test_keep_null_returns_value_unchanged(make_issue, ctx):
    issue = make_issue(IssueKind.MISSING, raw="")
    # 값을 바꾸지 않으므로 엔진의 repaired 판정에서 제외된다
    assert keep_null("", issue, {}, ctx) == ("", Action.KEEP)


def test_set_null_replaces_value(make_issue, ctx):
    issue = make_issue(IssueKind.OUTLIER, raw="250")
    assert set_null(250, issue, {}, ctx) == (None, Action.KEEP)


def test_fill_zero_fills_missing(make_issue, ctx):
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert fill_zero(None, issue, {}, ctx) == (0, Action.KEEP)


def test_fill_zero_fills_outlier(make_issue, ctx):
    issue = make_issue(IssueKind.OUTLIER, raw="250")
    assert fill_zero(250, issue, {}, ctx) == (0, Action.KEEP)


def test_fill_zero_defends_type_error(make_issue, ctx):
    # 해석 불가한 값에 0을 채우면 안 된다 — set_null과 같은 효과로 빠진다
    issue = make_issue(IssueKind.TYPE_ERROR, raw="31.6xyz")
    assert fill_zero("31.6xyz", issue, {}, ctx) == (None, Action.KEEP)


def test_fill_default_uses_spec_default(make_spec, make_issue, ctx):
    spec = make_spec(default=-1)
    issue = make_issue(IssueKind.MISSING, spec=spec, raw=None)
    assert fill_default(None, issue, {}, ctx) == (-1, Action.KEEP)


def test_fill_default_defends_type_error(make_spec, make_issue, ctx):
    spec = make_spec(default=-1)
    issue = make_issue(IssueKind.TYPE_ERROR, spec=spec, raw="abc")
    assert fill_default("abc", issue, {}, ctx) == (None, Action.KEEP)


def test_fill_default_without_declared_default_returns_none(make_issue, ctx):
    # 로드 시점에 막을지는 #2의 결정이다. #3에서는 set_null과 같은 효과로 둔다
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert fill_default(None, issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_clips_above_max(make_spec, make_issue, ctx):
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="250")
    assert clip_to_range(250, issue, {}, ctx) == (200, Action.KEEP)


def test_clip_to_range_clips_below_min(make_spec, make_issue, ctx):
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="-5")
    assert clip_to_range(-5, issue, {}, ctx) == (0, Action.KEEP)


def test_clip_to_range_defends_type_error(make_spec, make_issue, ctx):
    # 4분면 기본값이 교정형인 소스에서 TYPE_ERROR가 이쪽으로 디스패치된다
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.TYPE_ERROR, spec=spec, raw="31.6xyz")
    assert clip_to_range("31.6xyz", issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_defends_missing(make_spec, make_issue, ctx):
    # `on_missing: clip_to_range`는 YAML에서 합법이다
    spec = make_spec(range=(0, 200))
    issue = make_issue(IssueKind.MISSING, spec=spec, raw=None)
    assert clip_to_range(None, issue, {}, ctx) == (None, Action.KEEP)


def test_clip_to_range_defends_missing_range(make_spec, make_issue, ctx):
    # enum만 선언한 컬럼(PTY)에 on_outlier: clip_to_range를 걸었을 때
    spec = make_spec(enum=(0, 1, 2, 3, 5, 6, 7))
    issue = make_issue(IssueKind.OUTLIER, spec=spec, raw="9")
    assert clip_to_range(9, issue, {}, ctx) == (None, Action.KEEP)


def test_drop_row_returns_drop_action(make_issue, ctx):
    issue = make_issue(IssueKind.MISSING, raw=None)
    assert drop_row(None, issue, {}, ctx) == (None, Action.DROP_ROW)


def test_fail_batch_returns_fail_action(make_issue, ctx):
    issue = make_issue(IssueKind.TYPE_ERROR, raw="junk")
    assert fail_batch("junk", issue, {}, ctx) == ("junk", Action.FAIL_BATCH)
