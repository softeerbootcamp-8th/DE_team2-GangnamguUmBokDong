"""계약 타입의 값 문자열은 디스크 계약이다 — manifest · quarantine에 그대로 나간다."""

from dataclasses import FrozenInstanceError

import pytest
from validation.types import Action, Issue, IssueKind, RowVerdict, RunContext


def test_action_values_match_disk_contract():
    # quarantine `_issues[].action`으로 나가는 문자열
    assert [a.value for a in Action] == ["keep", "drop_row", "fail_batch"]


def test_issue_kind_values_match_disk_contract():
    # manifest `column_issues`의 키, quarantine `_issues[].kind`로 나가는 문자열
    assert [k.value for k in IssueKind] == ["missing", "type_error", "outlier"]


def test_row_verdict_values():
    assert [v.value for v in RowVerdict] == ["keep", "drop"]


def test_issue_is_frozen():
    issue = Issue(column="stationId", kind=IssueKind.MISSING, required=True, raw_value=None, spec=None)
    with pytest.raises(FrozenInstanceError):
        issue.column = "other"


def test_run_context_is_frozen():
    from datetime import datetime
    from zoneinfo import ZoneInfo

    kst = ZoneInfo("Asia/Seoul")
    ctx = RunContext(
        source_id="bike_station_realtime",
        window_start=datetime(2026, 8, 12, 14, 10, tzinfo=kst),
        window_end=datetime(2026, 8, 12, 14, 15, tzinfo=kst),
        attempt=1,
    )
    with pytest.raises(FrozenInstanceError):
        ctx.attempt = 2
