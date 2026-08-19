"""생활인구 비식별 마스킹 표기(`*`) → 결측 판정 규칙.

`living_population_grid`는 소수 인구를 `*`로 가려서 보낸다(표본 1,000행에서 나이·성별
28컬럼의 53.4%, `SPOP`의 9.8%). `docs/collector/DataSchema.md`는 이 값의 결측 기준을
"값 없음(마스킹 `*` → null)"으로 명시한다 — null이 되는 것은 의도된 동작이다.

문제는 판정 **라벨**이었다. `types: [float]`로 두면 `float("*")`이 실패해
TYPE_ERROR가 되고 `optional_outlier` 정책을 탄다. 그래서:

- manifest의 `column_issues`에 `missing`이 아니라 `type_error`로 집계돼, 정상 마스킹과
  진짜 형식 오류를 지표로 구분할 수 없었다.
- `optional_outlier`를 `drop_row`로 바꾸면 정상 마스킹 행이 통째로 폐기되고,
  `optional_missing`을 조정해도 마스킹에는 아무 영향이 없었다(손잡이가 반대로 걸림).

`masked_float` 캐스터는 `*`을 결측으로 판정시켜 문서가 말하는 대로 동작하게 한다.
"""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from compaction import archive_schema
from config.schema import ColumnSpec, Policies, Range, SourceConfig
from core.masked import MaskedValue, parse_masked_float
from validation.engine import _judge_column, validate_batch
from validation.types import IssueKind, RunContext

_KST = timezone(timedelta(hours=9))


class TestParseMaskedFloat:
    def test_masked_marker_raises_masked_value(self):
        """MaskedValue는 ValueError·TypeError가 아니어야 한다 — `_try_cast`가
        삼켜버리면 TYPE_ERROR로 되돌아간다."""
        with pytest.raises(MaskedValue):
            parse_masked_float("*")

    def test_masked_value_is_not_value_or_type_error(self):
        assert not issubclass(MaskedValue, (ValueError, TypeError))

    def test_masked_marker_with_surrounding_space(self):
        with pytest.raises(MaskedValue):
            parse_masked_float(" * ")

    def test_number_string_becomes_float(self):
        assert parse_masked_float("824.74") == 824.74

    def test_accepts_float_input(self):
        """silver가 이미 숫자로 저장된 뒤 다시 넘기는 경로."""
        assert parse_masked_float(5.32) == 5.32

    def test_unparseable_text_raises_value_error(self):
        """마스킹이 아닌 진짜 형식 오류는 여전히 TYPE_ERROR로 가야 한다."""
        with pytest.raises(ValueError):
            parse_masked_float("알수없음")

    def test_none_raises(self):
        with pytest.raises((TypeError, ValueError)):
            parse_masked_float(None)


class TestJudgedAsMissing:
    def test_masked_marker_is_missing_not_type_error(self):
        value, issue = _judge_column("*", "SPOP", ColumnSpec(types=("masked_float",)))

        assert value is None
        assert issue is not None
        assert issue.kind is IssueKind.MISSING

    def test_masked_marker_skips_range_check(self):
        """결측이므로 range 판정에 닿지 않는다(OUTLIER가 되면 정책이 또 갈린다)."""
        spec = ColumnSpec(types=("masked_float",), range=Range(min=0, max=10_000_000))
        _, issue = _judge_column("*", "SPOP", spec)

        assert issue is not None
        assert issue.kind is IssueKind.MISSING

    def test_real_number_passes_range_check(self):
        spec = ColumnSpec(types=("masked_float",), range=Range(min=0, max=10_000_000))
        value, issue = _judge_column("824.74", "SPOP", spec)

        assert value == 824.74
        assert issue is None

    def test_out_of_range_number_is_still_an_outlier(self):
        spec = ColumnSpec(types=("masked_float",), range=Range(min=0, max=100))
        _, issue = _judge_column("999", "SPOP", spec)

        assert issue is not None
        assert issue.kind is IssueKind.OUTLIER

    def test_unparseable_text_is_still_a_type_error(self):
        _, issue = _judge_column("알수없음", "SPOP", ColumnSpec(types=("masked_float",)))

        assert issue is not None
        assert issue.kind is IssueKind.TYPE_ERROR


def _ctx() -> RunContext:
    return RunContext(
        source_id="living_population_grid",
        window_start=datetime(2026, 8, 19, 0, 0, tzinfo=_KST),
        window_end=datetime(2026, 8, 20, 0, 0, tzinfo=_KST),
        attempt=1,
    )


def _population_config(**policy_overrides) -> SourceConfig:
    """마스킹 컬럼 하나만 있는 최소 설정."""
    from tests.test_compaction import _config

    policies = {
        "required_missing": "drop_row",
        "required_outlier": "drop_row",
        "optional_missing": "keep_null",
        "optional_outlier": "set_null",
        **policy_overrides,
    }
    return _config(
        columns={"SPOP": ColumnSpec(types=("masked_float",))},
        policies=Policies(**policies),
    )


class TestPolicyKnobsLineUp:
    """문서가 "결측"이라 부르는 값이 결측 정책으로 다뤄져야 한다."""

    def test_counted_as_missing_in_manifest(self):
        outcome = validate_batch(
            [{"SPOP": "*"}, {"SPOP": "824.74"}],
            _population_config(),
            _ctx(),
        )

        assert outcome.column_issues["SPOP"]["missing"] == 1
        assert outcome.column_issues["SPOP"]["type_error"] == 0

    def test_optional_missing_knob_controls_masked_values(self):
        """수정 전에는 optional_missing을 바꿔도 마스킹에 영향이 없었다(회귀 방지)."""
        outcome = validate_batch(
            [{"SPOP": "*"}, {"SPOP": "824.74"}],
            _population_config(optional_missing="drop_row"),
            _ctx(),
        )

        assert outcome.counts["dropped"] == 1
        assert outcome.counts["kept"] == 1

    def test_optional_outlier_knob_no_longer_drops_masked_rows(self):
        """수정 전에는 optional_outlier를 drop_row로 바꾸면 정상 마스킹 행이
        통째로 폐기됐다."""
        outcome = validate_batch(
            [{"SPOP": "*"}, {"SPOP": "824.74"}],
            _population_config(optional_outlier="drop_row"),
            _ctx(),
        )

        assert outcome.counts["dropped"] == 0
        assert outcome.silver_rows[0]["SPOP"] is None


class TestArchiveSchema:
    def test_masked_float_maps_to_float64(self):
        """compaction이 `types[0]`로 archive 스키마를 만든다. 빠지면 KeyError로 터진다."""
        from tests.test_compaction import _config

        schema = archive_schema(_config(columns={"SPOP": ColumnSpec(types=("masked_float",))}))

        assert schema.field("SPOP").type == pa.float64()
