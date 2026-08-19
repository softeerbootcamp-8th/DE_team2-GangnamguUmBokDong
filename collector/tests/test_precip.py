"""기상청 강수량 범주 문자열 → mm 실수 변환 규칙.

`core.precip.parse_precip`은 collector의 캐스터(`validation/engine.py`의 `_CASTERS`)와
loader의 `_parse_precip_str`이 함께 쓰는 단일 규칙이다. 두 계층이 각자 파싱하면
같은 원본이 silver와 RDB에서 다른 숫자가 되므로 규칙은 여기 한 곳에만 둔다.

범위형 값은 **하한**을 취한다(`"30.0~50.0mm"` → 30.0). 상한 없는 `"50.0mm 이상"`도
같은 규칙으로 50.0이다 — 평균을 정의할 수 없어 하한 규칙으로 통일했다.

적설(`SNO`)은 표기 형태가 같지만 단위가 cm라 이 함수가 받지 않는다.
그 규칙과 "서로의 표기를 거부한다"는 검증은 `tests/test_snow.py`에 있다.
"""

import pyarrow as pa
import pytest

from compaction import archive_schema
from config.schema import ColumnSpec
from core.precip import parse_precip
from validation.engine import _judge_column
from validation.types import IssueKind


class TestNoPrecipitation:
    def test_no_rain_is_zero(self):
        assert parse_precip("강수없음") == 0.0

    def test_bare_zero_is_zero(self):
        """실호출에서 '강수없음'과 맨 '0'이 같은 응답에 섞여 나온다."""
        assert parse_precip("0") == 0.0


class TestExactValues:
    def test_value_with_unit(self):
        assert parse_precip("2.0mm") == 2.0

    def test_value_without_unit(self):
        assert parse_precip("5.5") == 5.5

    def test_accepts_float_input(self):
        """silver가 이미 숫자로 저장된 뒤 loader가 다시 넘기는 경로."""
        assert parse_precip(2.0) == 2.0


class TestRanges:
    def test_range_takes_lower_bound(self):
        assert parse_precip("30.0~50.0mm") == 30.0

    def test_at_least_takes_lower_bound(self):
        """상한이 없어 평균을 정의할 수 없다. 하한 규칙을 그대로 적용한다."""
        assert parse_precip("50.0mm 이상") == 50.0

    def test_below_threshold_is_half(self):
        """'1.0mm 미만'은 실제로 0.1~1.0 구간이다. 0.5로 대표한다."""
        assert parse_precip("1.0mm 미만") == 0.5


class TestFailures:
    """캐스터로 쓰이므로 실패는 예외여야 한다 — `_try_cast`가 다음 타입으로 넘어가고,
    전부 실패하면 TYPE_ERROR 이슈가 되어 정책이 처리한다. None을 돌려주면 이 흐름이
    결측(MISSING)과 구분되지 않는다."""

    def test_unparseable_text_raises(self):
        with pytest.raises(ValueError):
            parse_precip("맑음")

    def test_none_raises(self):
        with pytest.raises((TypeError, ValueError)):
            parse_precip(None)


class TestRegisteredAsCaster:
    """yaml에서 `types: [precip]`으로 선언하면 silver에 숫자가 들어가야 한다."""

    def test_range_string_becomes_number_in_silver(self):
        value, issue = _judge_column("30.0~50.0mm", "PCP", ColumnSpec(types=("precip",)))

        assert value == 30.0
        assert issue is None

    def test_unparseable_value_is_a_type_error(self):
        _, issue = _judge_column("맑음", "PCP", ColumnSpec(types=("precip",)))

        assert issue is not None
        assert issue.kind is IssueKind.TYPE_ERROR

    def test_empty_value_stays_missing_not_type_error(self):
        """빈 값은 캐스터에 닿기 전에 결측으로 걸러진다 — 정책이 달라 구분이 필요하다."""
        _, issue = _judge_column("", "PCP", ColumnSpec(types=("precip",)))

        assert issue is not None
        assert issue.kind is IssueKind.MISSING


class TestArchiveSchema:
    def test_precip_maps_to_float64(self):
        """compaction이 `types[0]`로 archive 스키마를 만든다. 빠지면 KeyError로 터진다."""
        from tests.test_compaction import _config

        schema = archive_schema(_config(columns={"PCP": ColumnSpec(types=("precip",))}))

        assert schema.field("PCP").type == pa.float64()
