"""기상청 적설 범주 문자열 → cm 실수 변환 규칙.

`SNO`(3시간 신적설)는 `PCP`와 같은 응답에 오고 표기 형태도 같은 계열이지만
**단위가 cm다**. mm용 `parse_precip`에 넘기면 `"5.0cm"`가 `float("5.0cm")`에서 터져
TYPE_ERROR → set_null이 된다(여름에는 `"적설없음"`만 와서 드러나지 않는다).

`"1.0cm 미만"`의 대표값도 mm의 0.5를 그대로 쓸 수 없다 — cm 기준으로 0.5cm는
실제 구간(0.1~1.0cm)의 대표값이지만, mm 컬럼에 0.5로 들어가면 10배 축소된다.
그래서 단위별로 함수를 분리한다.
"""

import pyarrow as pa
import pytest

from compaction import archive_schema
from config.schema import ColumnSpec
from core.precip import parse_precip
from core.snow import parse_snow
from validation.engine import _judge_column
from validation.types import IssueKind


class TestNoSnow:
    def test_no_snow_is_zero(self):
        assert parse_snow("적설없음") == 0.0

    def test_bare_zero_is_zero(self):
        """PCP와 마찬가지로 맨 '0'이 섞여 온다."""
        assert parse_snow("0") == 0.0


class TestExactValues:
    def test_value_with_cm_unit(self):
        """mm용 파서가 터지던 형태다. 이게 이 모듈이 존재하는 이유다."""
        assert parse_snow("5.0cm") == 5.0

    def test_value_without_unit(self):
        assert parse_snow("3.5") == 3.5

    def test_accepts_float_input(self):
        """silver가 이미 숫자로 저장된 뒤 다시 넘기는 경로."""
        assert parse_snow(2.0) == 2.0


class TestRanges:
    def test_range_takes_lower_bound(self):
        assert parse_snow("1.0~4.9cm") == 1.0

    def test_at_least_takes_lower_bound(self):
        """mm용 파서가 터지던 형태다('이상'을 지운 뒤 'cm'가 남았다)."""
        assert parse_snow("5.0cm 이상") == 5.0

    def test_below_threshold_is_half_cm(self):
        """'1.0cm 미만'은 실제로 0.1~1.0cm 구간이다. 0.5cm로 대표한다."""
        assert parse_snow("1.0cm 미만") == 0.5


class TestFailures:
    """캐스터로 쓰이므로 실패는 예외여야 한다(`parse_precip`과 같은 이유)."""

    def test_unparseable_text_raises(self):
        with pytest.raises(ValueError):
            parse_snow("맑음")

    def test_none_raises(self):
        with pytest.raises((TypeError, ValueError)):
            parse_snow(None)

    def test_rain_label_is_not_accepted(self):
        """강수 표기를 적설 파서에 넘기는 것은 설정 오류다. 조용히 0이 되면 안 된다."""
        with pytest.raises(ValueError):
            parse_snow("강수없음")


class TestPrecipNoLongerAcceptsSnow:
    """단위가 다른 두 표기를 한 함수가 받으면 어느 단위로 저장됐는지 알 수 없다.
    적설은 `parse_snow`만 받는다."""

    def test_precip_rejects_snow_label(self):
        with pytest.raises(ValueError):
            parse_precip("적설없음")

    def test_precip_still_rejects_cm_values(self):
        """mm 파서가 cm를 조용히 통과시키면 10배 오차가 생긴다."""
        with pytest.raises(ValueError):
            parse_precip("5.0cm")


class TestRegisteredAsCaster:
    """yaml에서 `types: [snow]`로 선언하면 silver에 숫자가 들어가야 한다."""

    def test_cm_string_becomes_number_in_silver(self):
        value, issue = _judge_column("1.0~4.9cm", "SNO", ColumnSpec(types=("snow",)))

        assert value == 1.0
        assert issue is None

    def test_plain_cm_value_is_not_a_type_error(self):
        """수정 전에는 이 값이 TYPE_ERROR → set_null이 됐다(회귀 방지)."""
        value, issue = _judge_column("5.0cm", "SNO", ColumnSpec(types=("snow",)))

        assert value == 5.0
        assert issue is None

    def test_unparseable_value_is_a_type_error(self):
        _, issue = _judge_column("맑음", "SNO", ColumnSpec(types=("snow",)))

        assert issue is not None
        assert issue.kind is IssueKind.TYPE_ERROR


class TestArchiveSchema:
    def test_snow_maps_to_float64(self):
        """compaction이 `types[0]`로 archive 스키마를 만든다. 빠지면 KeyError로 터진다."""
        from tests.test_compaction import _config

        schema = archive_schema(_config(columns={"SNO": ColumnSpec(types=("snow",))}))

        assert schema.field("SNO").type == pa.float64()
