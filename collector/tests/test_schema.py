"""config.schema 모델 단위 테스트."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from config.schema import ColumnSpec, Range, _parse_duration


class TestParseDuration:
    def test_minutes(self):
        assert _parse_duration("5m") == timedelta(minutes=5)

    def test_hours(self):
        assert _parse_duration("3h") == timedelta(hours=3)

    def test_days(self):
        assert _parse_duration("1d") == timedelta(days=1)

    def test_compound(self):
        assert _parse_duration("2m30s") == timedelta(minutes=2, seconds=30)

    def test_passthrough_timedelta(self):
        value = timedelta(hours=1)
        assert _parse_duration(value) is value

    def test_empty_string_rejected(self):
        with pytest.raises(ValueError):
            _parse_duration("")

    def test_garbage_rejected(self):
        with pytest.raises(ValueError):
            _parse_duration("five minutes")


class TestRange:
    def test_requires_both_min_and_max(self):
        with pytest.raises(ValidationError):
            Range(min=0)

    def test_requires_both_min_and_max_max_only(self):
        with pytest.raises(ValidationError):
            Range(max=200)

    def test_valid(self):
        r = Range(min=0, max=200)
        assert r.min == 0
        assert r.max == 200

    def test_forbids_extra_keys(self):
        with pytest.raises(ValidationError):
            Range(min=0, max=200, step=1)


class TestColumnSpec:
    def test_minimal(self):
        spec = ColumnSpec(types=["int"])
        assert spec.types == ("int",)
        assert spec.required is False
        assert spec.range is None

    def test_rejects_range_and_enum_together(self):
        with pytest.raises(ValidationError):
            ColumnSpec(types=["int"], range={"min": 0, "max": 200}, enum=[0, 1, 2])

    def test_allows_range_only(self):
        spec = ColumnSpec(types=["int"], range={"min": 0, "max": 200})
        assert spec.range.max == 200

    def test_allows_enum_only(self):
        spec = ColumnSpec(types=["int"], enum=[0, 1, 2, 3])
        assert spec.enum == (0, 1, 2, 3)

    def test_requires_at_least_one_type(self):
        with pytest.raises(ValidationError):
            ColumnSpec(types=[])
