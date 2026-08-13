"""config.schema 모델 단위 테스트."""

from datetime import timedelta

import pytest
from pydantic import ValidationError

from config.schema import Backfill, ColumnSpec, Fetch, Quality, Range, Schedule, Storage, _parse_duration


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


class TestSchedule:
    def test_parses_interval(self):
        s = Schedule(interval="5m")
        assert s.interval == timedelta(minutes=5)

    def test_forbids_extra_keys(self):
        with pytest.raises(ValidationError):
            Schedule(interval="5m", cron="*/5 * * * *")


class TestStorage:
    def test_valid(self):
        s = Storage(bronze_format="json", silver_format="parquet", partition=["dt", "hh"])
        assert s.partition == ("dt", "hh")

    def test_requires_nonempty_partition(self):
        with pytest.raises(ValidationError):
            Storage(bronze_format="json", silver_format="parquet", partition=[])


class TestQuality:
    def test_defaults(self):
        q = Quality(max_drop_ratio=0.05)
        assert q.max_missing_ratio == 0.0
        assert q.allow_empty is False

    def test_max_drop_ratio_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            Quality(max_drop_ratio=1.5)

    def test_max_missing_ratio_out_of_range_rejected(self):
        with pytest.raises(ValidationError):
            Quality(max_drop_ratio=0.05, max_missing_ratio=-0.1)


class TestFetch:
    def test_budget_optional(self):
        assert Fetch().budget is None

    def test_budget_parses(self):
        assert Fetch(budget="2m30s").budget == timedelta(minutes=2, seconds=30)


class TestBackfill:
    def test_disabled_without_max_age_ok(self):
        b = Backfill()
        assert b.enabled is False
        assert b.max_age is None

    def test_enabled_without_max_age_rejected(self):
        with pytest.raises(ValidationError):
            Backfill(enabled=True)

    def test_enabled_with_max_age_ok(self):
        b = Backfill(enabled=True, max_age="7d")
        assert b.max_age == timedelta(days=7)
