"""compaction 실행 경로를 moto S3로 검증한다 — 압축·건너뛰기·격리·manifest."""

from datetime import date, datetime

import pyarrow as pa
import pytest

from compaction import RECOVERY_DAYS, compact_date, compact_range, target_dates
from config.schema import Backfill, ColumnSpec, Policies, Quality, Schedule, SourceConfig
from config.schema import Compaction as CompactionConfig
from config.schema import Storage as StorageConfig
from core.s3 import read_parquet
from storage import read_archive_manifest, write_silver
from tests.conftest import KST

DAY = date(2026, 8, 12)
TODAY = date(2026, 8, 13)


def _config(**overrides):
    fields = {
        "source_id": "t_source",
        "description": "테스트 소스",
        "adapter": "t_adapter",
        "schedule": Schedule(interval="5m"),
        "storage": StorageConfig(bronze_format="json", silver_format="parquet", partition=("dt", "hh")),
        "quality": Quality(max_drop_ratio=0.5, max_missing_ratio=0.0, allow_empty=False),
        "policies": Policies(
            required_missing="drop_row", required_outlier="drop_row",
            optional_missing="keep_null", optional_outlier="set_null",
        ),
        "columns": {"sta": ColumnSpec(types=("str",)), "cnt": ColumnSpec(types=("int",))},
        "config_version": "v1",
    }
    fields.update(overrides)
    return SourceConfig(**fields)


def _put_silver(source_id, minute, rows=2, day=DAY):
    """해당 날짜 hh=09의 HH:MM 윈도우에 silver 하나를 쓴다."""
    table = pa.table({
        "sta": [f"ST-{i}" for i in range(rows)],
        "cnt": list(range(rows)),
        "_row_status": ["ok"] * rows,
    })
    write_silver(source_id, datetime(day.year, day.month, day.day, 9, minute, tzinfo=KST), table)


class TestCompactDate:
    def test_row_count_is_preserved(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver("t_source", minute, rows=2)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 6

    def test_archive_is_readable_with_declared_schema(self):
        config = _config()
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.schema.names == ["sta", "cnt", "_row_status", "_window_start", "_source_kind"]
        assert table.schema.field("cnt").type == pa.int64()

    def test_marks_rows_as_collector_sourced(self):
        config = _config()
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert set(table.column("_source_kind").to_pylist()) == {"collector"}

    def test_window_start_distinguishes_source_files(self):
        config = _config()
        for minute in (5, 10, 15):
            _put_silver("t_source", minute, rows=2)

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert sorted(set(table.column("_window_start").to_pylist())) == [
            "2026-08-12T09:05:00+09:00",
            "2026-08-12T09:10:00+09:00",
            "2026-08-12T09:15:00+09:00",
        ]

    def test_empty_day_writes_nothing(self):
        config = _config()

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "empty"
        assert result.archive_key is None
        assert read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False) is None

    def test_dates_with_different_null_patterns_share_one_schema(self):
        """전량 결측 컬럼이 있는 날과 없는 날의 archive 스키마가 같아야 한다."""
        config = _config()
        _put_silver("t_source", 5)
        write_silver(
            "t_source",
            datetime(2026, 8, 11, 9, 5, tzinfo=KST),
            pa.table({"sta": ["ST-0"], "cnt": pa.array([None], type=pa.null()), "_row_status": ["ok"]}),
        )

        compact_date(config, DAY, today=TODAY)
        compact_date(config, date(2026, 8, 11), today=TODAY)

        a = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        b = read_parquet("archive/t_source/dt=2026-08-11.parquet", as_pandas=False)
        assert a.schema == b.schema


class TestChangeDetection:
    def test_second_run_skips_when_nothing_changed(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "skipped"

    def test_new_silver_file_triggers_recompaction(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        _put_silver("t_source", 10)
        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 4

    def test_overwritten_silver_file_triggers_recompaction(self):
        """백필은 같은 키를 다시 쓴다 — 키 목록만 보면 못 잡는다."""
        config = _config()
        _put_silver("t_source", 5, rows=2)
        compact_date(config, DAY, today=TODAY)

        _put_silver("t_source", 5, rows=5)
        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "compacted"
        assert result.rows == 5

    def test_force_recompacts_unchanged_date(self):
        config = _config()
        _put_silver("t_source", 5)
        compact_date(config, DAY, today=TODAY)

        result = compact_date(config, DAY, today=TODAY, force=True)

        assert result.status == "compacted"


class TestArchiveManifest:
    def test_records_completeness_against_expected_windows(self):
        config = _config()
        for minute in (5, 10):
            _put_silver("t_source", minute)

        compact_date(config, DAY, today=TODAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["expected_windows"] == 288
        assert manifest["found_windows"] == 2
        assert manifest["completeness"] == pytest.approx(2 / 288)

    def test_records_rows_and_archive_key(self):
        config = _config()
        _put_silver("t_source", 5, rows=3)

        compact_date(config, DAY, today=TODAY)

        manifest = read_archive_manifest("t_source", DAY)
        assert manifest["rows"] == 3
        assert manifest["archive_key"] == "archive/t_source/dt=2026-08-12.parquet"

    def test_backfill_window_open_inside_max_age(self):
        config = _config(backfill=Backfill(enabled=True, max_age="7d"))
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is False

    def test_backfill_window_closed_beyond_max_age(self):
        config = _config(backfill=Backfill(enabled=True, max_age="6h"))
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=date(2026, 8, 20))

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is True

    def test_backfill_window_always_closed_without_backfill(self):
        config = _config(backfill=None)
        _put_silver("t_source", 5)

        compact_date(config, DAY, today=TODAY)

        assert read_archive_manifest("t_source", DAY)["backfill_window_closed"] is True


class TestFailureIsolation:
    def test_cast_failure_does_not_write_archive_or_manifest(self):
        """부분 결과를 남기지 않아야 다음 실행이 자동 재시도한다."""
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert result.status == "failed"
        assert read_archive_manifest("t_source", DAY) is None
        assert read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False) is None

    def test_one_bad_date_does_not_block_others(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )
        write_silver(
            "t_source",
            datetime(2026, 8, 11, 9, 5, tzinfo=KST),
            pa.table({"cnt": [1], "_row_status": ["ok"]}),
        )

        results = compact_range(config, [date(2026, 8, 11), DAY], today=TODAY)

        by_day = {r.day: r.status for r in results}
        assert by_day[date(2026, 8, 11)] == "compacted"
        assert by_day[DAY] == "failed"

    def test_failure_records_the_reason(self):
        config = _config(columns={"cnt": ColumnSpec(types=("int",))})
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, 5, tzinfo=KST),
            pa.table({"cnt": ["숫자가 아님"], "_row_status": ["ok"]}),
        )

        result = compact_date(config, DAY, today=TODAY)

        assert "cnt" in result.error


class TestTargetDates:
    def test_spans_lookback_window_ending_today(self):
        config = _config(backfill=None)

        days = target_dates(config, TODAY)

        assert len(days) == RECOVERY_DAYS
        assert days[-1] == TODAY

    def test_longer_backfill_window_widens_the_range(self):
        config = _config(backfill=Backfill(enabled=True, max_age="7d"))

        assert len(target_dates(config, TODAY)) == 8

    def test_dates_are_ascending_and_contiguous(self):
        config = _config(backfill=None)

        days = target_dates(config, TODAY)

        assert days == sorted(days)
        assert (days[-1] - days[0]).days == len(days) - 1


class TestDedup:
    """`bike_rental_history`는 path_suffix가 날짜 단위인데 5분마다 돌아, 윈도우마다
    같은 날 데이터를 통째로 다시 받는다. 윈도우 중복은 완전히 동일한 행을 만들고,
    원본 자체의 중복은 값이 다르다(같은 대여인데 이용시간·이용거리가 미세하게 다른
    사례가 실측으로 확인됨). 이 차이를 이용해 무손실로 제거한다.
    """

    def _put(self, minute, rows):
        write_silver(
            "t_source",
            datetime(2026, 8, 12, 9, minute, tzinfo=KST),
            pa.table({
                "sta": [r[0] for r in rows],
                "cnt": [r[1] for r in rows],
                "_row_status": ["ok"] * len(rows),
            }),
        )

    def test_off_by_default_keeps_window_duplicates(self):
        """스냅샷 소스는 연속 윈도우가 같은 값을 내는 게 정상이라 지우면 안 된다."""
        config = _config()
        self._put(5, [("ST-1", 3)])
        self._put(10, [("ST-1", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 2

    def test_collapses_rows_repeated_across_windows(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])
        self._put(10, [("ST-1", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 1

    def test_keeps_earliest_window_start(self):
        """그 기록이 처음 보인 시점이 의미 있는 값이다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(10, [("ST-1", 3)])
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.column("_window_start").to_pylist() == ["2026-08-12T09:05:00+09:00"]

    def test_preserves_rows_differing_in_any_data_column(self):
        """원본의 진짜 중복(값이 다름)은 compaction이 판단해 지울 것이 아니다."""
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 31), ("ST-1", 32)])
        self._put(10, [("ST-1", 31), ("ST-1", 32)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 2

    def test_preserves_declared_schema(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 3)])

        compact_date(config, DAY, today=TODAY)

        table = read_parquet("archive/t_source/dt=2026-08-12.parquet", as_pandas=False)
        assert table.schema.names == ["sta", "cnt", "_row_status", "_window_start", "_source_kind"]
        assert table.schema.field("cnt").type == pa.int64()

    def test_distinct_rows_are_untouched(self):
        config = _config(compaction=CompactionConfig(dedup=True))
        self._put(5, [("ST-1", 1), ("ST-2", 2)])
        self._put(10, [("ST-3", 3)])

        result = compact_date(config, DAY, today=TODAY)

        assert result.rows == 3
