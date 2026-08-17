"""main.py: CLI 인자 파싱과 baseline 날짜 판정 테스트."""

from datetime import date, datetime

import pyarrow as pa
import pytest

import main
import storage
from tests.conftest import KST


class TestParseArgs:
    def test_requires_window_start(self):
        with pytest.raises(SystemExit):
            main.parse_args([])

    def test_default_baseline_date_mode_is_strict(self):
        args = main.parse_args(["--window-start", "2026-08-12T14:05:00+09:00"])
        assert args.baseline_date_mode == "strict"

    def test_explicit_latest_mode(self):
        args = main.parse_args([
            "--window-start", "2026-08-12T14:05:00+09:00",
            "--baseline-date-mode", "latest",
        ])
        assert args.baseline_date_mode == "latest"

    def test_rejects_unknown_mode(self):
        with pytest.raises(SystemExit):
            main.parse_args([
                "--window-start", "2026-08-12T14:05:00+09:00",
                "--baseline-date-mode", "bogus",
            ])


class TestResolveBaselineDate:
    def test_strict_uses_window_start_date_when_partition_exists(self, monkeypatch):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        monkeypatch.setattr(storage, "partition_exists", lambda source_id, d: d == date(2026, 8, 12))

        result = main._resolve_baseline_date(window_start, "strict")

        assert result == date(2026, 8, 12)

    def test_strict_raises_when_partition_missing(self, monkeypatch):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        monkeypatch.setattr(storage, "partition_exists", lambda source_id, d: False)

        with pytest.raises(storage.PartitionNotFoundError):
            main._resolve_baseline_date(window_start, "strict")

    def test_latest_mode_ignores_window_start_date(self, monkeypatch):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        monkeypatch.setattr(storage, "find_latest_partition_date", lambda source_id: date(2026, 8, 8))

        result = main._resolve_baseline_date(window_start, "latest")

        assert result == date(2026, 8, 8)


class TestFilterGridRowsForHour:
    def test_keeps_only_matching_tt_and_dedupes_by_cell_id(self):
        table = pa.table({
            "CELL_ID": ["다사53815262", "다사53815262", "다사53815262"],
            "TT": ["13", "14", "14"],
            "H_DNG_CD": ["1100053", "1100053", "1100053"],
            "SPOP": [10.0, 20.0, 30.0],
            **{c: [None, None, None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert set(result.keys()) == {"다사53815262"}
        assert result["다사53815262"].spop == 30.0  # 마지막(TT=14) 중복 행이 남음

    def test_null_spop_becomes_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": pa.array([None], type=pa.float64()),
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert result["다사53815262"].spop == 0.0

    def test_null_ages_become_zero(self):
        table = pa.table({
            "CELL_ID": ["다사53815262"],
            "TT": ["14"],
            "H_DNG_CD": ["1100053"],
            "SPOP": [10.0],
            **{c: [None] for c in main.merge.AGE_COLUMNS},
        })

        result = main._filter_grid_rows_for_hour(table, hour=14)

        assert all(v == 0.0 for v in result["다사53815262"].ages.values())
