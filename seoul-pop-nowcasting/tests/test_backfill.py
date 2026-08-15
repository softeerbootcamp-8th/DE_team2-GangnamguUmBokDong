"""backfill.py: CSV 로드 결과(YMD/TT/CELL_ID/SPOP 등)를 아카이브 스키마로 정규화."""

from datetime import date

import pyarrow as pa

import backfill


class TestAddEstimationColumns:
    def test_marks_rows_as_actual(self):
        table = pa.table({"CELL_ID": ["A", "B"], "SPOP": [1.0, 2.0]})

        result = backfill.add_estimation_columns(table)

        assert result.column("is_estimated").to_pylist() == [False, False]
        assert result.column("estimation_method").to_pylist() == ["actual", "actual"]
        assert result.column("CELL_ID").to_pylist() == ["A", "B"]


class TestGroupRowsByDate:
    def test_splits_table_into_one_table_per_ymd_value(self):
        table = pa.table(
            {
                "YMD": ["20260810", "20260810", "20260811"],
                "CELL_ID": ["A", "B", "C"],
                "SPOP": [1.0, 2.0, 3.0],
            }
        )

        grouped = backfill.group_rows_by_date(table)

        assert set(grouped.keys()) == {date(2026, 8, 10), date(2026, 8, 11)}
        assert sorted(grouped[date(2026, 8, 10)].column("CELL_ID").to_pylist()) == ["A", "B"]
        assert grouped[date(2026, 8, 11)].column("CELL_ID").to_pylist() == ["C"]

    def test_empty_table_returns_empty_dict(self):
        table = pa.table({"YMD": pa.array([], type=pa.string()), "CELL_ID": pa.array([], type=pa.string())})

        assert backfill.group_rows_by_date(table) == {}
