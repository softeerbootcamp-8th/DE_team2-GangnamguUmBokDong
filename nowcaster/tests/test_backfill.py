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


class TestReadSourceCsv:
    def test_reads_real_portal_header_and_renames_columns(self, tmp_path):
        csv_path = tmp_path / "250_LOCAL_RESD_20260808.csv"
        header = (
            '"일자","시간","행정동코드","250M격자","생활인구합계","남자 0~9세","남자 10~14세",'
            '"남자 15~19세","남자 20~24세","남자 25~29세","남자 30~34세","남자 35~39세",'
            '"남자 40~44세","남자 45~49세","남자 50~54세","남자 55~59세","남자 60~64세",'
            '"남자 65~69세","남자 70세 이상","여자 0~9세","여자 10~14세","여자 15~19세",'
            '"여자 20~24세","여자 25~29세","여자 30~34세","여자 35~39세","여자 40~44세",'
            '"여자 45~49세","여자 50~54세","여자 55~59세","여자 60~64세","여자 65~69세",'
            '"여자 70세 이상"'
        )
        row = (
            '"20260808","00","11110515     ","다사52505350","892.57","*","6.17","12.96","4.33",'
            '"33.36","23.66","31.98","49.81","38.39","21.13","37.59","20.11","14.31","64.56",'
            '"10.98","18.22","25.18","*","35.78","45.1","47.21","34.74","63.73","45.81","25.9",'
            '"31.24","48.9","100.2"'
        )
        csv_path.write_bytes((header + "\n" + row + "\n").encode("euc-kr"))

        table = backfill.read_source_csv(csv_path)

        assert table.column_names[:5] == ["YMD", "TT", "H_DNG_CD", "CELL_ID", "SPOP"]
        assert table.column("YMD").to_pylist() == ["20260808"]
        assert table.column("CELL_ID").to_pylist() == ["다사52505350"]
        assert table.column("SPOP").to_pylist() == [892.57]
        # 마스킹 문자 "*"는 null로 정규화된다
        assert table.column("M00").to_pylist() == [None]
        assert table.column("M10").to_pylist() == [6.17]
        assert table.column("F20").to_pylist() == [None]
        assert table.column("F70").to_pylist() == [100.2]
