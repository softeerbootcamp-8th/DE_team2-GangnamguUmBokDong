"""CSV 입력 테스트: 인코딩·컬럼 매핑·값 매핑·날짜 버킷팅·파일명 범위 필터·Arrow 반환."""

from datetime import date

import pyarrow as pa
import pytest

from bootstrap.config import BootstrapConfig
from bootstrap.csv_source import read_by_date

HEADER = "자전거번호,대여일시,이용자종류,성별\n"


def _cfg(**overrides):
    fields = {
        "kind": "csv",
        "encoding": "cp949",
        "column_map": {
            "자전거번호": "BIKE_ID", "대여일시": "RENT_DT",
            "이용자종류": "USR_CLS_CD", "성별": "SEX_CD",
        },
        "value_map": {
            "USR_CLS_CD": {"내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003"},
            "SEX_CD": {"m": "M", "f": "F"},
        },
        "window": {"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
    }
    fields.update(overrides)
    return BootstrapConfig.model_validate(fields)


def _write(tmp_path, body, name="a.csv", encoding="cp949"):
    (tmp_path / name).write_text(HEADER + body, encoding=encoding)
    return tmp_path


class TestReadByDate:
    def test_renames_headers_to_physical_columns(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert list(result[date(2026, 6, 1)].to_pylist()[0].keys()) == [
            "BIKE_ID", "RENT_DT", "USR_CLS_CD", "SEX_CD",
        ]

    def test_applies_value_map(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,비회원,f\n")

        row = read_by_date(_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()[0]

        assert row["USR_CLS_CD"] == "USR_003"
        assert row["SEX_CD"] == "F"

    def test_leaves_unmapped_values_alone(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        row = read_by_date(_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()[0]

        assert row["SEX_CD"] == "M"

    def test_buckets_rows_by_date(self, tmp_path):
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n"
            "SPB-2,2026-06-02 01:10:00,내국인,M\n"
            "SPB-3,2026-06-01 23:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1), date(2026, 6, 2)})

        assert result[date(2026, 6, 1)].num_rows == 2
        assert result[date(2026, 6, 2)].num_rows == 1

    def test_survives_rows_that_are_not_date_sorted(self, tmp_path):
        """실측 CSV는 대여일시 순으로 완전히 정렬돼 있지 않다."""
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:30:00,내국인,M\n"
            "SPB-2,2026-06-02 00:00:00,내국인,M\n"
            "SPB-3,2026-06-01 00:18:46,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 2

    def test_skips_dates_not_requested(self, tmp_path):
        d = _write(tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n"
            "SPB-2,2026-06-05 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})

        assert set(result) == {date(2026, 6, 1)}

    def test_reads_multiple_files_in_the_directory(self, tmp_path):
        _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n", name="a.csv")
        _write(tmp_path, "SPB-2,2026-06-01 02:10:00,내국인,M\n", name="b.csv")

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 2

    def test_ignores_non_csv_files(self, tmp_path):
        _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")
        (tmp_path / "notes.txt").write_text("무시", encoding="utf-8")

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 1

    def test_requested_date_with_no_rows_is_absent(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1), date(2026, 6, 9)})

        assert date(2026, 6, 9) not in result

    def test_na_values_become_empty_string(self, tmp_path):
        """빈 문자열은 collector 검증 엔진이 결측으로 판정한다."""
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,\\N\n")

        row = read_by_date(_cfg(na_values=["\\N"]), d, {date(2026, 6, 1)})[
            date(2026, 6, 1)
        ].to_pylist()[0]

        assert row["SEX_CD"] == ""

    def test_unparseable_window_column_raises(self, tmp_path):
        d = _write(tmp_path, "SPB-1,날짜아님,내국인,M\n")

        with pytest.raises(ValueError):
            read_by_date(_cfg(), d, {date(2026, 6, 1)})


class TestFilenameRangeFilter:
    def test_file_outside_requested_month_is_not_opened(self, tmp_path):
        # 이 파일에만 있는 자전거번호가 결과에 나타나면 파일이 열린 것이다.
        _write(
            tmp_path,
            "SPB-JULY-ONLY,2026-07-01 00:10:00,내국인,M\n",
            name="서울특별시 공공자전거 대여이력 정보_2607.csv",
        )
        _write(
            tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n",
            name="서울특별시 공공자전거 대여이력 정보_2606.csv",
        )

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        bike_ids = {row["BIKE_ID"] for row in result[date(2026, 6, 1)].to_pylist()}
        assert bike_ids == {"SPB-1"}

    def test_file_outside_requested_month_would_fail_if_opened(self, tmp_path):
        """더 강한 확인: 범위 밖 파일 이름 자리에 디렉터리를 둬서, 열리면 반드시
        예외가 나게 만든다(디렉터리는 `.open()`할 수 없다)."""
        bad = tmp_path / "서울특별시 공공자전거 대여이력 정보_2607 (2).csv"
        bad.mkdir()
        _write(
            tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n",
            name="서울특별시 공공자전거 대여이력 정보_2606.csv",
        )

        # 예외 없이 끝나야 한다 — bad 파일은 열리지 않았다는 뜻.
        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 1

    def test_file_inside_requested_range_is_opened(self, tmp_path):
        _write(
            tmp_path,
            "SPB-1,2026-06-01 00:10:00,내국인,M\n",
            name="서울특별시 공공자전거 대여이력 정보_2606.csv",
        )

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 1

    def test_filename_without_extractable_yymm_is_still_read(self, tmp_path):
        _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n", name="random.csv")

        result = read_by_date(_cfg(), tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 1


class TestMissingHeaderAcrossVintages:
    """vintage마다 컬럼 수가 다르다 — 매핑에 있는 헤더가 CSV에 없어도 죽지 않아야 한다."""

    def _cfg_with_bike_se_cd(self, **overrides):
        fields = {
            "kind": "csv",
            "encoding": "cp949",
            "column_map": {
                "자전거번호": "BIKE_ID", "대여일시": "RENT_DT",
                "이용자종류": "USR_CLS_CD", "성별": "SEX_CD",
                "자전거구분": "BIKE_SE_CD",
            },
            "value_map": {
                "USR_CLS_CD": {"내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003"},
                "SEX_CD": {"m": "M", "f": "F"},
            },
            "window": {"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
        }
        fields.update(overrides)
        return BootstrapConfig.model_validate(fields)

    def test_reads_both_17_and_16_column_files_in_same_directory(self, tmp_path):
        (tmp_path / "wide.csv").write_text(
            "자전거번호,대여일시,이용자종류,성별,자전거구분\n"
            "SPB-WIDE,2026-06-01 00:10:00,내국인,M,일반자전거\n",
            encoding="cp949",
        )
        (tmp_path / "narrow.csv").write_text(
            "자전거번호,대여일시,이용자종류,성별\n"
            "SPB-NARROW,2026-06-01 00:20:00,내국인,M\n",
            encoding="cp949",
        )

        result = read_by_date(self._cfg_with_bike_se_cd(), tmp_path, {date(2026, 6, 1)})

        rows = {row["BIKE_ID"]: row for row in result[date(2026, 6, 1)].to_pylist()}
        assert set(rows) == {"SPB-WIDE", "SPB-NARROW"}
        assert rows["SPB-WIDE"]["BIKE_SE_CD"] == "일반자전거"
        assert rows["SPB-NARROW"]["BIKE_SE_CD"] == ""


class TestArrowReturnType:
    def test_returns_pyarrow_table_with_string_columns(self, tmp_path):
        d = _write(tmp_path, "SPB-1,2026-06-01 00:10:00,내국인,M\n")

        result = read_by_date(_cfg(), d, {date(2026, 6, 1)})
        table = result[date(2026, 6, 1)]

        assert isinstance(table, pa.Table)
        assert all(pa.types.is_string(field.type) for field in table.schema)


class TestChunkBoundary:
    def test_date_buckets_are_intact_across_chunk_boundary(self, tmp_path, monkeypatch):
        import bootstrap.csv_source as csv_source_module

        monkeypatch.setattr(csv_source_module, "_CHUNK_ROWS", 10)

        n = 25
        lines = []
        for i in range(n):
            day = "01" if i % 2 == 0 else "02"
            lines.append(f"SPB-{i},2026-06-{day} 00:10:00,내국인,M\n")
        d = _write(tmp_path, "".join(lines))

        result = read_by_date(_cfg(), d, {date(2026, 6, 1), date(2026, 6, 2)})

        assert result[date(2026, 6, 1)].num_rows == 13
        assert result[date(2026, 6, 2)].num_rows == 12
        total_bike_ids = {
            row["BIKE_ID"]
            for day in (date(2026, 6, 1), date(2026, 6, 2))
            for row in result[day].to_pylist()
        }
        assert total_bike_ids == {f"SPB-{i}" for i in range(n)}
