"""CSV 입력 테스트: 인코딩·컬럼 매핑·값 매핑·날짜 버킷팅·파일명 범위 필터·Arrow 반환."""

from datetime import date

import pyarrow as pa
import pytest

from bootstrap.config import BootstrapConfig
from bootstrap.csv_source import _format_component, read_by_date
from bootstrap.station_join import StationInfo, StationMap

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


# ---------------------------------------------------------------------------
# 상수 컬럼과 시각 컬럼 분해
#
# 기상청 ASOS 시간자료로 weather_ultra_short_live를 초기 로드할 때 필요하다.
# 그 CSV는 required 컬럼 네 개(nx·ny·baseDate·baseTime)를 하나도 직접 주지 못한다 —
# 격자가 없고(단일 관측 지점), `일시` 한 컬럼을 baseDate+baseTime 둘로 쪼개야 한다.
# column_map은 헤더 하나를 컬럼 하나로 옮길 뿐이라 둘 다 표현할 수 없었고, 그대로
# 돌리면 required_missing=drop_row 때문에 전 행이 폐기된다(실측 drop_ratio=1.000).
# ---------------------------------------------------------------------------

ASOS_HEADER = "일시,기온(°C),강수량(mm),풍속(m/s),풍향(16방위)\n"


def _asos_cfg(**overrides):
    fields = {
        "kind": "csv",
        "encoding": "cp949",
        "column_map": {"기온(°C)": "T1H", "강수량(mm)": "RN1",
                       "풍속(m/s)": "WSD", "풍향(16방위)": "VEC"},
        "constants": {"nx": "60", "ny": "127"},
        "derived_time": {
            "일시": {
                "parse": "%Y-%m-%d %H:%M",
                "into": {"baseDate": "%Y%m%d", "baseTime": "%H%M"},
            }
        },
        # ASOS는 무강수 시각의 강수량을 빈 문자열로 둔다 — 결측이 아니라 0이다.
        "value_map": {"RN1": {"": "0"}},
        "window": {"from_column": "baseDate", "format": "%Y%m%d"},
    }
    fields.update(overrides)
    return BootstrapConfig.model_validate(fields)


def _write_asos(tmp_path, body, name="weather_realtime_2026.csv"):
    (tmp_path / name).write_text(ASOS_HEADER + body, encoding="cp949")
    return tmp_path


class TestConstantColumns:
    def test_constants_appear_in_every_row(self, tmp_path):
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n2026-06-01 01:00,24.8,1.7,0.9,90\n")

        rows = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

        assert [r["nx"] for r in rows] == ["60", "60"]
        assert [r["ny"] for r in rows] == ["127", "127"]

    def test_constants_are_strings_so_the_validation_engine_casts_them(self, tmp_path):
        """csv_source는 전 컬럼을 문자열로 넘기고 캐스팅은 검증 엔진의 types가 맡는다."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n")

        table = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)]

        assert table.schema.field("nx").type == pa.string()

    def test_constant_colliding_with_column_map_is_rejected(self):
        with pytest.raises(ValueError):
            _asos_cfg(constants={"T1H": "0"})


class TestDerivedTimeColumns:
    def test_one_timestamp_column_becomes_two(self, tmp_path):
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n2026-06-01 13:40,24.8,1.7,0.9,90\n")

        rows = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

        assert [r["baseDate"] for r in rows] == ["20260601", "20260601"]
        assert [r["baseTime"] for r in rows] == ["0000", "1340"]

    def test_source_header_is_not_kept_as_a_column(self, tmp_path):
        """`일시`는 collector 컬럼이 아니라 분해 재료다 — archive에 남으면 스키마가 어긋난다."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n")

        table = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)]

        assert "일시" not in table.schema.names
        assert set(table.schema.names) == {"T1H", "RN1", "WSD", "VEC", "nx", "ny", "baseDate", "baseTime"}

    def test_window_column_can_be_a_derived_column(self, tmp_path):
        """날짜 버킷팅이 분해 결과를 읽어야 한다 — 분해가 _row_date보다 먼저 일어난다."""
        d = _write_asos(tmp_path, "2026-06-01 23:00,25.1,,1.2,270\n2026-06-02 00:00,24.8,,0.9,90\n")

        result = read_by_date(_asos_cfg(), d, {date(2026, 6, 1), date(2026, 6, 2)})

        assert sorted(result) == [date(2026, 6, 1), date(2026, 6, 2)]
        assert result[date(2026, 6, 1)].to_pylist()[0]["baseTime"] == "2300"
        assert result[date(2026, 6, 2)].to_pylist()[0]["baseTime"] == "0000"

    def test_unparseable_timestamp_raises(self, tmp_path):
        d = _write_asos(tmp_path, "2026/06/01 00:00,25.1,,1.2,270\n")

        with pytest.raises(ValueError):
            read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})

    def test_derived_target_colliding_with_column_map_is_rejected(self):
        with pytest.raises(ValueError):
            _asos_cfg(derived_time={"일시": {"parse": "%Y-%m-%d %H:%M", "into": {"T1H": "%H%M"}}})


class TestEmptyStringValueMap:
    def test_blank_becomes_zero_because_asos_omits_no_precipitation(self, tmp_path):
        """ASOS는 무강수를 빈 문자열로 둔다. 그대로 두면 검증 엔진이 결측으로 판정해
        precip이 90% null이 된다(실측: 22,995행 중 20,352행이 빈값)."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n2026-06-01 01:00,24.8,1.7,0.9,90\n")

        rows = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

        assert [r["RN1"] for r in rows] == ["0", "1.7"]

    def test_genuinely_missing_wind_stays_empty(self, tmp_path):
        """풍속의 빈값은 실제 결측이다(실측 31건). 0으로 바꾸면 안 된다."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,,\n")

        rows = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

        assert rows[0]["WSD"] == ""


class TestFilePattern:
    """한 디렉터리에 여러 소스의 CSV가 섞여 있을 수 있다. 실제로 `data/`에
    따릉이 대여이력과 ASOS 기상자료가 함께 있었고, 파일명 YYMM이 요청 범위와 겹치면
    다른 소스의 파일을 열어 시각 컬럼 파싱에서 죽었다."""

    def test_only_matching_files_are_read(self, tmp_path):
        (tmp_path / "weather_realtime_2026.csv").write_text(
            ASOS_HEADER + "2026-06-01 00:00,25.1,,1.2,270\n", encoding="cp949")
        # 다른 소스의 파일. 열면 `일시`가 없어 ValueError로 죽는다.
        (tmp_path / "따릉이_2606.csv").write_text(
            "자전거번호,대여일시\nSPB-1,2026-06-01 00:10:00\n", encoding="cp949")

        cfg = _asos_cfg(file_pattern="weather_realtime_*.csv")
        result = read_by_date(cfg, tmp_path, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 1

    def test_default_pattern_reads_every_csv(self, tmp_path):
        """기존 동작(패턴 미지정)은 그대로 유지한다."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,1.2,270\n", name="a.csv")
        _write_asos(d, "2026-06-01 01:00,24.8,,0.9,90\n", name="b.csv")

        result = read_by_date(_asos_cfg(), d, {date(2026, 6, 1)})

        assert result[date(2026, 6, 1)].num_rows == 2


class TestDerivedWind:
    """ASOS 시간자료에는 UUU·VVV가 없고 풍속·풍향만 있다. 같은 공식으로 채워
    운영 수집분과 컬럼을 맞춘다(`core.wind` 참고 — 실API 대조 검증됨).

    값을 소수 첫째 자리로 찍는 이유: 운영 수집분의 UUU·VVV가 그 자리까지만 온다.
    재계산으로 둘째 자리를 붙이면 같은 컬럼에 정밀도가 다른 값이 섞인다 — 게다가
    입력인 WSD·VEC가 이미 반올림된 값이라 그 자리는 실제 정밀도가 아니다.
    """

    def _cfg(self, **overrides):
        return _asos_cfg(
            derived_wind={"speed": "WSD", "direction": "VEC", "u": "UUU", "v": "VVV"},
            **overrides,
        )

    def _rows(self, tmp_path, body):
        d = _write_asos(tmp_path, body)
        return read_by_date(self._cfg(), d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

    def test_components_are_derived_from_speed_and_direction(self, tmp_path):
        # 서풍(270도) 3.0m/s -> 동쪽으로 부는 바람: u=+3.0, v=0.0
        rows = self._rows(tmp_path, "2026-06-01 00:00,25.1,,3.0,270\n")

        assert rows[0]["UUU"] == "3.0"
        assert rows[0]["VVV"] == "0.0"

    def test_north_wind_has_negative_north_south_component(self, tmp_path):
        rows = self._rows(tmp_path, "2026-06-01 00:00,25.1,,2.0,0\n")

        assert rows[0]["UUU"] == "0.0"
        assert rows[0]["VVV"] == "-2.0"

    def test_rounded_to_one_decimal_like_the_operational_source(self, tmp_path):
        rows = self._rows(tmp_path, "2026-06-01 00:00,25.1,,1.4,225\n")

        # -1.4*sin(225) = 0.9899..., -1.4*cos(225) = 0.9899...
        assert rows[0]["UUU"] == "1.0"
        assert rows[0]["VVV"] == "1.0"

    def test_missing_wind_leaves_components_empty(self, tmp_path):
        """풍속 결측은 실측 31건 있다. 0으로 채우면 '무풍'이 되어 뜻이 달라진다."""
        rows = self._rows(tmp_path, "2026-06-01 00:00,25.1,,,\n")

        assert rows[0]["WSD"] == ""
        assert rows[0]["UUU"] == ""
        assert rows[0]["VVV"] == ""

    def test_missing_direction_alone_also_leaves_components_empty(self, tmp_path):
        rows = self._rows(tmp_path, "2026-06-01 00:00,25.1,,2.0,\n")

        assert rows[0]["UUU"] == ""
        assert rows[0]["VVV"] == ""

    def test_negative_zero_is_normalised(self):
        """북풍(VEC=0)이면 동서 성분이 정확히 -0.0이고, 절댓값 0.05 미만의 음수도
        반올림 후 -0.0이 된다. 수치적으로 같은 값이 '0.0'과 '-0.0' 두 표기로 섞이면
        archive에서 비교·집계가 혼란해진다."""
        assert _format_component(-0.0) == "0.0"
        assert _format_component(-0.04) == "0.0"
        # 0.05 이상은 그대로 음수로 남아야 한다 — 정규화가 실제 값을 삼키면 안 된다.
        assert _format_component(-0.06) == "-0.1"
        assert _format_component(-1.25) == "-1.2"

    def test_target_colliding_with_column_map_is_rejected(self):
        with pytest.raises(ValueError):
            _asos_cfg(derived_wind={"speed": "WSD", "direction": "VEC", "u": "T1H", "v": "VVV"})

    def test_derivation_reads_values_after_value_map(self, tmp_path):
        """value_map이 풍속을 고칠 수도 있으므로 파생은 그 뒤에 일어나야 한다."""
        d = _write_asos(tmp_path, "2026-06-01 00:00,25.1,,-9,270\n")
        cfg = _asos_cfg(
            derived_wind={"speed": "WSD", "direction": "VEC", "u": "UUU", "v": "VVV"},
            value_map={"RN1": {"": "0"}, "WSD": {"-9": "3.0"}},
        )

        rows = read_by_date(cfg, d, {date(2026, 6, 1)})[date(2026, 6, 1)].to_pylist()

        assert rows[0]["WSD"] == "3.0"
        assert rows[0]["UUU"] == "3.0"


STOCK_HEADER = "일시,대여소번호,대여소명,시간대,거치대수량\n"


def _stock_cfg(**overrides):
    """재고 CSV(대여소별 공공자전거 대여가능 수량) 설정."""
    fields = {
        "kind": "csv",
        "encoding": "cp949",
        "column_map": {
            "대여소번호": "_station_no",
            "대여소명": "stationName",
            "거치대수량": "parkingBikeTotCnt",
        },
        "composed_time": {
            "stationDt": {
                "from": ["일시", "시간대"],
                "parse": "%Y-%m-%d %H",
                "format": "%Y%m%d%H",
            }
        },
        "window": {"from_column": "stationDt", "format": "%Y%m%d%H"},
    }
    fields.update(overrides)
    return BootstrapConfig.model_validate(fields)


def _write_stock(tmp_path, body, name="stock.csv"):
    (tmp_path / name).write_text(STOCK_HEADER + body, encoding="cp949")
    return tmp_path


class TestComposedTime:
    def test_joins_two_columns_into_one_time_column(self, tmp_path):
        d = _write_stock(tmp_path, "2025-12-01,00102,102. 망원역,7,8\n")

        row = read_by_date(_stock_cfg(), d, {date(2025, 12, 1)})[date(2025, 12, 1)].to_pylist()[0]

        assert row["stationDt"] == "2025120107"

    def test_accepts_hour_without_zero_padding(self, tmp_path):
        """CSV의 `시간대`는 `0`~`23`이라 한 자리 시각이 그대로 온다(실측)."""
        d = _write_stock(tmp_path, "2025-12-01,00102,102. 망원역,0,8\n")

        row = read_by_date(_stock_cfg(), d, {date(2025, 12, 1)})[date(2025, 12, 1)].to_pylist()[0]

        assert row["stationDt"] == "2025120100"

    def test_buckets_by_composed_date(self, tmp_path):
        d = _write_stock(tmp_path,
            "2025-12-01,00102,102. 망원역,23,8\n"
            "2025-12-02,00102,102. 망원역,0,9\n")

        result = read_by_date(_stock_cfg(), d, {date(2025, 12, 1)})

        assert result[date(2025, 12, 1)].num_rows == 1
        assert date(2025, 12, 2) not in result

    def test_raises_when_time_cannot_be_parsed(self, tmp_path):
        """조용히 빈 값으로 넘기면 날짜 버킷팅이 죽거나 행 전체가 사라진다."""
        d = _write_stock(tmp_path, "2025/12/01,00102,102. 망원역,7,8\n")

        with pytest.raises(ValueError, match="stationDt"):
            read_by_date(_stock_cfg(), d, {date(2025, 12, 1)})


class TestJoin:
    """CSV에 없는 대여소 컬럼을 매핑표에서 채운다."""

    def _cfg(self):
        return _stock_cfg(join={
            "provider": "bike_station",
            "by": {"number": "_station_no", "name": "stationName"},
            "fills": ["stationId", "rackTotCnt", "shared",
                      "stationLatitude", "stationLongitude"],
        })

    def _map(self, **entries):
        table = StationMap()
        for name, info in entries.items():
            table.by_name[name] = info
        return table

    def test_fills_columns_from_station_map(self, tmp_path):
        d = _write_stock(tmp_path, "2025-12-01,00102,102. 망원역,7,8\n")
        table = self._map(**{"102. 망원역": StationInfo(
            station_id="ST-4", rack_tot_cnt="15", shared="13",
            latitude="37.55", longitude="126.91")})

        row = read_by_date(self._cfg(), d, {date(2025, 12, 1)}, station_map=table)[
            date(2025, 12, 1)].to_pylist()[0]

        assert row["stationId"] == "ST-4"
        assert row["rackTotCnt"] == "15"
        assert row["shared"] == "13"
        assert (row["stationLatitude"], row["stationLongitude"]) == ("37.55", "126.91")

    def test_leaves_fills_empty_when_station_is_unknown(self, tmp_path):
        """빈 stationId는 required 결측이라 검증 엔진의 drop_row 정책이 행을 폐기한다."""
        d = _write_stock(tmp_path, "2025-12-01,09999,없는 대여소,7,8\n")

        row = read_by_date(self._cfg(), d, {date(2025, 12, 1)}, station_map=self._map())[
            date(2025, 12, 1)].to_pylist()[0]

        assert row["stationId"] == ""
        assert row["rackTotCnt"] == ""

    def test_keeps_partial_info_from_history_only_stations(self, tmp_path):
        """폐쇄 대여소는 stationId만 안다 — 나머지를 지어내지 않는다."""
        d = _write_stock(tmp_path, "2025-12-01,00211,211. 여의도역,7,8\n")
        table = self._map(**{"211. 여의도역": StationInfo(station_id="ST-99")})

        row = read_by_date(self._cfg(), d, {date(2025, 12, 1)}, station_map=table)[
            date(2025, 12, 1)].to_pylist()[0]

        assert row["stationId"] == "ST-99"
        assert row["shared"] == ""

    def test_join_key_column_is_dropped_from_output(self, tmp_path):
        """`_station_no`는 조인용 임시 컬럼이라 archive 스키마에 남지 않는다."""
        d = _write_stock(tmp_path, "2025-12-01,00102,102. 망원역,7,8\n")
        table = self._map(**{"102. 망원역": StationInfo(station_id="ST-4")})

        columns = read_by_date(self._cfg(), d, {date(2025, 12, 1)}, station_map=table)[
            date(2025, 12, 1)].column_names

        assert "_station_no" not in columns

    def test_raises_when_join_declared_but_map_missing(self, tmp_path):
        """매핑표 없이 돌면 stationId가 전부 비어 그 날짜가 통째로 폐기된다."""
        d = _write_stock(tmp_path, "2025-12-01,00102,102. 망원역,7,8\n")

        with pytest.raises(ValueError, match="station_map"):
            read_by_date(self._cfg(), d, {date(2025, 12, 1)})
