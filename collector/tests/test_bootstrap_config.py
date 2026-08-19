"""bootstrap 매핑 설정 로딩 테스트.

운영 설정(collector/sources/*.yaml)과 분리한 이유는 수명이 다르기 때문이다 —
bootstrap 설정은 한 번 쓰고 버리는데 운영 yaml은 5분마다 읽히고 오래 유지된다.
"""

import pytest
from pydantic import ValidationError

from bootstrap import config as bootstrap_config


class TestLoad:
    def test_loads_csv_kind_for_rental(self):
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.kind == "csv"
        assert cfg.encoding == "cp949"
        assert cfg.window.from_column == "RENT_DT"

    def test_rental_column_map_covers_all_csv_headers(self):
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.column_map["자전거번호"] == "BIKE_ID"
        assert cfg.column_map["대여대여소ID"] == "RENT_STATION_ID"
        assert cfg.column_map["자전거구분"] == "BIKE_SE_CD"
        assert len(cfg.column_map) == 17

    def test_rental_value_map_is_the_verified_mapping(self):
        """빈도로 추정하면 USR_002/USR_003이 뒤집힌다. 조인으로 확정한 값이다."""
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.value_map["USR_CLS_CD"] == {
            "내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003",
        }

    def test_loads_csv_kind_for_station(self):
        """재고는 과거 조회 API가 아니라 대여가능 수량 CSV로 채운다."""
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.kind == "csv"
        assert cfg.encoding == "cp949"
        assert cfg.window.from_column == "stationDt"

    def test_station_time_is_composed_from_two_columns(self):
        """CSV는 시각을 `일시`와 `시간대`로 나눠 준다."""
        cfg = bootstrap_config.load("bike_station_realtime")
        spec = cfg.composed_time["stationDt"]

        assert spec.from_ == ("일시", "시간대")
        assert spec.format == "%Y%m%d%H"

    def test_station_joins_columns_missing_from_csv(self):
        """CSV에는 stationId·거치대 수·거치율·좌표가 없다."""
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.join.provider == "bike_station"
        assert cfg.join.by.number == "_station_no"
        assert "stationId" in cfg.join.fills

    def test_unknown_source_raises(self):
        with pytest.raises(FileNotFoundError):
            bootstrap_config.load("nonexistent_source")

    def test_station_realtime_does_not_enable_dedup(self):
        """CSV는 (일시, 시간대, 대여소번호) 중복이 0건이다(실측 207만 행).

        과거 조회 API(`bikeListHist`)는 한 시각에 스테이션마다 행을 2개 줘서 dedup이
        필요했지만 CSV에는 그 문제가 없다.
        """
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.dedup is False

    def test_rental_history_does_not_enable_dedup(self):
        """대여이력은 이 문제가 없다 — 기본값(False)을 그대로 쓴다."""
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.dedup is False


class TestValidation:
    def test_history_api_requires_service(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="history_api",
                window={"from_column": "stationDt", "format": "%Y%m%d%H"},
                time_format="%Y%m%d%H",
            )

    def test_history_api_requires_time_format(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="history_api",
                window={"from_column": "stationDt", "format": "%Y%m%d%H"},
                service="bikeListHist",
            )

    def test_csv_requires_column_map(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="csv",
                window={"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
            )

    def test_unknown_field_is_rejected(self):
        with pytest.raises(ValidationError):
            bootstrap_config.BootstrapConfig(
                kind="csv",
                window={"from_column": "RENT_DT", "format": "%Y-%m-%d %H:%M:%S"},
                column_map={"a": "A"},
                oops=1,
            )


class TestComposedTime:
    """시각이 두 컬럼에 나뉜 CSV(`일시` + `시간대`)를 한 물리 컬럼으로 합치는 규칙."""

    def _cfg(self, **overrides):
        fields = {
            "kind": "csv",
            "column_map": {"거치대수량": "parkingBikeTotCnt"},
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
        return bootstrap_config.BootstrapConfig.model_validate(fields)

    def test_parses_from_alias(self):
        """yaml 키는 파이썬 예약어라 `from_`이 아니라 `from`으로 쓴다."""
        cfg = self._cfg()

        assert cfg.composed_time["stationDt"].from_ == ("일시", "시간대")

    def test_keeps_parse_and_format(self):
        cfg = self._cfg()
        spec = cfg.composed_time["stationDt"]

        assert (spec.parse, spec.format) == ("%Y-%m-%d %H", "%Y%m%d%H")

    def test_requires_at_least_one_source_column(self):
        with pytest.raises(ValidationError):
            self._cfg(composed_time={
                "stationDt": {"from": [], "parse": "%Y-%m-%d %H", "format": "%Y%m%d%H"}
            })

    def test_rejects_target_already_filled_by_column_map(self):
        """한 물리 컬럼을 두 경로가 채우면 어느 값이 남는지가 적용 순서에 달린다."""
        with pytest.raises(ValidationError):
            self._cfg(column_map={"일시": "stationDt"})


class TestJoin:
    """대여소번호를 외부 매핑표와 조인해 CSV에 없는 컬럼을 채우는 규칙."""

    def _cfg(self, **overrides):
        fields = {
            "kind": "csv",
            "column_map": {"대여소번호": "_station_no", "대여소명": "stationName"},
            "join": {
                "provider": "bike_station",
                "by": {"number": "_station_no", "name": "stationName"},
                "fills": ["stationId", "rackTotCnt"],
            },
            "window": {"from_column": "stationDt", "format": "%Y%m%d%H"},
        }
        fields.update(overrides)
        return bootstrap_config.BootstrapConfig.model_validate(fields)

    def test_keeps_join_keys_and_fills(self):
        cfg = self._cfg()

        assert cfg.join.by.number == "_station_no"
        assert cfg.join.by.name == "stationName"
        assert cfg.join.fills == ("stationId", "rackTotCnt")

    def test_rejects_unknown_provider(self):
        """provider는 station_join이 아는 이름만 허용한다 — 오타를 설정 로딩에서 끊는다."""
        with pytest.raises(ValidationError):
            self._cfg(join={
                "provider": "nope",
                "by": {"number": "_station_no", "name": "stationName"},
                "fills": ["stationId"],
            })

    def test_rejects_fill_already_filled_by_column_map(self):
        with pytest.raises(ValidationError):
            self._cfg(column_map={"대여소번호": "_station_no", "대여소명": "stationId"})

    def test_requires_at_least_one_fill(self):
        with pytest.raises(ValidationError):
            self._cfg(join={
                "provider": "bike_station",
                "by": {"number": "_station_no", "name": "stationName"},
                "fills": [],
            })

    def test_rejects_unknown_fill_column(self):
        """provider가 모르는 컬럼명은 조용히 빈 값이 되어 그 행이 통째로 폐기된다."""
        with pytest.raises(ValidationError):
            self._cfg(join={
                "provider": "bike_station",
                "by": {"number": "_station_no", "name": "stationName"},
                "fills": ["stationId", "rackTotCntt"],
            })
