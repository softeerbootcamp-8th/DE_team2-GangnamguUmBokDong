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
        assert len(cfg.column_map) == 16

    def test_rental_value_map_is_the_verified_mapping(self):
        """빈도로 추정하면 USR_002/USR_003이 뒤집힌다. 조인으로 확정한 값이다."""
        cfg = bootstrap_config.load("bike_rental_history")

        assert cfg.value_map["USR_CLS_CD"] == {
            "내국인": "USR_001", "외국인": "USR_002", "비회원": "USR_003",
        }

    def test_loads_history_api_kind_for_station(self):
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.kind == "history_api"
        assert cfg.service == "bikeListHist"
        assert cfg.window.from_column == "stationDt"

    def test_station_time_format_is_ten_digits(self):
        """8자리를 주면 API가 에러 없이 최신 스냅샷을 반환한다 — 조용히 틀린 데이터가 들어온다."""
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.time_format == "%Y%m%d%H"

    def test_unknown_source_raises(self):
        with pytest.raises(FileNotFoundError):
            bootstrap_config.load("nonexistent_source")

    def test_station_realtime_enables_dedup(self):
        """시간당 스테이션마다 완전 동일한 행이 2개씩 온다(실측) — bootstrap이 합쳐야 한다."""
        cfg = bootstrap_config.load("bike_station_realtime")

        assert cfg.dedup is True

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
