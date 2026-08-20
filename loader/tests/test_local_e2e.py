"""로컬 E2E fixture의 결정적인 변환과 입력 검증을 테스트한다."""

from __future__ import annotations

from datetime import datetime

import local_e2e
import pytest
from ml_core.serving_release import validate_station_profile_payload


def test_parse_logical_dttm_requires_five_minute_boundary() -> None:
    """Fixture logical time은 offset을 가진 5분 경계만 허용한다."""
    parsed = local_e2e._parse_logical_dttm("2026-08-20T16:40:00+09:00")

    assert parsed == datetime.fromisoformat("2026-08-20T16:40:00+09:00")
    with pytest.raises(ValueError, match="5분 경계"):
        local_e2e._parse_logical_dttm("2026-08-20T16:42:00+09:00")
    with pytest.raises(ValueError, match="timezone offset"):
        local_e2e._parse_logical_dttm("2026-08-20T16:40:00")


def test_local_station_asset_builds_serving_rows() -> None:
    """Repository station 자산이 모델 int16 범위의 유효한 행을 충분히 제공한다."""
    stations = local_e2e._load_stations()

    assert len(stations) > 2_000
    assert tuple(station["station_no"] for station in stations) == tuple(
        sorted(station["station_no"] for station in stations)
    )
    assert len({station["station_id"] for station in stations}) == len(stations)


def test_nowcast_fixture_has_every_hour_and_age_column() -> None:
    """Nowcast fixture는 normalizer가 요구하는 24시간·연령 스키마를 가진다."""
    table = local_e2e._nowcast_table()

    assert table.num_rows == 24
    assert tuple(table.column("TT").to_pylist()) == tuple(range(24))
    assert set(local_e2e._AGE_COLUMNS).issubset(table.column_names)


def test_station_profile_fixture_passes_release_validator() -> None:
    """작은 profile도 model category와 global minute grid 계약을 만족한다."""
    station_nos = tuple(range(1, 101))
    payload = local_e2e._station_profile_payload(station_nos)

    verified = validate_station_profile_payload(
        payload,
        expected_grid_tick_minutes=local_e2e.common_config.GRID_TICK_MINUTES,
    )

    assert verified.station_nos == station_nos
    assert verified.row_count == len(station_nos)
