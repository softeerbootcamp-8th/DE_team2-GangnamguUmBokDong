"""로컬 E2E fixture의 결정적인 변환과 입력 검증을 테스트한다."""

from __future__ import annotations

import importlib.util
from datetime import datetime
from pathlib import Path

import local_e2e
import pyarrow as pa
import pytest
from ml_core.serving_release import validate_station_profile_payload

_E2E_TIME_PATH = Path(__file__).resolve().parents[2] / "ops/e2e_time.py"
_E2E_TIME_SPEC = importlib.util.spec_from_file_location("e2e_time", _E2E_TIME_PATH)
assert _E2E_TIME_SPEC is not None and _E2E_TIME_SPEC.loader is not None
_E2E_TIME = importlib.util.module_from_spec(_E2E_TIME_SPEC)
_E2E_TIME_SPEC.loader.exec_module(_E2E_TIME)


def test_parse_logical_dttm_requires_five_minute_boundary() -> None:
    """Fixture logical time은 offset을 가진 5분 경계만 허용한다."""
    parsed = local_e2e._parse_logical_dttm("2026-08-20T16:40:00+09:00")

    assert parsed == datetime.fromisoformat("2026-08-20T16:40:00+09:00")
    with pytest.raises(ValueError, match="5분 경계"):
        local_e2e._parse_logical_dttm("2026-08-20T16:42:00+09:00")
    with pytest.raises(ValueError, match="timezone offset"):
        local_e2e._parse_logical_dttm("2026-08-20T16:40:00")


@pytest.mark.parametrize(
    ("logical", "expected"),
    (
        ("2026-08-20T16:40:00+09:00", "2026-08-20T16:35:00+09:00"),
        ("2026-08-20T00:00:00+09:00", "2026-08-19T23:55:00+09:00"),
        ("2026-01-01T00:00:00+09:00", "2025-12-31T23:55:00+09:00"),
    ),
)
def test_station_source_dttm_preserves_offset_across_boundaries(
    logical: str,
    expected: str,
) -> None:
    """5분 감산이 offset을 보존하며 날짜와 연도 경계를 넘는다."""
    assert _E2E_TIME.station_source_dttm(logical) == expected


@pytest.mark.parametrize(
    "logical",
    (
        "2026-08-20T16:40:00",
        "2026-08-20T16:41:00+09:00",
        "2026-08-20T16:40:01+09:00",
        "not-a-datetime",
    ),
)
def test_station_source_dttm_rejects_invalid_logical_time(logical: str) -> None:
    """Timezone 또는 정확한 5분 경계가 없는 입력을 거부한다."""
    with pytest.raises(ValueError):
        _E2E_TIME.station_source_dttm(logical)


def test_fixture_environment_requires_opt_in_and_local_endpoints(monkeypatch) -> None:
    """Fixture 게시 경계는 명시적 opt-in과 로컬 host를 모두 요구한다."""
    monkeypatch.setenv(
        "DATABASE_URL", "postgresql://postgres:postgres@postgres:5432/app"
    )
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://minio:9000")
    monkeypatch.delenv("LOCAL_E2E_ALLOW_FIXTURE", raising=False)

    with pytest.raises(ValueError, match="opt-in"):
        local_e2e._require_local_fixture_environment()

    monkeypatch.setenv("LOCAL_E2E_ALLOW_FIXTURE", "1")
    local_e2e._require_local_fixture_environment()
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.amazonaws.com")
    with pytest.raises(ValueError, match="scheme|host"):
        local_e2e._require_local_fixture_environment()


def test_realtime_station_snapshot_builds_serving_rows() -> None:
    """실제 station ID와 좌표를 보존하며 모델용 번호만 결정적으로 부여한다."""
    table = pa.Table.from_pylist(
        [
            {
                "stationId": "ST-9",
                "stationName": "두 번째 대여소",
                "rackTotCnt": 12,
                "stationLatitude": 37.51,
                "stationLongitude": 127.01,
            },
            {
                "stationId": "ST-4",
                "stationName": "첫 번째 대여소",
                "rackTotCnt": 15,
                "stationLatitude": 37.50,
                "stationLongitude": 127.00,
            },
        ]
    )

    stations = local_e2e._stations_from_realtime(table)

    assert tuple(station["station_id"] for station in stations) == ("ST-4", "ST-9")
    assert tuple(station["station_no"] for station in stations) == (1, 2)
    assert stations[0]["lat"] == 37.50
    assert stations[0]["capacity"] == 15


def test_nowcast_fixture_has_every_hour_and_age_column() -> None:
    """Nowcast fixture는 normalizer가 요구하는 24시간·연령 스키마를 가진다."""
    table = local_e2e._nowcast_table()

    assert table.num_rows == 24
    assert tuple(table.column("TT").to_pylist()) == tuple(range(24))
    assert set(local_e2e._AGE_COLUMNS).issubset(table.column_names)


def test_weather_forecast_fixture_covers_every_grid_and_hour() -> None:
    """단기예보 fixture는 Gold resolver의 34 grid×13시간을 완전히 덮는다."""
    logical = datetime.fromisoformat("2026-08-20T16:40:00+09:00")
    base = local_e2e._floor_logical(logical, 180)

    table = local_e2e._weather_forecast_table(
        logical,
        base,
        source_id="weather_short_term_forecast",
    )

    assert table.num_rows == 34 * 13
    assert (
        len(set(zip(table["nx"].to_pylist(), table["ny"].to_pylist(), strict=True)))
        == 34
    )
    assert (
        len(
            set(
                zip(
                    table["fcstDate"].to_pylist(),
                    table["fcstTime"].to_pylist(),
                    strict=True,
                )
            )
        )
        == 13
    )


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
