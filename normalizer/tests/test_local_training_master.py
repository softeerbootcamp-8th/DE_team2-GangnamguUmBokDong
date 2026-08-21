"""로컬 학습용 station master 준비 경계를 검증한다."""

from __future__ import annotations

import local_training_master
import pyarrow as pa
import pytest


def test_fixture_write_requires_opt_in_and_local_endpoint(monkeypatch) -> None:
    """명시적 opt-in과 로컬 HTTP endpoint만 허용한다."""
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:9000")
    monkeypatch.delenv("LOCAL_TRAINING_SMOKE_ALLOW_WRITE", raising=False)
    with pytest.raises(ValueError, match="opt-in"):
        local_training_master._require_local_environment()

    monkeypatch.setenv("LOCAL_TRAINING_SMOKE_ALLOW_WRITE", "1")
    local_training_master._require_local_environment()
    monkeypatch.setenv("S3_ENDPOINT_URL", "https://s3.amazonaws.com")
    with pytest.raises(ValueError, match="로컬 HTTP"):
        local_training_master._require_local_environment()


def test_master_preserves_real_station_id_and_coordinates() -> None:
    """Archive의 실제 station ID와 좌표를 보존하며 최신 행을 선택한다."""
    table = pa.Table.from_pylist(
        [
            {
                "stationId": "ST-9",
                "stationName": "이전 이름",
                "stationLatitude": 37.50,
                "stationLongitude": 127.00,
            },
            {
                "stationId": "ST-9",
                "stationName": "현재 이름",
                "stationLatitude": 37.51,
                "stationLongitude": 127.01,
            },
            {
                "stationId": "ST-4",
                "stationName": "다른 대여소",
                "stationLatitude": 37.52,
                "stationLongitude": 127.02,
            },
        ]
    )

    result = local_training_master._master_from_station_archive(table)

    assert result["RNTLS_ID"].to_pylist() == ["ST-4", "ST-9"]
    assert result["ADDR1"].to_pylist() == ["다른 대여소", "현재 이름"]
    assert result["LAT"].to_pylist() == [37.52, 37.51]
