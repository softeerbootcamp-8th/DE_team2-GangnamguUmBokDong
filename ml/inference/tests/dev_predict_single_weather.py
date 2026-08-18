"""weather_10min/weather_3h Silver와 실시간 inference의 선택 계약."""

import pandas as pd
import pytest

from inference import predict_single as ps


def _forecast_row(*, date="20260817", time="2100", temp=25.0, precip="강수없음", short=False):
    row = {
        "fcstDate": date,
        "fcstTime": time,
        "REH": 80.0,
        "WSD": 1.5,
    }
    row.update({"TMP": temp, "PCP": precip} if short else {"T1H": temp, "RN1": precip})
    return row


def test_future_horizon_prefers_latest_ultra_short_forecast_as_of_anchor(monkeypatch):
    ps._weather_forecast_snapshot_cache = {}
    ultra = ps.silver_schema.WEATHER_ULTRA_FORECAST_SOURCE_ID
    keys = [
        f"silver/{ultra}/dt=2026-08-17/hh=19/1930.parquet",
        f"silver/{ultra}/dt=2026-08-17/hh=20/2030.parquet",
    ]
    monkeypatch.setattr(ps.s3_io, "list_keys", lambda prefix: keys)
    monkeypatch.setattr(
        ps.s3_io,
        "read_parquet",
        lambda key: pd.DataFrame(
            [
                _forecast_row(temp=24.0, precip="1mm 미만"),
                _forecast_row(temp=26.0, precip="강수없음"),
            ]
        ),
    )

    result = ps._get_recent_weather(
        pd.Timestamp("2026-08-17 21:00"),
        as_of_ts=pd.Timestamp("2026-08-17 19:35"),
    )

    assert result == pytest.approx({"temp": 25.0, "precip": 0.25, "wind": 1.5, "humidity": 80.0})


def test_future_horizon_uses_short_term_when_ultra_does_not_cover_target(monkeypatch):
    ps._weather_forecast_snapshot_cache = {}
    ultra = ps.silver_schema.WEATHER_ULTRA_FORECAST_SOURCE_ID
    short = ps.silver_schema.WEATHER_SHORT_FORECAST_SOURCE_ID

    def list_keys(prefix):
        source = ultra if ultra in prefix else short
        return [f"silver/{source}/dt=2026-08-17/hh=19/1930.parquet"]

    def read_parquet(key):
        if ultra in key:
            return pd.DataFrame([_forecast_row(time="2100")])
        return pd.DataFrame(
            [_forecast_row(date="20260818", time="0700", temp=20.0, precip="2.0mm", short=True)]
        )

    monkeypatch.setattr(ps.s3_io, "list_keys", list_keys)
    monkeypatch.setattr(ps.s3_io, "read_parquet", read_parquet)

    result = ps._get_recent_weather(
        pd.Timestamp("2026-08-18 07:00"),
        as_of_ts=pd.Timestamp("2026-08-17 19:35"),
    )

    assert result == pytest.approx({"temp": 20.0, "precip": 2.0, "wind": 1.5, "humidity": 80.0})


def test_current_horizon_uses_weather_10min_live_snapshot(monkeypatch):
    monkeypatch.setattr(
        ps.s3_io,
        "read_parquet_many",
        lambda keys: [pd.DataFrame([{"T1H": 27.0, "RN1": 0.0, "WSD": 1.0, "REH": 75.0}])],
    )
    monkeypatch.setattr(ps.s3_io, "list_keys", lambda prefix: pytest.fail("현재 시각에는 예보를 조회하면 안 됨"))

    result = ps._get_recent_weather(
        pd.Timestamp("2026-08-17 19:35"),
        as_of_ts=pd.Timestamp("2026-08-17 19:35"),
    )

    assert result == {"temp": 27.0, "precip": 0.0, "wind": 1.0, "humidity": 75.0}
