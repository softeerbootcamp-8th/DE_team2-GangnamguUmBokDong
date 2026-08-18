"""Silver key 생성 계약을 검증한다."""

import pandas as pd

from ml_core import silver_schema


def test_weather_tick_keys_include_realtime_five_minute_window():
    """5분 E2E DAG의 :05/:15 날씨 산출물을 inference 조회 목록에 포함한다."""
    keys = silver_schema.weather_tick_keys(pd.Timestamp("2026-08-17 18:35"), lookback_hours=0.5)

    assert keys[-1] == "silver/weather_ultra_short_live/dt=2026-08-17/hh=18/1835.parquet"
    assert "silver/weather_ultra_short_live/dt=2026-08-17/hh=18/1830.parquet" in keys
