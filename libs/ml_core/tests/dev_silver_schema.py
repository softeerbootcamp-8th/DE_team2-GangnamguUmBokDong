"""Silver key 생성 계약을 검증한다."""

import pandas as pd

from ml_core import silver_schema


def test_weather_tick_keys_include_realtime_five_minute_window():
    """5분 E2E DAG의 :05/:15 날씨 산출물을 inference 조회 목록에 포함한다."""
    keys = silver_schema.weather_tick_keys(pd.Timestamp("2026-08-17 18:35"), lookback_hours=0.5)

    assert keys[-1] == "silver/weather_ultra_short_live/dt=2026-08-17/hh=18/1835.parquet"
    assert "silver/weather_ultra_short_live/dt=2026-08-17/hh=18/1830.parquet" in keys

def test_population_normalized_tick_keys_cover_one_hour_without_future_tick():
    keys = silver_schema.population_normalized_tick_keys(
        pd.Timestamp("2026-08-17 20:27")
    )

    assert len(keys) == 13
    assert keys[0] == "silver/living_population_normalized/dt=2026-08-17/hh=19/1925.parquet"
    assert keys[-1] == "silver/living_population_normalized/dt=2026-08-17/hh=20/2025.parquet"
    assert not any(key.endswith("/2030.parquet") for key in keys)