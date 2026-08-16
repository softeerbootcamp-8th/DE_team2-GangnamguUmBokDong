"""테이블별 실행 스펙(silver source_id, transform 함수, upsert 충돌/갱신 컬럼)을 모은다.

main.py가 `--table`로 받은 이름을 이 레지스트리에서 찾아 실행한다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

import transform

SOURCE_BIKE_STATION_REALTIME = "bike_station_realtime"
SOURCE_WEATHER_ULTRA_SHORT_TERM = "weather_ultra_short_term"
SOURCE_WEATHER_SHORT_TERM_FORECAST = "weather_short_term_forecast"
SOURCE_CULTURAL_EVENT = "cultural_event"


@dataclass(frozen=True)
class TableSpec:
    source_id: str
    transform: Callable
    conflict_cols: list[str]
    update_cols: list[str]


TABLE_SPECS: dict[str, TableSpec] = {
    "stations": TableSpec(
        source_id=SOURCE_BIKE_STATION_REALTIME,
        transform=transform.stations_from_silver,
        conflict_cols=["sta_id"],
        update_cols=["sta_nm", "gu", "sta_addr", "lat", "lon", "hold_cnt"],
    ),
    "station_stock": TableSpec(
        source_id=SOURCE_BIKE_STATION_REALTIME,
        transform=transform.station_stock_from_silver,
        conflict_cols=["sta_id", "observed_at"],
        update_cols=["parking_bike_tot_cnt"],
    ),
    "weather_current": TableSpec(
        source_id=SOURCE_WEATHER_ULTRA_SHORT_TERM,
        transform=transform.weather_current_from_silver,
        conflict_cols=["gu"],
        update_cols=["observed_at", "temperature", "humidity", "wind_speed", "rainfall", "pty_type"],
    ),
    "weather_forecast": TableSpec(
        source_id=SOURCE_WEATHER_SHORT_TERM_FORECAST,
        transform=transform.weather_forecast_from_silver,
        conflict_cols=["gu", "forecast_dttm"],
        update_cols=["temperature", "precip_prob", "sky_cond", "pty_type"],
    ),
    "cultural_events": TableSpec(
        source_id=SOURCE_CULTURAL_EVENT,
        transform=transform.cultural_events_from_silver,
        conflict_cols=["event_id"],
        update_cols=["title", "category", "gu", "place", "start_date", "end_date", "is_free", "lat", "lon"],
    ),
}
