"""테이블별 실행 스펙(silver source_id, transform 함수, upsert 충돌/갱신 컬럼)을 모은다.

main.py가 `--table`로 받은 이름을 이 레지스트리에서 찾아 실행한다.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime

import pandas as pd

import s3_reader
import transform

SOURCE_BIKE_STATION_REALTIME = "bike_station_realtime"
SOURCE_WEATHER_ULTRA_SHORT_LIVE = "weather_ultra_short_live"
SOURCE_WEATHER_SHORT_TERM_FORECAST = "weather_short_term_forecast"
SOURCE_CULTURAL_EVENT = "cultural_event"
SOURCE_PERFORMANCE_EVENT = "performance_event"
SOURCE_WEATHER_ULTRA_SHORT_FORECAST = "weather_ultra_short_forecast"
# ml/inference가 predictions/dt=.../hh=.../inference_{HHMM}.parquet에 쓰는 결과물 —
# collector가 정의한 source_id가 아니라 의도를 나타내는 sentinel일 뿐이며,
# read_predictions()가 실제 읽기 경로를 담당한다(TABLE_SPECS["forecast_points"] 참고).
SOURCE_ML_PREDICTIONS = "ml_predictions"


def _read_silver_as_pandas(source_id: str, window_start: datetime) -> pd.DataFrame:
    return s3_reader.read_silver(source_id, window_start).to_pandas()


@dataclass(frozen=True)
class TableSpec:
    source_id: str
    transform: Callable
    conflict_cols: list[str]
    update_cols: list[str]
    reader: Callable[[datetime], pd.DataFrame] | None = None

    def read(self, window_start: datetime) -> pd.DataFrame:
        if self.reader is not None:
            return self.reader(window_start)
        return _read_silver_as_pandas(self.source_id, window_start)


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
        source_id=SOURCE_WEATHER_ULTRA_SHORT_LIVE,
        transform=transform.weather_current_from_silver,
        conflict_cols=["gu"],
        update_cols=["observed_at", "temperature", "humidity", "wind_speed", "rainfall", "pty_type"],
    ),
    "weather_forecast": TableSpec(
        source_id=SOURCE_WEATHER_SHORT_TERM_FORECAST,
        transform=transform.weather_forecast_from_silver,
        conflict_cols=["gu", "forecast_dttm"],
        update_cols=["sky_cond", "pty_type", "temperature", "precip_prob", "precip_amount", "humidity", "wind_speed", "base_dttm"],
    ),
    "weather_forecast_ultra": TableSpec(
        source_id=SOURCE_WEATHER_ULTRA_SHORT_FORECAST,
        transform=transform.weather_forecast_ultra_from_silver,
        conflict_cols=["gu", "forecast_dttm"],
        update_cols=["sky_cond", "pty_type", "temperature", "precip_prob", "precip_amount", "humidity", "wind_speed", "base_dttm"],
    ),
    "cultural_events": TableSpec(
        source_id=SOURCE_CULTURAL_EVENT,
        transform=transform.cultural_events_from_silver,
        conflict_cols=["event_id"],
        update_cols=["title", "category", "gu", "place", "start_date", "end_date", "is_free", "lat", "lon"],
    ),
    "cultural_events_performance": TableSpec(
        source_id=SOURCE_PERFORMANCE_EVENT,
        transform=transform.performance_events_from_silver,
        conflict_cols=["event_id"],
        update_cols=["title", "category", "gu", "place", "start_date", "end_date", "is_free", "lat", "lon"],
    ),
    "forecast_points": TableSpec(
        source_id=SOURCE_ML_PREDICTIONS,
        transform=transform.forecast_points_from_predictions,
        conflict_cols=["sta_id", "predicted_dttm"],
        update_cols=["predicted_rent_cnt", "predicted_return_cnt", "batch_run_at"],
        reader=lambda window_start: s3_reader.read_predictions(window_start).to_pandas(),
    ),
}
