"""Collector Silver 계층 스키마 — 컬럼명 매핑 + S3 키 빌더.

기준 스키마는 `dev/seed_s3_from_local.py`(실제로 MinIO에 데이터를 넣는 유일한
스크립트, `dev/S3_DATA_CATALOG.md`와 일치 확인됨)다. `collector`의 실제 수집
어댑터는 아직 구현되지 않았다(docstring만 있음) — 그래서 이 파일은 "실제로
검증 가능한 유일한 Silver 데이터"를 기준으로 하고, 문서상 계획과 다른 부분
(예: `rental`의 실제 계획된 수집 주기는 5분이지만 시딩 스크립트는 1시간 단위)은
`docs/collector/ml-integration-requests.md`에 별도로 남긴다.

키 생성 규칙(`silver_key()`)은 `collector/storage.py`의 `_layer_key()`와 정확히
같은 문자열 포맷을 쓴다 — 그래야 실제 collector가 나중에 쓰기 시작해도 같은
위치를 가리킨다. `ml_common`은 `collector`를 import하지 않는다(서로 다른
인스턴스에 독립 배포되는 모듈이라 의존 관계를 만들면 안 됨) — 같은 규칙을
독립적으로 복제해서 쓴다.
"""

from __future__ import annotations

import pandas as pd

STATION_MASTER_KEY = "silver/station/station_master.parquet"

# dev/seed_s3_from_local.py의 seed_station_master() 기준.
STATION_COLUMN_MAP = {
    "sta_id": "station_id",
    "sta_no": "station_no",
    "sta_nm": "station_name",
    "hold_cnt": "capacity",
    "lat": "lat",
    "lon": "lon",
    "grid_id": "grid_id",
}

# dev/seed_s3_from_local.py의 seed_bike_station_realtime() 기준 — 시각은
# 데이터 컬럼이 아니라 파일 경로(dt=/hh=/HHMM)에서만 나온다.
BIKE_REALTIME_COLUMN_MAP = {
    "stationId": "station_id",
    "stationName": "station_name",
    "rackTotCnt": "capacity",
    "parkingBikeTotCnt": "bike_count",
    "stationLatitude": "lat",
    "stationLongitude": "lon",
}

# dev/seed_s3_from_local.py의 seed_rental_history() 기준 — rent_sta_id/
# rtn_sta_id의 raw-숫자 vs "ST-" 접두 여부는 공식 스키마 문서(DataSchema.md)
# 자체에 "물리 FK 적용 여부는 과거 폐쇄 대여소 확인 후 결정"이라고 명시돼
# 있어 미정이다 — 지금은 시딩 스크립트가 넣는 5자리 zero-pad 숫자 문자열
# 기준으로 두고, `normalize_station_no()`(libs/ml_common/trip_events.py)로
# 방어적으로 매칭한다.
RENTAL_COLUMN_MAP = {
    "rent_dttm": "start_dt",
    "rtn_dttm": "end_dt",
    "rent_sta_id": "start_st",
    "rtn_sta_id": "end_st",
    "use_min": "duration_min",
    "use_dst": "distance_m",
    "bike_id": "bike_id",
}

# dev/seed_s3_from_local.py의 seed_weather_forecast() 기준.
WEATHER_COLUMN_MAP = {
    "forecast_dttm": "hour_ts",
    "temperature": "temp",
    "precipitation_amount": "precip",
    "wind_speed": "wind",
    "humidity": "humidity",
}

# dev/seed_s3_from_local.py의 seed_population() 기준.
POPULATION_COLUMN_MAP = {
    "base_dttm": "hour_ts",
    "pop_grid_id": "grid_id",
    "living_pop_tot": "pop_total",
    "pop_resd": "pop_resd",
    "pop_long_foreign": "pop_long_foreign",
    "pop_short_foreign": "pop_short_foreign",
}

# 실제 collector 계획 문서(implementation-plan.md)에는 weather_forecast/
# living_population_per_population_grid의 정확한 source_id가 명시돼 있지
# 않고, rental도 "bike_rental_history"라는 이름 후보만 있다 — 지금은 dev
# 시딩 스크립트가 실제로 쓰는 이 4개 문자열을 기준으로 한다(docs/collector/
# ml-integration-requests.md에 확정 요청 남김).
BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
RENTAL_SOURCE_ID = "rental"
WEATHER_SOURCE_ID = "weather_forecast"
POPULATION_SOURCE_ID = "living_population_per_population_grid"


def silver_key(source_id: str, window_start: pd.Timestamp) -> str:
    """collector/storage.py의 `_layer_key("silver", ...)`와 동일한 키 규칙."""
    return f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{window_start:%H%M}.parquet"


def bike_realtime_tick_keys(anchor_ts: pd.Timestamp, lookback_hours: float = 1.0) -> list[str]:
    """anchor_ts부터 과거 lookback_hours시간 동안의 5분 tick 키를 전부 만든다(anchor_ts 포함).

    args:
        anchor_ts: 조회 기준 시각(5분 tick 위여야 함)
        lookback_hours: 몇 시간 전까지 볼지
    returns:
        오래된 것부터 최신 순으로 정렬된 키 목록
    """
    n_ticks = round(lookback_hours * 60 / 5) + 1
    ticks = [anchor_ts - pd.Timedelta(minutes=5 * i) for i in range(n_ticks - 1, -1, -1)]
    return [silver_key(BIKE_REALTIME_SOURCE_ID, t) for t in ticks]


def predictions_key(window_start: pd.Timestamp) -> str:
    """추론 결과 저장 키 — Silver와 같은 dt=/hh= 파티션 규칙, 최상위 prefix만 다르다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def predictions_failed_key(window_start: pd.Timestamp) -> str:
    """부분실패 station 목록 저장 키 — `predictions_key()`와 같은 시각, 파일명만 다르다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}_failed.json"


def hourly_keys(source_id: str, anchor_ts: pd.Timestamp, lookback_hours: int) -> list[str]:
    """anchor_ts가 속한 시간부터 과거 lookback_hours시간 동안의 정시(매시 0분) 키를 만든다.

    `rental`/`weather_forecast`/`living_population_per_population_grid`는
    dev 시딩 스크립트 기준 전부 시간 단위 파일 하나씩이다(실제 collector가
    다른 주기로 쌓기 시작하면 이 함수도 같이 고쳐야 함 — ml-integration-
    requests.md 참고).

    args:
        source_id: 조회할 Silver 소스 이름
        anchor_ts: 조회 기준 시각
        lookback_hours: 몇 시간 전까지 볼지(anchor_ts가 속한 시간 포함)
    returns:
        오래된 것부터 최신 순으로 정렬된 키 목록
    """
    start_hour = anchor_ts.floor("h")
    hours = [start_hour - pd.Timedelta(hours=i) for i in range(lookback_hours, -1, -1)]
    return [silver_key(source_id, h) for h in hours]
