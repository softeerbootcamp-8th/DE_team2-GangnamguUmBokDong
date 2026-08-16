"""Collector Silver 계층 스키마 — 컬럼명 매핑 + S3 키 빌더.

기준 스키마는 collector 팀이 실제로 수집해 넘겨준 예시 데이터(`ml/data/silver/*`,
2026-08-15 기준)다. `dev/seed_s3_from_local.py`(로컬 MinIO 시딩 스크립트)와
`docs/collector/DataSchema.md`/`implementation-plan.md`(계획 문서)는 이 실제
예시와 소스 이름·컬럼명·수집 주기가 상당히 달랐다 — 그 차이는
`docs/collector/ml-integration-requests.md`에 정리해뒀고, 이 파일은 실제
예시 데이터를 그대로 반영한다(계획 문서나 시딩 스크립트가 아니라).

키 생성 규칙(`silver_key()`)은 `collector/storage.py`의 `_layer_key()`와 정확히
같은 문자열 포맷을 쓴다 — 그래야 실제 collector가 나중에 쓰기 시작해도 같은
위치를 가리킨다. `ml_common`은 `collector`를 import하지 않는다(서로 다른
인스턴스에 독립 배포되는 모듈이라 의존 관계를 만들면 안 됨) — 같은 규칙을
독립적으로 복제해서 쓴다.
"""

from __future__ import annotations

import pandas as pd

STATION_MASTER_KEY = "silver/station/station_master.parquet"

# 실제 예시 데이터에 station 마스터 샘플이 없어 아직 검증 못 했다 — 지금은
# dev/seed_s3_from_local.py 기준을 그대로 둔다(docs/collector/ml-integration-requests.md
# 확인 요청 참고).
STATION_COLUMN_MAP = {
    "sta_id": "station_id",
    "sta_no": "station_no",
    "sta_nm": "station_name",
    "hold_cnt": "capacity",
    "lat": "lat",
    "lon": "lon",
    "grid_id": "grid_id",
}

# ml/data/silver/bike_station_realtime/ 예시 데이터로 확인 — dev 시딩 스크립트
# 기준과 완전히 일치한다(변경 없음). 5분 tick.
BIKE_REALTIME_COLUMN_MAP = {
    "stationId": "station_id",
    "stationName": "station_name",
    "rackTotCnt": "capacity",
    "parkingBikeTotCnt": "bike_count",
    "stationLatitude": "lat",
    "stationLongitude": "lon",
}

# ml/data/silver/bike_rental_history/ 예시 데이터 기준(대여이력 실제 source_id는
# "rental"이 아니라 "bike_rental_history", 컬럼도 대문자 스네이크케이스). 실제
# 예시 파일이 dt=.../hh=14/1445,1450,1455.parquet처럼 5분 간격으로 쌓여 있어
# implementation-plan.md의 계획(5분)과 일치 — dev 시딩 스크립트의 1시간 가정이
# 틀렸었다. RENT_STATION_ID/RETURN_STATION_ID는 이미 "ST-2565"처럼 station_id와
# 동일한 형식(5자리 raw 숫자가 아님)이라, station_no 크로스워크
# (`normalize_station_no()`) 없이 station_id로 직접 매칭한다.
RENTAL_COLUMN_MAP = {
    "RENT_DT": "start_dt",
    "RTN_DT": "end_dt",
    "RENT_STATION_ID": "start_st",
    "RETURN_STATION_ID": "end_st",
    "USE_MIN": "duration_min",
    "USE_DST": "distance_m",
    "BIKE_ID": "bike_id",
}

# ml/data/silver/weather_ultra_short_term/ 예시 데이터 기준(기상청 초단기실황,
# 10분 간격) — 우리가 가정했던 "weather_forecast" 하나가 아니라 실제로는 소스가
# 2개로 나뉘어 있다: 이 소스(관측치, 강수량 mm 있음)와
# weather_short_term_forecast(예보, 3시간 간격, 강수량 대신 강수확률%만 있어
# precip과 단위가 안 맞음 — 지금은 안 씀, ml-integration-requests.md 참고).
# `_get_recent_weather()`가 이미 "가장 최근 관측값"을 찾는 용도라 이 소스가
# 의미상으로도 더 맞는다.
WEATHER_COLUMN_MAP = {
    "T1H": "temp",
    "REH": "humidity",
    "WSD": "wind",
    "RN1": "precip",
}

# ml/data/silver/living_population_grid/ 예시 데이터 기준 — 우리가 가정했던
# "living_population_per_population_grid"(pop_resd/pop_long_foreign/
# pop_short_foreign 구분)와 소스 이름도, 컬럼 구성도 다르다. 실제로는
# 나이대(10살 단위)x성별(M/F) 인구만 제공하고 내국인/장단기체류외국인 구분
# 자체가 없다 — `_get_recent_population()`에서 SPOP(총 생활인구)만 취하고
# 나머지 breakdown은 근사치로 채운다(자세한 내용은 그 함수 docstring 참고).
POPULATION_COLUMN_MAP = {
    "CELL_ID": "grid_id",
    "SPOP": "pop_total",
}

BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
RENTAL_SOURCE_ID = "bike_rental_history"
WEATHER_SOURCE_ID = "weather_ultra_short_term"
POPULATION_SOURCE_ID = "living_population_grid"

BIKE_REALTIME_TICK_MINUTES = 5
RENTAL_TICK_MINUTES = 5
WEATHER_TICK_MINUTES = 10


def silver_key(source_id: str, window_start: pd.Timestamp) -> str:
    """collector/storage.py의 `_layer_key("silver", ...)`와 동일한 키 규칙."""
    return f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{window_start:%H%M}.parquet"


def _tick_keys(source_id: str, anchor_ts: pd.Timestamp, lookback_hours: float, tick_minutes: int) -> list[str]:
    """anchor_ts부터 과거 lookback_hours시간 동안, tick_minutes 간격의 키를 전부 만든다
    (anchor_ts를 그 간격으로 내림한 시각 포함).

    args:
        source_id: 조회할 Silver 소스 이름
        anchor_ts: 조회 기준 시각
        lookback_hours: 몇 시간 전까지 볼지
        tick_minutes: 파일이 쌓이는 간격(분)
    returns:
        오래된 것부터 최신 순으로 정렬된 키 목록
    """
    anchor_ts = anchor_ts.floor(f"{tick_minutes}min")
    n_ticks = round(lookback_hours * 60 / tick_minutes) + 1
    ticks = [anchor_ts - pd.Timedelta(minutes=tick_minutes * i) for i in range(n_ticks - 1, -1, -1)]
    return [silver_key(source_id, t) for t in ticks]


def bike_realtime_tick_keys(anchor_ts: pd.Timestamp, lookback_hours: float = 1.0) -> list[str]:
    """`bike_station_realtime`의 5분 tick 키 목록(오래된 것부터 최신 순)."""
    return _tick_keys(BIKE_REALTIME_SOURCE_ID, anchor_ts, lookback_hours, BIKE_REALTIME_TICK_MINUTES)


def rental_tick_keys(anchor_ts: pd.Timestamp, lookback_hours: float) -> list[str]:
    """`bike_rental_history`의 5분 tick 키 목록(오래된 것부터 최신 순)."""
    return _tick_keys(RENTAL_SOURCE_ID, anchor_ts, lookback_hours, RENTAL_TICK_MINUTES)


def weather_tick_keys(anchor_ts: pd.Timestamp, lookback_hours: float = 3.0) -> list[str]:
    """`weather_ultra_short_term`의 10분 tick 키 목록(오래된 것부터 최신 순)."""
    return _tick_keys(WEATHER_SOURCE_ID, anchor_ts, lookback_hours, WEATHER_TICK_MINUTES)


def population_daily_prefix(day: pd.Timestamp) -> str:
    """`living_population_grid`는 하루 1개 파일(YMD/TT 컬럼으로 그날 24시간을 전부
    담음)만 쌓인다 — collector job이 실제로 언제 도는지(파일명의 hh=/HHMM)는 알 수
    없어 정확한 키를 만들 수 없다. 그래서 그 날짜의 dt=.../ prefix를 통째로 LIST해서
    실제로 존재하는 파일을 찾는다(`_get_recent_population()` 참고).
    """
    return f"silver/{POPULATION_SOURCE_ID}/dt={day:%Y-%m-%d}/"


def predictions_key(window_start: pd.Timestamp) -> str:
    """추론 결과 저장 키 — Silver와 같은 dt=/hh= 파티션 규칙, 최상위 prefix만 다르다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def predictions_failed_key(window_start: pd.Timestamp) -> str:
    """부분실패 station 목록 저장 키 — `predictions_key()`와 같은 시각, 파일명만 다르다."""
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}_failed.json"


def single_prediction_key(station_id: str, window_start: pd.Timestamp) -> str:
    """단일 정류소 추론 결과 저장 S3 키를 생성한다.

    args:
        station_id: 정류소 ID (예: "ST-2000")
        window_start: 추론 기준 시각
    returns:
        S3 객체 키 문자열 (예: "predictions/single/dt=2026-08-15/hh=17/ST-2000_1700.parquet")
    """
    return f"predictions/single/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{station_id}_{window_start:%H%M}.parquet"

