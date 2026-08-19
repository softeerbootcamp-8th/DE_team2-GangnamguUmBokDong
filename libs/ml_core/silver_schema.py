"""Collector Silver 계층 스키마 — 컬럼명 매핑 + S3 키 빌더.

기준 스키마는 collector 팀이 실제로 수집해 넘겨준 예시 데이터(`ml/data/silver/*`,
2026-08-15 기준)다. `dev/seed_s3_from_local.py`(로컬 MinIO 시딩 스크립트)와
`docs/collector/DataSchema.md`/`implementation-plan.md`(계획 문서)는 이 실제
예시와 소스 이름·컬럼명·수집 주기가 상당히 달랐다 — 그 차이는
`docs/collector/ml-integration-requests.md`에 정리해뒀고, 이 파일은 실제
예시 데이터를 그대로 반영한다(계획 문서나 시딩 스크립트가 아니라).

키 생성 규칙(`silver_key()`)은 `collector/storage.py`의 `_layer_key()`와 정확히
같은 문자열 포맷을 쓴다 — 그래야 실제 collector가 나중에 쓰기 시작해도 같은
위치를 가리킨다. `ml_core`은 `collector`를 import하지 않는다(서로 다른
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

# ml/data/silver/weather_ultra_short_live/ 예시 데이터 기준(기상청 초단기실황,
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

# **2026-08 확정**: `loader/transform.py`의 `weather_forecast_from_silver()`가
# 이 소스를 이미 실제로 소비하고 있어(collector 예보 수집 자체는 이 브랜치엔
# 아직 없지만, loader 쪽에서 실제 raw 응답 기준으로 이미 구현/검증됨) 그 코드를
# 그대로 근거로 쓴다 — 앞서(ml-integration-requests.md #6) "PCP는 숫자로 정규화돼
# 내려온다"고 가정했던 건 틀렸다: PCP는 여전히 raw 텍스트("강수없음"/"1.0mm 미만"/
# "30.0~50.0mm"/순수 숫자)로 온다(`parse_kma_precip_text()`가 loader의
# `_parse_precip_str()`과 동일한 정책으로 파싱). 타겟 시각도 단일 컬럼이 아니라
# `fcstDate`(YYYYMMDD)+`fcstTime`(HHMM, KST) 두 컬럼을 합쳐야 한다(발표 시각은
# `baseDate`/`baseTime`). nx/ny(격자)는 무시한다 — `weather_ultra_short_live`
# 관측 소스도 지금 격자 구분 없이 "서울 전체 공유" 1개 값으로 취급하므로
# (`WEATHER_SOURCE_ID` 관련 함수들 참고) 예보만 격자를 따로 가리는 건 일관성이
# 없다(추후 격자별로 세분화하려면 관측 쪽도 같이 손봐야 함).
WEATHER_FORECAST_SOURCE_ID = "weather_short_term_forecast"
WEATHER_FORECAST_COLUMN_MAP = {"TMP": "temp"}  # PCP는 parse_kma_precip_text()로 별도 처리(단순 rename 불가)
WEATHER_FORECAST_DATE_COLUMN = "fcstDate"
WEATHER_FORECAST_TIME_COLUMN = "fcstTime"
# 실제 발표 주기/스케줄은 아직 확정되지 않았다(자동 수집 DAG 자체가 아직 없음,
# ml-integration-requests.md #11) — 다른 소스와 같은 3시간 격자를 잠정 가정한다.
WEATHER_FORECAST_ISSUE_TICK_MINUTES = 180


def parse_kma_precip_text(value) -> float | None:
    """기상청 강수량 raw 값("강수없음"/"1.0mm 미만"/"30.0~50.0mm"/순수 숫자)을 float(mm)로 변환한다.

    `loader/transform.py`의 `_parse_precip_str()`과 정확히 같은 정책이다 — 두
    인스턴스가 서로 import하지 않는다는 원칙(이 파일 모듈 docstring 참고)이라
    독립적으로 복제한다. 범위 표현("30.0~50.0mm")은 하한값을 쓴다(둘 다 동일).
    """
    if value is None or value == "":
        return None
    text = str(value).strip()
    if text in ("강수없음", "적설없음"):
        return 0.0
    if "미만" in text:
        return 0.5
    text = text.replace("mm", "").strip()
    if "~" in text:
        text = text.split("~")[0]
    try:
        return float(text)
    except (TypeError, ValueError):
        return None

BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
RENTAL_SOURCE_ID = "bike_rental_history"
WEATHER_SOURCE_ID = "weather_ultra_short_live"
POPULATION_SOURCE_ID = "living_population_grid"
# `normalizer`(舊 seoul-pop-normalizer)가 5분마다 만드는, 실시간 도시데이터(population_realtime)로
# 보정한 생활인구 — 물리 스키마는 POPULATION_SOURCE_ID와 동일(CELL_ID/SPOP/H_DNG_CD/
# 나이대x성별)하지만 YMD/TT 컬럼이 없다(시각이 이미 S3 키 경로에 있음 — silver_key()와
# 동일한 dt=/hh=/HHMM 규칙). 서빙(inference)의 실시간 인구 조회 전용 — 학습/평가는
# 여전히 POPULATION_SOURCE_ID(원본)를 그대로 쓴다(feature_engine/spark/silver_source.py
# 참고, 정답 라벨은 사후 보정 없는 실측 그대로여야 하므로).
POPULATION_NORMALIZED_SOURCE_ID = "living_population_normalized"

BIKE_REALTIME_TICK_MINUTES = 5
RENTAL_TICK_MINUTES = 5
WEATHER_TICK_MINUTES = 10
POPULATION_NORMALIZED_TICK_MINUTES = 5


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
    """`weather_ultra_short_live`의 10분 tick 키 목록(오래된 것부터 최신 순)."""
    return _tick_keys(WEATHER_SOURCE_ID, anchor_ts, lookback_hours, WEATHER_TICK_MINUTES)


def weather_forecast_issue_keys(anchor_ts: pd.Timestamp, lookback_hours: float = 24.0) -> list[str]:
    """`weather_short_term_forecast`의 발표(issue) 파일 키 목록(오래된 것부터 최신 순).

    다른 소스와 달리 이 파일 하나엔 미래 여러 시각의 예보가 여러 행으로 들어있다
    (관측 소스처럼 "그 tick의 값 1개"가 아님) — 그래서 이 키들은 "그 시각의
    예보값"이 아니라 "그 시각에 발표된 예보 파일 전체"를 가리킨다. 호출부가 가장
    최근 발표 파일부터 훑으며 그 안에서 원하는 미래 시각(target_ts)에 가장 가까운
    행을 골라 쓴다. lookback을 기본 24시간으로 넉넉히 잡은 이유는 실제 발표 주기가
    3시간 격자에 정확히 맞아떨어지는지 아직 확정되지 않았기 때문(자동 수집 스케줄
    자체가 아직 없음, `WEATHER_FORECAST_ISSUE_TICK_MINUTES` 주석 참고) — 최소 한
    번은 걸리게 넉넉히 본다.
    """
    return _tick_keys(WEATHER_FORECAST_SOURCE_ID, anchor_ts, lookback_hours, WEATHER_FORECAST_ISSUE_TICK_MINUTES)


def population_normalized_tick_keys(anchor_ts: pd.Timestamp, lookback_hours: float = 1.0) -> list[str]:
    """`living_population_normalized`의 5분 tick 키 목록(오래된 것부터 최신 순)."""
    return _tick_keys(POPULATION_NORMALIZED_SOURCE_ID, anchor_ts, lookback_hours, POPULATION_NORMALIZED_TICK_MINUTES)


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

