"""단일 시점 입력 기반 수요 예측 모듈.

정류소ID + 날짜/시각 + 날씨를 넣으면 그 시점의 대여/반납 수요(점추정 +
P10/P50/P90)를 반환한다. 생활인구(`population`)는 있으면 넣고, 없으면
(인구 데이터 피드가 끊긴 경우 등) 생략해도 자동으로 대체된다.

    from inference.predict_single import predict_rental_demand
    predict_rental_demand(
        station_id="ST-2000", date="2025-06-01", hour=8,
        temp=22.5, precip=0.0,
        population=3200,   # 없으면 생략 가능 — 격자 평소 패턴으로 대체됨
    )

설계 메모 — 실제 데이터 수집(collector가 Silver로 쌓는 것)은 이 모듈의 책임이
아니지만, temp/precip/population/stockout을 생략하면 이 모듈이 직접 Silver
(S3/MinIO)에서 그 시점 근처의 최근 값을 읽어온다(`_get_recent_weather()`/
`_get_recent_bike_status()`/`_get_recent_population()`) — 학습(1년치 전체를 Spark로
집계)과 달리 "지금 이 순간"만 필요하므로 조회 범위가 훨씬 좁다. 값을 직접 주면
(테스트 등) 그 조회를 건너뛰고 준 값을 그대로 쓴다(하위 호환).

**학습과 날씨를 다루는 방식이 다르다**: 학습은 target_ts(예측 대상 시점)에 실제로
관측된 날씨(ground truth)로 배운다 — 그 시점이 이미 지난 과거라 실측이 있기
때문(`feature_engine/spark/build_multi_horizon_features.py`). 반면 추론 시점엔
target_ts가 미래일 수 있어(horizon>1) 실측이 없다 — `_resolve_live_weather()`가
target_ts와 anchor_ts(T0, "지금")를 비교해, 미래면 예보(`_get_forecast_weather()`,
`weather_short_term_forecast`)를 먼저 쓰고 그렇지 않으면(또는 예보가 없으면)
관측(`_get_recent_weather()`, `weather_ultra_short_live`)을 쓴다. **주의(2026-08)**:
collector 자체의 예보 자동 수집 스케줄은 이 저장소에 아직 없다(수동 트리거만
가능, `docs/collector/ml-integration-requests.md` #11) — 그래서 예보 소스가
실제로 채워져 있지 않으면 이 경로는 조용히 관측 fallback으로 넘어간다. 다만
raw 스키마 자체(`fcstDate`/`fcstTime`/`TMP`/`PCP`)는 `loader/transform.py`의
`weather_forecast_from_silver()`가 이미 같은 소스를 실제로 소비하고 있어 그
코드를 근거로 확인된 값이다(가정이 아님).

모델이 쓰는 lag feature는 대여/반납 각 1개(`rental_lag_1h`/`return_lag_1h`)뿐이고,
그걸 계산하려면 "최근 실적 히스토리"가 필요하다. 히스토리 소스는 두 개로 나뉜다:
(1) `_get_history_by_station()` — Silver `rental`을 시간 단위로 집계한 것,
지연 관측 문제가 없는 반납(return_lag_1h)에 쓴다. (2)
`_get_rental_events_by_station()`(`_get_raw_rental_trips()` 경유로 Silver
`rental`을 트립 단위 원본 그대로 fetch) — 대여(rental_lag_1h)에 쓴다 — 대여는
반납이 완료돼야 로그에 잡히는 지연 관측 문제(REALTIME_FEATURES.md)가 있어서,
시간 단위 집계만으로는 그 시점에 실제로 관측 가능했던 값을 재현할 수 없기
때문이다(`rolling_window_features.count_visible_in_window()`로 계산).

**실시간 데이터가 끊기거나 지연될 때의 동작 (fallback)**: lag 계산에 필요한
특정 시각의 실적이 히스토리에 없으면, 그 값을 무작정 NaN으로 두지 않고
`station_hourly_profile.parquet`(정류소×시간×요일×**월**별 2025년 평균/표준편차,
`build_station_profile.py`로 생성)에서 "그 정류소가 이 달 이 요일 이 시간에
보통 어느 정도였는지"로 대체한다. 월을 반드시 포함하는 이유는 계절에 따라
대여량 자체가 크게 달라지기 때문 — 1월과 6월을 구분 안 하면 겨울 결측을
여름 수준으로(또는 그 반대로) 채우는 오류가 생긴다. 재귀적으로 예측값을 다시 입력에 먹이는 방식이
아니라 각 lag를 독립적으로 그 정류소의 평소 패턴으로 메우는 방식이라, 여러 시간
앞을 예측해도 오차가 누적되지 않는다. 다만 이 fallback은 "평소 패턴"일 뿐 그날의
특수성(예: 그날만 갑자기 몰린 상황)은 반영하지 못하므로, 실시간 데이터가
있을 때보다는 정확도가 낮다. 반환값의 `lag_fallback_used`/`lag_data_freshness`로
이번 예측이 실시간 데이터를 얼마나 썼는지 확인할 수 있다.

**인구 데이터도 같은 방식으로 대비된다**: `population`을 안 주면 먼저
`living_population_normalized`(`normalizer`가 실시간 도시데이터로 보정한
생활인구, `_get_recent_population()` 참고 — 학습/평가는 원본
`living_population_grid`를 그대로 쓴다)에서 실시간 조회를 시도하고, 그마저
없으면 `population_hourly_profile.parquet`(격자×시간×요일별 2025년 평균,
`build_population_profile.py`로 생성, **원본 기준**)에서 그 정류소가 속한
격자의 평소 인구로 대체한다. 인구 프로필은 **month을 키에 넣지 않는다** — 실측 기준
생활인구는 월별로는 거의 안 변하고(최대/최소 1.05배) 시간대별로만 크게
변해서(1.42배, 출퇴근 패턴), station 프로필처럼 월을 나누면 표본만 줄고
얻는 게 적다. 대체 여부는 반환값의 `population_source`(`"provided"` 또는
`"fallback"`)로 확인할 수 있다.

한계: 정류소/격자 자체가 2025년에 데이터가 없었거나(신규 정류소 등) 프로필도
없는 경우엔 fallback도 NaN이 된다. LightGBM은 결측을 네이티브로 처리하므로
예측은 나오지만 정확도는 더 떨어진다.

**재고(stockout)도 값 자체는 "품절 아님"으로 조용히 대체하지만, 대체 여부는
반드시 확인 가능해야 한다**: `stockout`을 안 주면 Silver `bike_station_realtime`
실시간 조회를 시도하고, 그 station의 데이터 자체가 없으면 `False`(품절 아님)로
기본값을 쓴다 — 이 기본값이 `rental_exposure`를 1.0(정상)으로 만들어 실제로는
품절이었을 수도 있는 시간대의 대여 수요를 과대평가할 수 있다. population과 똑같이
반환값의 `stockout_source`(`"provided"` 또는 `"fallback"`)로 그 여부를 확인할 것 —
`_stockout_from_status()` 참고.
"""

import gc
import sys
from collections.abc import Sequence

import numpy as np
import pandas as pd
from core import s3 as s3_io
from ml_core import scoring as scoring_io
from ml_core import silver_schema
from ml_core.day_index import day_index
from ml_core.holidays_kr import korean_holidays
from ml_core.minute_of_day import minute_of_day
from ml_core.model_contract import (
    RENTAL_EXPOSURE_DTYPE,
    RENTAL_FEATURE_COLUMN_DTYPES,
    RENTAL_FEATURE_COLUMNS,
    RETURN_FEATURE_COLUMN_DTYPES,
    RETURN_FEATURE_COLUMNS,
    load_station_dtype,
)
from ml_core.scoring import predict

from . import config

# 대여/반납 record를 하나의 dict로 같이 조립해두고 predict()가 model_name에 맞는
# 컬럼만 골라 쓰게 한다(ml_core.scoring.predict() 참고) — 그래서 dtype 캐스팅도
# 두 모델 컬럼의 합집합 하나로 한 번에 한다(겹치는 공통 컬럼의 dtype 값은 동일하므로
# merge 시 충돌 없음).
_ALL_FEATURE_COLUMNS = sorted(set(RENTAL_FEATURE_COLUMNS) | set(RETURN_FEATURE_COLUMNS))
_COMBINED_FEATURE_COLUMN_DTYPES = {**RENTAL_FEATURE_COLUMN_DTYPES, **RETURN_FEATURE_COLUMN_DTYPES}
_MAX_FORECAST_DISTANCE = pd.Timedelta(minutes=35)

# return_lag_1h(정확히 1시간 전 1개 값)와 rental_lag_1h의 censored window
# ([T-100분, T-40분))를 둘 다 커버하는 여유 — Silver rental을 raw로 fetch할 때
# 이만큼만 과거로 가면 충분하다.
_RENTAL_LOOKBACK_HOURS = 3

_history_by_station: dict[str, pd.DataFrame] | None = None
_rental_events_by_station: dict[str, pd.DataFrame] | None = None
_rental_events_sorted_by_station: dict[str, tuple] = {}  # station_id -> (start_dt 정렬된 numpy 배열, 그 순서로 정렬된 end_dt 배열) — _rental_visible_at() 캐시
_all_rental_events_sorted: tuple | None = None  # (station_id 배열, start_dt로 정렬된 배열, 같은 순서의 end_dt 배열) — 전체 정류소 통합, _rental_visible_batch_all_stations() 캐시
_rental_events_coverage: tuple[pd.Timestamp, pd.Timestamp] | None = None
_STATION_PROFILE_STAT_COLS = ("rental_mean", "rental_std", "return_mean", "return_std")
_STATION_PROFILE_STAT_INDEX = {name: i for i, name in enumerate(_STATION_PROFILE_STAT_COLS)}
# station_no -> station 축 인덱스, 그리고 (station, model minute//tick, dow,
# month-1, stat) dense 배열 — dict[tuple, dict[str,float]] 대신 쓰는 이유는
# _get_station_profile() 참고.
_station_profile_station_index: dict[int, int] | None = None
_station_profile_values: np.ndarray | None = None
_population_profile: dict[tuple[str, int, int], dict[str, float]] | None = None
_station_master: pd.DataFrame | None = None
_holidays_by_year: dict[int, set[str]] = {}
_raw_rental_trips: pd.DataFrame | None = None  # station_id/end_station_id 매칭까지 끝난 원본 트립
_recent_population_by_ts: dict[pd.Timestamp, pd.DataFrame] = {}  # _get_recent_population() 캐시 — target_ts별로 한 번만 S3에서 읽는다

# 이 모듈의 실시간 캐시는 "프로세스 생애주기 동안 딱 한 번"만 채워진다(기존과 동일한
# 철학) — 실제 서빙은 5분마다 새 프로세스(cron/배치 실행)로 도는 걸 전제라, 프로세스
# 하나가 "지금 이 순간"만 다루면 충분하고 anchor_ts가 바뀌었는지 감시할 필요가 없다.
# 같은 anchor_ts로 정류소×horizon을 아무리 많이 반복 호출해도(predict_demand_multi_hour_all_stations
# 등) 최초 1번만 S3에서 읽는다.

N_LAG_ROLLING_FEATURES = 2  # rental_lag_1h + return_lag_1h


def _fetch_recent_rental_trips(anchor_ts: pd.Timestamp, lookback_hours: int) -> pd.DataFrame:
    """anchor_ts 기준 최근 lookback_hours시간 동안의 Silver `bike_rental_history` 트립 원본을 읽어온다.

    `silver_schema.rental_tick_keys()`로 필요한 5분 tick 키를 결정적으로 만들고
    (LIST 없이), `s3_io.read_parquet_many()`로 병렬 조회한 뒤 존재하는 파일만
    이어붙인다 — 아직 안 쌓였거나 결측인 tick은 자연히 빠진다. 실제 수집 주기가
    5분이라(예시 데이터로 확인, `docs/collector/ml-integration-requests.md`) lag_168h
    기준 lookback 하나에 키가 2천 개 가까이 되므로 순차 조회는 병목이 된다.
    컬럼명만 `RENTAL_COLUMN_MAP`으로 바꾸고, station_id 매칭(`start_st`/`end_st` ->
    station_id)은 아직 안 한다(`_resolve_rental_stations()`가 담당).

    실제로 collector의 각 5분 tick 파일이 "그 5분 동안 새로 생긴 트립"만 담는
    델타(incremental)인지, 아니면 그날 지금까지의 트립을 매번 통째로 다시 담는
    누적(cumulative) 스냅샷인지는 실제 예시 데이터만으로는 확정할 수 없었다
    (`docs/collector/ml-integration-requests.md` 10번 참고) — 누적이면 여러 tick을
    이어붙일 때 같은 트립이 몇 번이고 중복된다. 어느 쪽이든 안전하도록
    `(bike_id, start_dt)`로 중복을 제거한다(같은 자전거가 같은 순간에 두 트립을
    동시에 시작할 수 없으므로 실제 트립 하나를 안정적으로 식별하는 키).

    args:
        anchor_ts: 조회 기준 시각("지금")
        lookback_hours: 몇 시간 전까지 읽을지
    returns:
        pd.DataFrame: start_dt, end_dt, start_st, end_st (이미 station_id와 같은
            "ST-"형식 — station_no 크로스워크 불필요, `_resolve_rental_stations()` 참고)
    """
    keys = silver_schema.rental_tick_keys(anchor_ts, lookback_hours)
    raws = s3_io.read_parquet_many(keys)
    frames = [raw.rename(columns=silver_schema.RENTAL_COLUMN_MAP) for raw in raws if raw is not None and not raw.empty]
    if not frames:
        return pd.DataFrame(columns=["start_dt", "end_dt", "start_st", "end_st"])
    trips = pd.concat(frames, ignore_index=True)
    trips["start_dt"] = pd.to_datetime(trips["start_dt"])
    trips["end_dt"] = pd.to_datetime(trips["end_dt"])
    if "bike_id" in trips.columns:
        trips = trips.drop_duplicates(subset=["bike_id", "start_dt"], ignore_index=True)
    return trips


def _resolve_rental_stations(trips: pd.DataFrame) -> pd.DataFrame:
    """트립의 start_st/end_st(대여소 ID)를 station_master 기준으로 걸러 station_id로 확정한다.

    실제 예시 데이터(`ml/data/silver/bike_rental_history/`) 확인 결과 `RENT_STATION_ID`/
    `RETURN_STATION_ID`가 이미 `"ST-2565"`처럼 station_id와 동일한 형식이라(raw 숫자
    대여소번호가 아님), `normalize_station_no()` + station_no 크로스워크가 필요 없다 —
    station_master에 실제로 존재하는 station_id인지만 확인한다. 대여소가 이상값이거나
    station_master에 없는 트립(폐쇄 대여소 등)은 대여 쪽(`station_id`)이면 행 자체를
    제외하고, 반납 쪽(`end_station_id`)이면 NaN으로 남겨 반납 집계에서만 빠지게 한다
    (`feature_engine/spark/build_targets.py`의 배치 경로와 같은 원칙 — raw 숫자
    대여이력 CSV를 읽는 그쪽은 `_normalize_station_no()`를 그대로 쓴다, 서로 다른 원본
    포맷이라 별개로 둔다).

    args:
        trips: _fetch_recent_rental_trips()의 결과
    returns:
        pd.DataFrame: 원본 컬럼 + station_id(대여 정류소), end_station_id(반납 정류소)
    """
    if trips.empty:
        return trips.assign(station_id=pd.Series(dtype=str), end_station_id=pd.Series(dtype=str))

    known_ids = _get_station_master().index
    trips = trips[trips["start_st"].isin(known_ids)].copy()
    trips["station_id"] = trips["start_st"]
    trips["end_station_id"] = trips["end_st"].where(trips["end_st"].isin(known_ids))
    return trips


def _get_raw_rental_trips(anchor_ts: pd.Timestamp) -> pd.DataFrame:
    """station_id/end_station_id 매칭까지 끝난 원본 트립을 프로세스당 한 번만 읽어 캐시한다.

    `_get_history_by_station()`과 `_get_rental_events_by_station()`이 공유한다 —
    둘 다 `_RENTAL_LOOKBACK_HOURS`(return_lag_1h/rental_lag_1h 계산에 필요한 만큼의
    여유)면 충분해서 한 번만 읽으면 된다. 이 모듈의 다른 캐시와
    마찬가지로 anchor_ts가 바뀌어도 재조회하지 않는다(프로세스 하나 = "지금 이 순간"
    하나를 다루는 짧은 배치 실행을 전제 — 모듈 docstring 참고).
    """
    global _raw_rental_trips
    if _raw_rental_trips is None:
        raw = _fetch_recent_rental_trips(anchor_ts, _RENTAL_LOOKBACK_HOURS)
        _raw_rental_trips = _resolve_rental_stations(raw)
    return _raw_rental_trips


def _get_history_by_station(anchor_ts: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """station_id -> 정확히 `anchor_ts - 1시간` 시점의 return_count 값 1개만 담은 DataFrame.

    원본 트립(`_get_raw_rental_trips()`)에서 직접 집계한다 —
    `feature_engine.build_targets.future_rolling_counts()`와 같은 정의
    ("[t, t+TARGET_HORIZON_MINUTES분) 동안 종료된 트립 수")를 end_station_id/end_dt
    기준으로 계산한다. **반납(return_lag_1h)만 이 경로로 계산한다** — 대여
    (rental_lag_1h)는 지연 관측 문제 때문에 트립 단위 point-in-time censored 계산
    (`_rental_visible_at()`)을 따로 쓴다(모듈 docstring 참고). lag가 lag_1h
    하나뿐이라 예전처럼 여러 시각(lag_24h/168h, rolling 창)을 미리 계산해둘 필요가
    없다 — 딱 필요한 시각 하나만 계산한다.

    returns:
        dict[str, pd.DataFrame]: station_id별로 [anchor_ts-1시간] 하나만 인덱스로
            갖는 1행짜리 return_count 테이블 (모듈 전역에 캐시)
    """
    global _history_by_station
    if _history_by_station is not None:
        return _history_by_station

    trips = _get_raw_rental_trips(anchor_ts)
    _history_by_station = {}
    if trips.empty:
        return _history_by_station

    point = anchor_ts - pd.Timedelta(hours=1)
    window_end = point + pd.Timedelta(minutes=config.TARGET_HORIZON_MINUTES)

    returned = trips.dropna(subset=["end_station_id"])
    for sid, g in returned.groupby("end_station_id", sort=False):
        end_dts = np.sort(g["end_dt"].to_numpy())
        lo = np.searchsorted(end_dts, np.datetime64(point), side="left")
        hi = np.searchsorted(end_dts, np.datetime64(window_end), side="left")
        _history_by_station[sid] = pd.DataFrame({"return_count": [float(hi - lo)]}, index=[point])

    return _history_by_station


def _get_rental_events_by_station(anchor_ts: pd.Timestamp) -> dict[str, pd.DataFrame]:
    """station_id -> 트립 단위 대여 이벤트(station_id, start_dt, end_dt), 최근 lookback 구간.

    rolling_window_features.count_visible_in_window()에 그대로 넘길 수 있는 형태다.
    `_get_raw_rental_trips()`를 그룹핑만 해서 재사용한다 — `_get_history_by_station()`과
    별개의 대여소 매칭(대여 쪽 station_id)만 쓴다.

    returns:
        dict[str, pd.DataFrame]: station_id별 (station_id, start_dt, end_dt) 이벤트
            (모듈 전역에 캐시). 커버리지 범위(최소/최대 start_dt)는
            _rental_events_coverage 전역에 함께 캐시된다.
    """
    global _rental_events_by_station, _rental_events_coverage, _all_rental_events_sorted
    if _rental_events_by_station is None:
        raw = _get_raw_rental_trips(anchor_ts)
        trips = raw.dropna(subset=["station_id"])[["station_id", "start_dt", "end_dt"]].reset_index(drop=True)
        if trips.empty:
            _rental_events_coverage = None
            _rental_events_by_station = {}
            _all_rental_events_sorted = (np.array([]), np.array([], dtype="datetime64[ns]"), np.array([], dtype="datetime64[ns]"))
            return _rental_events_by_station

        _rental_events_coverage = (trips["start_dt"].min(), trips["start_dt"].max())
        _rental_events_by_station = {
            sid: g.reset_index(drop=True) for sid, g in trips.groupby("station_id", sort=False)
        }
        # 전체 정류소를 한 번에 다루는 배치 경로(_rental_visible_batch_all_stations())용 —
        # 트립을 station 무관하게 start_dt 기준으로 한 번만 정렬해둔다. 같은 load를
        # 재사용해서(위에서 이미 읽은 trips) 다시 읽지 않는다.
        order = trips["start_dt"].to_numpy().argsort(kind="mergesort")
        _all_rental_events_sorted = (
            trips["station_id"].to_numpy()[order],
            trips["start_dt"].to_numpy()[order],
            trips["end_dt"].to_numpy()[order],
        )
    return _rental_events_by_station


def _rental_visible_at(station_id: str, anchors: list[pd.Timestamp]) -> pd.Series:
    """anchor 시각들에 대해 point-in-time censored 대여 카운트를 구한다 (트립 단위 계산).

    각 anchor가 요구하는 윈도우(`[anchor-100분, anchor-40분)`, `config.ROLLING_EMBARGO_MINUTES`/
    `ROLLING_WINDOW_MINUTES` 기준)가 로드된 트립 데이터의 커버리지(2025년 전체) 밖이면
    NaN(데이터 없음 — 호출부가 fallback으로 판단), 커버리지 안이면 실제 카운트(트립
    0건도 유효한 관측값이라 NaN이 아님)를 채운다.

    **왜 `count_visible_in_window()`를 anchor마다 그대로 안 부르는가**: 그 함수는
    "소량의 최근 이벤트 버퍼"를 전제로 설계돼 있어서, anchor 하나당 그 station의
    (여기서는 연간 전체) 트립을 매번 다시 boolean mask로 스캔한다 — 예전에는 지금은
    없어진 `roll_mean_24h` 하나에만 anchor 수백 개가 필요했고, 지금도
    `predict_demand_multi_hour_all_stations()`가 anchor 축으로 여러 station을 한 번에
    묶어 계산해서 anchor마다 전체 재스캔은 여전히 비쌌다(실측: 정류소 1개×12시간
    재귀 예측에 약 2초 —
    전체 정류소로는 감당 불가). 대신 그 station의 트립을 `start_dt` 기준으로 한
    번만 정렬해두고, anchor마다 `np.searchsorted`로 윈도우 경계 인덱스만 찾은 뒤
    그 좁은 구간에서만 `end_dt<=anchor`를 확인한다 — 결과는
    `count_visible_in_window()`(경계 포함/제외 규칙까지) 그대로이고 반복 전체
    스캔만 없앤 것이다(무작위 anchor 450개로 기존 구현과 값이 완전히 같음을 확인).

    args:
        station_id: 정류소 ID
        anchors: 조회할 기준 시각 목록
    returns:
        pd.Series: anchors를 인덱스로 하는 카운트(또는 NaN)
    """
    events = _get_rental_events_by_station(max(anchors)).get(station_id)
    coverage = _rental_events_coverage if events is not None else None

    if events is None or events.empty:
        return pd.Series({ts: np.nan for ts in anchors})

    # start_dt 정렬은 station당 한 번만 하면 되는데, 캐시가 없으면 이 함수를 부를
    # 때마다(재귀 스텝 하나에도 여러 번) station의 트립 전체를 다시 정렬하게 되어
    # (특히 트립이 많은 station일수록) 정렬 자체가 새로운 병목이 된다.
    sorted_arrays = _rental_events_sorted_by_station.get(station_id)
    if sorted_arrays is None:
        order = events["start_dt"].to_numpy().argsort(kind="mergesort")
        sorted_arrays = (events["start_dt"].to_numpy()[order], events["end_dt"].to_numpy()[order])
        _rental_events_sorted_by_station[station_id] = sorted_arrays
    starts_sorted, ends_sorted = sorted_arrays

    vals: dict[pd.Timestamp, float] = {}
    for ts in anchors:
        window_end = ts - pd.Timedelta(minutes=config.ROLLING_EMBARGO_MINUTES)
        window_start = window_end - pd.Timedelta(minutes=config.ROLLING_WINDOW_MINUTES)
        if coverage is None or window_end <= coverage[0] or window_start > coverage[1]:
            vals[ts] = np.nan  # 데이터 커버리지 밖 -> 결측(진짜 관측 불가)
            continue
        lo = np.searchsorted(starts_sorted, np.datetime64(window_start), side="left")
        hi = np.searchsorted(starts_sorted, np.datetime64(window_end), side="left")
        if lo == hi:
            vals[ts] = 0.0
            continue
        end_slice = ends_sorted[lo:hi]
        visible = ~pd.isna(end_slice) & (end_slice <= np.datetime64(ts))
        vals[ts] = float(visible.sum())
    return pd.Series(vals)


def _rental_visible_batch_all_stations(
    station_ids: list[str], anchors: list[pd.Timestamp]
) -> dict[pd.Timestamp, pd.Series]:
    """anchor마다 "정류소 전체"의 point-in-time censored 대여 카운트를 한 번에 계산한다.

    `_rental_visible_at(station_id, anchors)`는 정류소 1개를 고정하고 anchor
    여러 개를 처리한다 — `predict_demand_multi_hour_all_stations()`가 정류소
    2,582개를 순회하며 이 함수를 그만큼 반복 호출하면, anchor 자체는 몇 개
    안 되는데(최대 25개) "정류소 수만큼" 반복하는 게 병목이 됐다(history.md
    24번 항목). 이 함수는 **축을 뒤집어** anchor를 고정하고 정류소 전체를 한
    번에 처리한다 — "정류소가 몇 개든 anchor 개수만큼만 반복하면 된다"는
    구조로 바뀐다.

    같은 원리(전체 트립을 start_dt 기준 한 번만 정렬 + anchor마다
    `np.searchsorted`로 좁은 구간만 슬라이스)를 쓰지만, 그 슬라이스 안에서
    `_rental_visible_at()`처럼 한 정류소만 보는 게 아니라 `station_id`로
    묶어서(`value_counts()`) **모든 정류소의 카운트를 한 번에** 낸다 — 슬라이스
    자체가 좁아서(그 60분 창에 시작된 트립만) 정류소가 몇 개든 이 groupby
    비용은 거의 그대로다.

    args:
        station_ids: 결과에 포함할 정류소 목록(2025년에 트립이 단 한 건도 없던
            정류소는 `_rental_visible_at()`과 동일하게 NaN — 0건과 "관측 자체가
            없음"을 구분해야 해서 0으로 채우면 안 됨)
        anchors: 조회할 기준 시각 목록(정류소 전체가 공유하는 시각)
    returns:
        dict[pd.Timestamp, pd.Series]: anchor -> (station_id로 인덱싱된 카운트,
            커버리지 밖이면 NaN) — `_rental_visible_at()`과 같은 규약
    """
    events_by_station = _get_rental_events_by_station(max(anchors))  # _all_rental_events_sorted 캐시를 이 호출이 채워둠
    sids_arr, starts_sorted, ends_sorted = _all_rental_events_sorted
    coverage = _rental_events_coverage
    station_index = pd.Index(station_ids, name="station_id")
    # 2025년 트립이 단 한 건도 없는 정류소(예: station_master에는 있지만 활성
    # station이 아닌 395개)는 "그 anchor 근처에 트립이 없어서 0"이 아니라
    # "이 station 자체를 관측한 적이 없어서 모름"이다 — _rental_visible_at()도
    # 이 경우 NaN을 반환하므로 그대로 맞춘다.
    # **주의(실제로 겪은 성능 버그)**: `station_index.isin(sids_arr)`로 짰다가
    # 3,700만 건짜리 Arrow 문자열 배열 전체를 매 hour-step마다 다시 해시테이블로
    # 만드는 바람에 호출 1번에 94초가 걸렸다(history.md 24번 항목) — 이미
    # station별로 그룹화해서 캐시해둔 `_rental_events_by_station`의 키(dict라
    # O(1) 조회)를 재사용하는 것으로 바꿔서 해결했다.
    has_any_trip = np.array([sid in events_by_station for sid in station_ids])

    out: dict[pd.Timestamp, pd.Series] = {}
    for ts in anchors:
        window_end = ts - pd.Timedelta(minutes=config.ROLLING_EMBARGO_MINUTES)
        window_start = window_end - pd.Timedelta(minutes=config.ROLLING_WINDOW_MINUTES)
        if coverage is None or window_end <= coverage[0] or window_start > coverage[1]:
            out[ts] = pd.Series(np.nan, index=station_index)
            continue
        lo = np.searchsorted(starts_sorted, np.datetime64(window_start), side="left")
        hi = np.searchsorted(starts_sorted, np.datetime64(window_end), side="left")
        if lo == hi:
            counts = pd.Series(0.0, index=station_index)
        else:
            slice_sids = sids_arr[lo:hi]
            slice_ends = ends_sorted[lo:hi]
            visible = ~pd.isna(slice_ends) & (slice_ends <= np.datetime64(ts))
            counts = pd.Series(slice_sids[visible]).value_counts().reindex(station_index, fill_value=0.0).astype(float)
        counts[~has_any_trip] = np.nan
        out[ts] = counts
    return out


def _get_station_master() -> pd.DataFrame:
    """최신 Silver 보강 정류소 마스터를 station_id 인덱스로 캐시해 반환한다.

    feature_engine도 같은 보강 Silver의 최신 snapshot을 1차정제 산출물로 만들지만,
    inference는 그 중간 산출물 대신 Silver를 직접 읽어 정류소 신설/폐쇄를 즉시
    반영한다.

    returns:
        pd.DataFrame: station_id로 인덱싱된 정류소 마스터 (station_no, capacity, lat, lon, grid_id)
    """
    global _station_master
    if _station_master is None:
        keys = [
            key
            for key in s3_io.list_keys(silver_schema.STATION_MASTER_ENRICHED_PREFIX)
            if key.endswith(".parquet")
        ]
        if not keys:
            raise FileNotFoundError(
                f"S3에 없음: {silver_schema.STATION_MASTER_ENRICHED_PREFIX} 아래 parquet"
            )
        latest_key = max(keys)
        raw = s3_io.read_parquet(latest_key)
        if raw is None:
            raise FileNotFoundError(f"S3에 없음: {latest_key}")
        master = raw.rename(columns=silver_schema.STATION_COLUMN_MAP)
        if "grid_id" not in master:
            raise ValueError("보강 station master에 grid_id 컬럼이 없음")
        valid_grid = master["grid_id"].notna() & master["grid_id"].astype(str).str.strip().ne("")
        grid_coverage = float(valid_grid.mean())
        if grid_coverage < 0.95:
            raise ValueError(f"보강 station master의 grid_id 매핑률이 기준 미달: {grid_coverage:.3%}")
        master = master.set_index("station_id")
        # 행 전체를 한 번에 astype(int)하면 깨진 station_no 하나가 전체 배치를
        # 중단한다. 형 변환과 유효성 검사는 `_validated_station_row()`에서 station별로
        # 수행해 정상 정류소는 계속 서빙한다.
        _station_master = master
    return _station_master


def _validated_station_row(station_id: str, station_row: pd.Series | pd.DataFrame) -> pd.Series:
    """정류소 마스터 한 행을 검증하고 모델 입력용 숫자 타입으로 정규화한다.

    보강 마스터는 전체 grid_id 커버리지 게이트를 이미 통과했더라도 소수의 결측
    행을 허용한다. 그런 한 행 때문에 전체 정류소 배치가 죽지 않도록 이 검사는
    station 단위 예외를 만들며, 호출부가 해당 station만 failed로 격리한다.

    args:
        station_id: 오류 메시지에 포함할 정류소 ID
        station_row: station master에서 선택한 한 행
    returns:
        station_no/capacity는 int, lat/lon은 float, grid_id는 공백 제거 문자열로
        정규화한 행 복사본
    raises:
        ValueError: 필수 값이 결측·비수치·비정수·허용 좌표 범위 밖일 때
    """
    if isinstance(station_row, pd.DataFrame):
        raise TypeError(f"station master에 station_id가 중복됨: station_id={station_id!r}")

    normalized = station_row.copy()

    def _finite_number(column: str) -> float:
        """한 필드를 유한 실수로 변환하고 실패하면 station 문맥을 붙여 예외를 낸다."""
        message = f"station master의 {column} 값이 유효한 숫자가 아님: station_id={station_id!r}"
        try:
            raw_value = normalized[column]
            if isinstance(raw_value, (bool, np.bool_)):
                raise TypeError
            value = float(raw_value)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise ValueError(message) from exc
        if not np.isfinite(value):
            raise ValueError(message)
        return value

    for column, minimum in (("station_no", 1), ("capacity", 0)):
        value = _finite_number(column)
        if not value.is_integer() or value < minimum:
            raise ValueError(
                f"station master의 {column} 값이 유효한 정수가 아님: station_id={station_id!r}"
            )
        normalized[column] = int(value)

    latitude = _finite_number("lat")
    longitude = _finite_number("lon")
    if not 36.5 <= latitude <= 38.5:
        raise ValueError(f"station master의 lat 값이 서울 좌표 범위 밖임: station_id={station_id!r}")
    if not 125.5 <= longitude <= 128.5:
        raise ValueError(f"station master의 lon 값이 서울 좌표 범위 밖임: station_id={station_id!r}")
    normalized["lat"] = latitude
    normalized["lon"] = longitude

    try:
        grid_id = normalized["grid_id"]
    except KeyError as exc:
        raise ValueError(f"station master의 grid_id 값이 없음: station_id={station_id!r}") from exc
    if pd.isna(grid_id) or not str(grid_id).strip():
        raise ValueError(f"station master의 grid_id 값이 유효하지 않음: station_id={station_id!r}")
    normalized["grid_id"] = str(grid_id).strip()
    return normalized


def _get_holidays(year: int) -> set[str]:
    """year의 공휴일 목록을 연도별로 캐시해 반환한다.

    `holidays` 패키지(ml_core.holidays_kr)로 오프라인 계산하므로 연도가 몇 년이든
    바로 계산할 수 있다 — target_ts가 horizon만큼 미래로 밀려 해가 바뀌어도(예:
    12/31 앵커 + horizon으로 다음 해 1/1을 묻는 경우) 그 연도를 그대로 조회하면 된다.

    args:
        year: 조회할 연도
    returns:
        set[str]: 'YYYY-MM-DD' 형식의 그 연도 공휴일 집합
    """
    if year not in _holidays_by_year:
        _holidays_by_year[year] = korean_holidays(year)
    return _holidays_by_year[year]


def _build_station_profile_arrays(df: pd.DataFrame) -> tuple[dict[int, int], np.ndarray]:
    """station_hourly_profile 행들을 (station_no -> 축 인덱스) dict + dense numpy 배열로 압축한다.

    예전엔 (station_no, minute, dow, month) 튜플을 key로, {rental_mean, ...} dict를
    value로 갖는 파이썬 dict를 그대로 캐시했다 — 정류소 약 수천 개 x
    (1440/모델 tick) x 7요일 x 12개월 조합이라, dict-of-dict 하나당 파이썬 객체
    오버헤드(키 튜플 + 값 dict, 항목당 수백 바이트)만으로 프로세스당 수 GB를
    먹었다(리뷰 지적). minute/dow/month는 전부 값의 범위가 좁고 촘촘해서(tick
    간격으로 나누면 촘촘한 인덱스, dow 7개, month 12개) 해시 테이블이 필요 없다 — station마다
    dense numpy 배열 한 칸을 두면 항목당 파이썬 객체 오버헤드 없이 float32 4개만
    쓴다(5분 grid는 약 1GB, 기본 20분 grid는 그 약 1/4). station_no만 정수값
    자체가 넓게 흩어져 있어(정류소 자체는 수천 개뿐) 그것만 작은 dict로 따로
    0-based 인덱스로 압축한다.

    hour가 아니라 minute(자정 기준 경과분)으로 묶는 이유: rental_count/return_count
    자체가 60분짜리 미래 방향 롤링 합이라 인접한 모델 tick끼리 창이 많이 겹쳐
    거의 같은 값을 반복해서 보는 셈이다 — hour로 묶어 표본을 늘려도 독립적인 정보는
    별로 안 늘고, 오히려 모델이 실제로 구분하는 tick 단위(minute, BASE_FEATURE_COLUMNS
    참고)와 다른 값을 fallback으로 주게 된다(build_station_profile.py 모듈 docstring
    참고).

    month을 키에 포함하는 이유: 계절에 따라 대여량 자체가 크게 달라져서
    (실측 1월 대비 6월 약 2.44배), station x minute x dow로만 묶으면
    1월 결측과 6월 결측이 똑같은 연간 평균으로 채워지는 문제가 생긴다.

    args:
        df: station_no, minute, dow, month, rental_mean, rental_std, return_mean,
            return_std 컬럼을 가진 프로필 행들 (build_station_profile.py 산출물과 같은 스키마).
            minute은 GRID_TICK_MINUTES의 배수라고 가정한다(업스트림 grid 집계 결과이므로).
    returns:
        (station_no -> station 축 인덱스 dict, shape (n_station, 1440//tick, 7, 12, 4) float32 배열)
    raises:
        ValueError: profile key 차원 값이 범위를 벗어나거나 minute이 활성 모델
            grid에 맞지 않거나 logical key가 중복되어 dense 배열 한 칸을 여러
            행이 덮어쓸 수 있을 때
    """
    logical_key = ["station_no", "minute", "dow", "month"]
    station_no_values = pd.to_numeric(df["station_no"], errors="coerce")
    minute_values = pd.to_numeric(df["minute"], errors="coerce")
    dow_values = pd.to_numeric(df["dow"], errors="coerce")
    month_values = pd.to_numeric(df["month"], errors="coerce")

    invalid_station_no = station_no_values.isna() | (station_no_values % 1 != 0)
    if invalid_station_no.any():
        sample = df.loc[invalid_station_no, "station_no"].head(5).tolist()
        raise ValueError(f"station profile station_no가 정수가 아닙니다: sample={sample}")
    invalid_minute = (
        ~minute_values.between(0, 1439)
        | minute_values.isna()
        | (minute_values % config.GRID_TICK_MINUTES != 0)
    )
    if invalid_minute.any():
        sample = df.loc[invalid_minute, "minute"].head(5).tolist()
        raise ValueError(
            "station profile minute이 활성 모델 grid와 맞지 않습니다: "
            f"GRID_TICK_MINUTES={config.GRID_TICK_MINUTES}, sample={sample}"
        )
    invalid_dow = ~dow_values.between(0, 6) | dow_values.isna() | (dow_values % 1 != 0)
    invalid_month = ~month_values.between(1, 12) | month_values.isna() | (month_values % 1 != 0)
    if invalid_dow.any() or invalid_month.any():
        sample = df.loc[invalid_dow | invalid_month, ["dow", "month"]].head(5).to_dict("records")
        raise ValueError(f"station profile dow/month 범위가 잘못되었습니다: sample={sample}")

    normalized_key = pd.DataFrame({
        "station_no": station_no_values,
        "minute": minute_values,
        "dow": dow_values,
        "month": month_values,
    })
    duplicated = normalized_key.duplicated(subset=logical_key, keep=False)
    if duplicated.any():
        sample = df.loc[duplicated, logical_key].head(5).to_dict("records")
        raise ValueError(f"station profile logical key가 중복되었습니다: sample={sample}")

    station_nos = sorted(station_no_values.astype("int64").unique().tolist())
    station_index = {station_no: i for i, station_no in enumerate(station_nos)}
    n_minute_buckets = 1440 // config.GRID_TICK_MINUTES

    values = np.full(
        (len(station_nos), n_minute_buckets, 7, 12, len(_STATION_PROFILE_STAT_COLS)), np.nan, dtype="float32"
    )
    station_idx = station_no_values.map(station_index).to_numpy()
    minute_idx = minute_values.to_numpy(dtype="int64") // config.GRID_TICK_MINUTES
    dow_idx = dow_values.to_numpy(dtype="int64")
    month_idx = month_values.to_numpy(dtype="int64") - 1
    for stat_idx, col in enumerate(_STATION_PROFILE_STAT_COLS):
        values[station_idx, minute_idx, dow_idx, month_idx, stat_idx] = df[col].to_numpy()

    return station_index, values


def _get_station_profile() -> tuple[dict[int, int], np.ndarray]:
    """station_hourly_profile.parquet을 station_no 인덱스 dict + dense 배열로 캐시해 반환한다.

    station_id(텍스트)가 아니라 station_no(정수)로 조회한다 — build_station_profile.py가
    이제 그 기준으로 프로필을 만든다(모델 feature 자체가 station_no로 바뀐 것과 동일한
    이유, model_contract.BASE_FEATURE_COLUMNS 참고). 실제 압축 방식은
    `_build_station_profile_arrays()` 참고.

    returns:
        (station_no -> station 축 인덱스 dict, dense 배열) — `_profile_stat()` 참고
    """
    global _station_profile_station_index, _station_profile_values
    if _station_profile_values is None:
        df = s3_io.read_parquet(config.STATION_HOURLY_PROFILE_PARQUET)
        if df is None:
            raise FileNotFoundError(f"S3에 없음: {config.STATION_HOURLY_PROFILE_PARQUET}")
        _station_profile_station_index, _station_profile_values = _build_station_profile_arrays(df)
    return _station_profile_station_index, _station_profile_values


def _profile_stat(station_no: int, ts: pd.Timestamp, stat_key: str) -> float:
    """특정 시각(ts)의 (minute, dow, month)에 해당하는 station 평소 패턴 통계값을 조회한다.

    운영 요청은 5분 간격이고 station profile은 base 모델 grid(기본 20분,
    비교 프로필은 5~60분)의 모든 tick에 존재한다. 요청 시각이 실제 학습 anchor
    사이면 같은 날의 직전 anchor로 내림한다(예: g5/a20의 17:05/10/15 ->
    17:00). 실시간 point-in-time lag 계산은 정확한
    요청 시각을 그대로 쓰며, 이 정렬은 실시간 히스토리가 없어 과거 평균 profile을
    fallback으로 쓸 때만 적용한다. 미래 anchor나 검증되지 않은 보간값을 쓰지 않아
    profile fallback도 모델이 실제 학습한 값의 분포 안에 남긴다.

    args:
        station_no: 정류소 일련번호(station_master 크로스워크로 얻은 정수)
        ts: 조회할 시각 (minute_of_day/dayofweek/month를 사용 — month으로 계절성 반영)
        stat_key: "rental_mean" / "rental_std" / "return_mean" / "return_std" 중 하나
    returns:
        float: 직전 학습 anchor의 프로필 값. 해당 station 또는 조합의 프로필이
            없으면 NaN
    """
    station_index, values = _get_station_profile()
    row = station_index.get(station_no)
    if row is None:
        return np.nan
    minute = minute_of_day(ts)
    profile_minute = minute - minute % config.TRAIN_ANCHOR_TICK_MINUTES
    return values[
        row,
        profile_minute // config.GRID_TICK_MINUTES,
        ts.dayofweek,
        ts.month - 1,
        _STATION_PROFILE_STAT_INDEX[stat_key],
    ]


def _get_population_profile() -> dict[tuple[str, int, int], dict[str, float]]:
    """population_hourly_profile.parquet을 (grid_id, hour, dow) 키의 dict로 캐시해 반환한다.

    station 프로필과 달리 month을 키에 넣지 않는다 — 생활인구는 월별 변동이
    미미하고(1.05배) 시간대별 변동이 지배적이라(1.42배, 출퇴근 패턴) month을
    추가해도 얻는 게 적고 표본만 station 프로필처럼 줄어든다. pop_total만 모델
    피처라 나머지 국적별 breakdown 컬럼(parquet엔 남아있어도)은 읽지 않는다.

    returns:
        dict[tuple[str, int, int], dict[str, float]]: (grid_id, hour, dow) -> {pop_total_mean}
    """
    global _population_profile
    if _population_profile is None:
        df = s3_io.read_parquet(config.POPULATION_HOURLY_PROFILE_PARQUET)
        if df is None:
            raise FileNotFoundError(f"S3에 없음: {config.POPULATION_HOURLY_PROFILE_PARQUET}")
        _population_profile = {
            (r.grid_id, r.hour, r.dow): {"pop_total_mean": r.pop_total_mean} for r in df.itertuples()
        }
    return _population_profile


def _population_fallback(grid_id: str, ts: pd.Timestamp) -> float:
    """population 인자가 없을 때 그 격자의 평소 인구(hour, dow 기준)로 대체한다.

    args:
        grid_id: 정류소가 속한 250m 격자 ID
        ts: 예측하려는 시각 (hour/dayofweek만 사용)
    returns:
        float: pop_total (프로필이 없으면 NaN)
    """
    entry = _get_population_profile().get((grid_id, ts.hour, ts.dayofweek))
    return entry["pop_total_mean"] if entry is not None else np.nan


# --- 실시간 S3 조회(날씨/재고/인구) — 호출자가 temp/precip/population/stockout을
# 안 주면 이 함수들로 Silver에서 직접 읽는다. 학습(1년치 전체)과 달리 "그 시점
# 근처의 최근 값 하나"만 있으면 되므로 조회 범위가 훨씬 좁다. ---


def _weather_values(df: pd.DataFrame | None) -> dict[str, float] | None:
    """Silver 관측 행에서 유효한 기온·강수량만 골라 서울 평균을 만든다.

    Collector의 weather 스키마 범위(T1H -50~50°C, RN1 0~500mm)를 그대로
    적용한다. 최신 tick이 비어 있거나 숫자로 바꿀 수 없는 값, NaN/무한대,
    범위 밖 값만 담고 있으면 None을 반환해 호출부가 이전 tick을 계속 찾게 한다.

    args:
        df: weather_ultra_short_live Silver 한 tick의 원본 DataFrame
    returns:
        유효 행 평균의 temp/precip dict. 유효한 행이 없으면 None
    """
    if df is None or df.empty:
        return None
    values = df.rename(columns=silver_schema.WEATHER_COLUMN_MAP)
    required = ["temp", "precip"]
    if not set(required).issubset(values.columns):
        return None
    numeric = values[required].apply(pd.to_numeric, errors="coerce")
    valid = (
        np.isfinite(numeric["temp"])
        & np.isfinite(numeric["precip"])
        & numeric["temp"].between(-50.0, 50.0)
        & numeric["precip"].between(0.0, 500.0)
    )
    if not valid.any():
        return None
    means = numeric.loc[valid, required].mean()
    return {"temp": float(means["temp"]), "precip": float(means["precip"])}


def _get_recent_weather(
    target_ts: pd.Timestamp,
    lookback_hours: float = silver_schema.WEATHER_MAX_STALENESS_HOURS,
) -> dict[str, float]:
    """target_ts 시각(또는 그 근처)의 Silver 기상 **관측값**을 읽는다(서울 전체 공유).

    `weather_ultra_short_live`(기상청 초단기실황, 5분 수집 tick)만 쓴다 — 관측이라
    target_ts가 미래면 애초에 존재하지 않는다(이 함수는 "막 지난 시각"을 다루는
    용도). target_ts가 미래(horizon>1)면 `_resolve_live_weather()`가 이 함수 대신
    `_get_forecast_weather()`를 먼저 시도한다 — 이 함수는 target_ts가 anchor_ts와
    같거나 과거일 때, 또는 예보를 못 찾았을 때의 fallback으로만 호출된다.
    target_ts 키가 정확히 있으면 그걸, 없으면(수집 지연) 거슬러 올라가 가장 최근
    값을 대신 쓴다.

    args:
        target_ts: 조회하려는 시각
        lookback_hours: target_ts 키가 없을 때 몇 시간 전까지 대신 찾아볼지
    returns:
        dict[str, float]: temp, precip (wind/humidity는 더 이상 모델 피처가 아니라 안 읽음)
    raises:
        ValueError: target_ts부터 lookback_hours시간 전까지 전부 데이터가 없을 때
    """
    keys = silver_schema.weather_tick_keys(target_ts, lookback_hours)
    for df in reversed(s3_io.read_parquet_many(keys)):  # target_ts에 가장 가까운 것부터
        weather = _weather_values(df)
        if weather is not None:
            return weather
    raise ValueError(f"최근 {lookback_hours}시간 안에 날씨 데이터가 없습니다(target_ts={target_ts})")


def _get_forecast_weather(target_ts: pd.Timestamp, issue_lookback_hours: float = 24.0) -> dict[str, float] | None:
    """target_ts(미래) 시각의 예보(`weather_short_term_forecast`)를 찾는다.

    관측 소스와 달리 파일 하나에 미래 여러 시각의 예보가 여러 행으로 들어있다 —
    그래서 "그 시각의 파일"을 바로 읽는 게 아니라, 가장 최근에 발표된 예보
    파일부터 훑으면서 그 안에서 target_ts와 가장 가까운 시각의 모든 격자 행을
    검증하고 평균 내서 쓴다. 타겟 시각은 `fcstDate`(YYYYMMDD)+
    `fcstTime`(HHMM, KST) 두 컬럼을 합쳐서 구한다
    (단일 컬럼이 아님 — 기상청 raw 응답 자체가 이 형태, `loader/transform.py`의
    `weather_forecast_from_silver()`가 같은 소스를 이미 이렇게 읽고 있어 그
    스키마를 그대로 근거로 쓴다). 강수량(`PCP`)은 순수 숫자가 아니라
    "강수없음"/"1.0mm 미만"/"30.0~50.0mm" 같은 텍스트가 섞여 있어
    `silver_schema.parse_kma_precip_text()`로 파싱한다 — 단순 컬럼 rename으로는
    안 된다. 가장 가까운 행도 target_ts에서 35분을 넘게 벗어나면 해당 발표본은
    사용할 수 없는 것으로 보고 더 오래된 발표본을 계속 찾는다.

    args:
        target_ts: 예보를 찾으려는 미래 시각
        issue_lookback_hours: 예보 발표 파일을 몇 시간 전까지 거슬러 찾아볼지
            (발표 주기가 정확히 알려지지 않아 넉넉히 잡음, `weather_forecast_issue_keys()` 참고)
    returns:
        dict[str, float] | None: temp, precip — 예보 파일을 하나도 못 찾거나
            타겟 시각 컬럼이 없거나 강수량 파싱에 전부 실패하면 None(호출부가
            관측치 fallback으로 넘어감)
    """
    keys = silver_schema.weather_forecast_issue_keys(target_ts, issue_lookback_hours)
    date_col, time_col = silver_schema.WEATHER_FORECAST_DATE_COLUMN, silver_schema.WEATHER_FORECAST_TIME_COLUMN
    for df in reversed(s3_io.read_parquet_many(keys)):  # 가장 최근 발표 파일부터
        if df is None or df.empty or date_col not in df.columns or time_col not in df.columns:
            continue
        fcst_ts = pd.to_datetime(
            df[date_col].astype(str).str.zfill(8) + df[time_col].astype(str).str.zfill(4),
            format="%Y%m%d%H%M",
            errors="coerce",
        )
        distances = (fcst_ts - target_ts).abs()
        if distances.isna().all():
            continue
        minimum_distance = distances.min()
        if minimum_distance > _MAX_FORECAST_DISTANCE:
            continue
        # 한 발표 파일에는 같은 예보 시각의 서울 격자 행이 여러 개 있다. 임의의
        # 첫 행 하나를 고르면 S3/concat 순서에 따라 값이 달라지므로, 최소 시간거리인
        # 행을 모두 모은 뒤 유효한 격자들의 서울 평균을 사용한다.
        nearest = df.loc[distances.eq(minimum_distance)].rename(
            columns=silver_schema.WEATHER_FORECAST_COLUMN_MAP
        )
        if "temp" not in nearest.columns or "PCP" not in nearest.columns:
            continue
        numeric = pd.DataFrame({
            "temp": pd.to_numeric(nearest["temp"], errors="coerce"),
            "precip": pd.to_numeric(
                nearest["PCP"].map(silver_schema.parse_kma_precip_text), errors="coerce"
            ),
        })
        valid = (
            np.isfinite(numeric["temp"])
            & np.isfinite(numeric["precip"])
            & numeric["temp"].between(-50.0, 50.0)
            & numeric["precip"].between(0.0, 500.0)
        )
        if not valid.any():
            continue
        means = numeric.loc[valid, ["temp", "precip"]].mean()
        return {"temp": float(means["temp"]), "precip": float(means["precip"])}
    return None


def _get_recent_bike_status(anchor_ts: pd.Timestamp, lookback_hours: float = 1.0) -> pd.DataFrame:
    """가장 최근 5분 tick의 Silver 실시간 대여소 현황을 읽는다(전체 정류소가 한 파일에 있음).

    args:
        anchor_ts: 조회 기준 시각("지금")
        lookback_hours: anchor_ts 키가 없을 때 몇 시간 전까지 대신 찾아볼지
    returns:
        pd.DataFrame: station_id로 인덱싱된 bike_count/capacity/stockout_flag
            (lookback 안에 데이터가 전혀 없으면 빈 DataFrame — 호출부가 재고
            정보 없음으로 처리)
    """
    keys = silver_schema.bike_realtime_tick_keys(anchor_ts, lookback_hours)
    for key in reversed(keys):
        df = s3_io.read_parquet(key)
        if df is not None and not df.empty:
            df = df.rename(columns=silver_schema.BIKE_REALTIME_COLUMN_MAP)
            df["stockout_flag"] = (df["bike_count"] <= 0).astype(int)
            return df.set_index("station_id")[["bike_count", "capacity", "stockout_flag"]]
    return pd.DataFrame(columns=["bike_count", "capacity", "stockout_flag"])


def _get_recent_population(target_ts: pd.Timestamp, lookback_hours: float = 1.0) -> pd.DataFrame:
    """target_ts 시각(또는 그 근처)의 `living_population_normalized`(정규화된 생활인구) 스냅샷을 읽는다(격자별).

    **원본이 아니라 정규화된 소스를 쓴다**: `living_population_grid`(원본, 하루
    1개 파일에 공표 지연까지 있어 "지금 이 순간"을 못 담음)를 그대로 서빙에 쓰면
    실시간 추론 시점과 동떨어진 인구값이 들어간다. `normalizer`(舊
    seoul-pop-normalizer)가 원본 생활인구 추정치를 그 시각의 실시간 도시데이터
    (`population_realtime`, POI 121개 지점)로 보정해 5분마다 `living_population_normalized`에
    쌓아두므로, 서빙엔 이 보정된 값을 쓴다(`docs/normalizer/implementation_plan.md`).
    **학습/평가는 이 함수와 무관하게 여전히 원본을 그대로 쓴다** —
    `feature_engine/spark/silver_source.py`의 `read_population()`은 안 바뀌었다.
    정답 라벨(피처마트)은 사후 보정 없는 실측 그대로여야 학습-서빙 간 값의 의미가
    갈리지 않는다.

    `weather_ultra_short_live`/`bike_station_realtime`과 같은 5분 tick 소스라
    `_get_recent_weather()`/`_get_recent_bike_status()`와 동일한 패턴을 쓴다 —
    target_ts를 5분 단위로 내림한 키가 있으면 그걸, 없으면(정규화 지연 등)
    거슬러 올라가 가장 최근 값을 대신 쓴다. 원본과 달리 시각이 이미 S3 키
    경로(dt=/hh=/HHMM)에 있어 파일 내용에 YMD/TT 컬럼이 없다 — 그래서 원본처럼
    "그 안에서 TT만 맞춰 거르는" 과정이 필요 없다.

    실제 예시 데이터 기준으로 나이대x성별 인구(`M00`~`M70`/`F00`~`F70`)만 있고,
    `pop_total`(모델 피처)은 `SPOP`(총 생활인구, normalizer가 보정한 값)을 그대로 쓴다.

    args:
        target_ts: 조회하려는 시각(horizon에 따라 미래일 수 있음)
        lookback_hours: target_ts 키가 없을 때 몇 시간 전까지 대신 찾아볼지
    returns:
        pd.DataFrame: grid_id로 인덱싱된 pop_total (lookback 안에 데이터가 전혀
            없으면 빈 DataFrame — 호출부가 population_hourly_profile fallback으로
            자연히 넘어감)

    같은 target_ts로 여러 번 불려도(정류소마다 반복 호출하는 경우) 최초 1번만
    S3에서 읽는다 — 격자 전체가 파일 하나에 들어있어 정류소별로 다시 읽을 필요가
    없다(설계 원칙 6번, `_get_recent_weather()`/`_get_recent_bike_status()`와 동일한
    이유로 여기서만 캐싱: 이 함수는 station이 아니라 target_ts 축으로 반복 호출된다).
    """
    if target_ts in _recent_population_by_ts:
        return _recent_population_by_ts[target_ts]

    keys = silver_schema.population_normalized_tick_keys(target_ts, lookback_hours)
    result = pd.DataFrame(columns=["pop_total"])
    for df in reversed(s3_io.read_parquet_many(keys)):  # target_ts에 가장 가까운 것부터
        if df is not None and not df.empty:
            df = df.rename(columns=silver_schema.POPULATION_COLUMN_MAP)
            result = df.set_index("grid_id")[["pop_total"]]
            break

    _recent_population_by_ts[target_ts] = result
    return result


def _lag_rolling_features(
    station_id: str, station_no: int, target_ts: pd.Timestamp, skip_rental_recent: bool = False
) -> tuple[dict[str, float], list[str]]:
    """실시간 히스토리에서 lag_1h(대여/반납 각 1개)를 계산하되, 없는 값은 station 평소 패턴(profile)으로 대체한다.

    반납(return_lag_1h)은 시간 단위 집계 히스토리(_get_history_by_station)로 그대로
    계산한다 — 지연 관측 문제가 없기 때문이다. 대여(rental_lag_1h)는
    _censored_rental_recent()가 트립 단위 원본으로 따로 계산한다
    (features.py의 _rental_visible/_add_rental_lag_1h와 같은 원칙 — 배치/실시간 두
    경로로 나뉘어 있을 뿐. 자세한 배경은 REALTIME_FEATURES.md).

    args:
        station_id: 정류소 ID — 실시간 히스토리/트립 조회(Silver가 이 형식으로만
            제공)에 쓴다
        station_no: 정류소 일련번호 — profile fallback 조회에만 쓴다(station_hourly_profile이
            이 키로 만들어짐, `_profile_stat()` 참고)
        target_ts: 예측하려는 시각
        skip_rental_recent: True면 `_censored_rental_recent()`(트립 단위 dense
            조회, 이 시각 하나에도 anchor 300여 개 스캔이 필요해 비쌈)를 안
            부르고 rental_lag_1h를 NaN placeholder로 둔다 —
            `predict_demand_multi_hour_all_stations()`가 전체 정류소를 한
            번에 벡터화해서 계산하는 `_rental_recent_batch()`로 이 자리를 대신
            채우므로, 정류소마다 비싼 계산을 중복으로 할 필요가 없을 때만 쓴다.
    returns:
        tuple[dict[str, float], list[str]]: (rental_lag_1h/return_lag_1h 2개 feature
            dict, fallback을 쓴 feature 이름 목록 — 비어있으면 전부 실시간 데이터를
            그대로 썼다는 뜻)
    """
    history = _get_history_by_station(target_ts).get(station_id)
    out: dict[str, float] = {}
    fallback_fields: list[str] = []

    return_point = target_ts - pd.Timedelta(hours=1)
    series = history["return_count"] if history is not None else pd.Series(dtype=float)
    return_val = series.get(return_point, np.nan)
    if pd.isna(return_val):
        return_val = _profile_stat(station_no, return_point, "return_mean")
        fallback_fields.append("return_lag_1h")
    out["return_lag_1h"] = return_val

    if skip_rental_recent:
        out["rental_lag_1h"] = np.nan  # 호출부(predict_demand_multi_hour)가 곧 실제 값으로 덮어씀
    else:
        _censored_rental_recent(station_id, station_no, target_ts, out, fallback_fields)

    return out, fallback_fields


def _censored_rental_recent(
    station_id: str, station_no: int, target_ts: pd.Timestamp, out: dict[str, float], fallback_fields: list[str]
) -> None:
    """대여의 rental_lag_1h를 point-in-time censored 값으로 채운다.

    대여는 반납이 완료돼야 로그에 잡히므로, 시간 단위 집계 히스토리로는 실제 서빙
    시점에 관측 가능했던 값을 재현할 수 없다 — 트립 단위 이벤트에
    rolling_window_features.count_visible_in_window()를 직접 적용한다.
    `rental_lag_1h`는 target_ts 자체에서 계산한 값이다(추가 shift 불필요 — 이미
    [target_ts-100분, target_ts-40분) 이전 정보만 씀, `config.ROLLING_EMBARGO_MINUTES`/
    `ROLLING_WINDOW_MINUTES` 기준 — `_rental_visible_at()` 참고).

    args:
        station_id: 정류소 ID — 트립 이벤트 조회용
        station_no: 정류소 일련번호 — profile fallback 조회용
        target_ts: 예측하려는 시각
        out: 채워질 feature dict (in-place)
        fallback_fields: fallback 쓴 필드 이름이 append되는 리스트 (in-place)
    """
    visible_now = _rental_visible_at(station_id, [target_ts]).iloc[0]
    if pd.isna(visible_now):
        visible_now = _profile_stat(station_no, target_ts, "rental_mean")
        fallback_fields.append("rental_lag_1h")
    out["rental_lag_1h"] = visible_now


def _target_timestamp(date: str, hour: int, minute: int = 0) -> pd.Timestamp:
    """date+hour+minute을 target_ts로 조합한다.

    `minute`은 모델 학습 grid와 무관하게 운영 호출 주기인
    `config.SERVING_TICK_MINUTES`의 배수여야 한다. 5~60분 모델 grid 중 어느 것을
    선택해도 5분마다 호출할 수 있지만, 실제 운영 계약에 없는 임의 시각(예:
    17:07)은 거부한다.

    args:
        date: "YYYY-MM-DD"
        hour: 0~23
        minute: 0~59 중 SERVING_TICK_MINUTES의 배수 (기본값 0 — 정시)
    returns:
        pd.Timestamp: date+hour+minute을 합친 시각
    raises:
        ValueError: hour가 0~23 밖이거나 minute이 0~59 밖이거나 SERVING_TICK_MINUTES의
            배수가 아닐 때
    """
    if not (0 <= hour <= 23):
        raise ValueError(f"hour는 0~23 사이여야 함: {hour}")
    if not (0 <= minute < 60) or minute % config.SERVING_TICK_MINUTES != 0:
        raise ValueError(f"minute은 0~59 사이의 {config.SERVING_TICK_MINUTES}분 배수여야 함: {minute}")
    return pd.Timestamp(date) + pd.Timedelta(hours=hour, minutes=minute)


def _build_target_time_fields(
    station_id: str,
    station_row: pd.Series,
    target_ts: pd.Timestamp,
    temp: float,
    precip: float,
    population: float | None,
    stockout: bool,
    horizon: int,
) -> tuple[dict, bool]:
    """target_ts(및 horizon)에만 의존하는 feature 필드를 조립한다 — lag는 포함하지 않는다.

    lag(직전 실적)는 항상 anchor_ts(T0) 기준으로 딱 한 번만 계산하면 되고
    horizon이 바뀌어도 안 바뀌는 반면(`_lag_rolling_features()` 참고), 날씨/생활인구/
    캘린더/타겟은 horizon마다 다른 target_ts=T0+(horizon-1)시간 기준으로 매번 새로
    계산해야 한다 — 이 함수가 그 "매번 새로 계산해야 하는 부분"만 담당한다
    (history.md 18번 항목 — "horizon을 feature로", 재귀 예측 대체).

    `_build_feature_record()`(단발 호출, lag도 같이 계산)와
    `predict_demand_multi_hour()`/`predict_demand_multi_hour_all_stations()`(lag를
    한 번만 계산해두고 이 함수만 horizon마다 반복 호출)가 같이 쓴다.

    args:
        station_id: 정류소 ID
        station_row: `_get_station_master().loc[station_id]` (station_no/capacity/lat/lon/grid_id)
        target_ts: 이 horizon이 가리키는 예측 대상 시각(구간의 시작점)
        temp, precip: 이 horizon에 적용할 날씨(이미 스칼라로 resolve된 값)
        population: 생활인구 합계(pop_total). None이면 Silver 실시간 값을 target_ts
            기준으로 먼저 시도하고, 그 격자 데이터가 없으면 평소 인구(hour, dow
            기준)로 자동 대체 — horizon마다 자동으로 달라짐
        stockout: 이 horizon의 재고 없음 가정 여부
        horizon: 몇 시간 뒤인지(1~HORIZON_COUNT) — `record["horizon"]`에 그대로 들어감
    returns:
        tuple[dict, bool]: (station_id/station_no/capacity/lat/lon/날씨/인구/캘린더/
            rental_exposure/horizon/date를 담은 dict — lag 없음, population_fallback 여부)
    """
    population_fallback = population is None
    if population_fallback:
        live_population = _get_recent_population(target_ts)
        grid_id = station_row["grid_id"]
        if grid_id in live_population.index:
            pop_total = float(live_population.loc[grid_id, "pop_total"])
            population_fallback = False  # 실시간 값을 실제로 썼으므로 fallback 아님
        else:
            pop_total = _population_fallback(grid_id, target_ts)
    else:
        pop_total = population

    target_date = target_ts.strftime("%Y-%m-%d")
    target_hour = int(target_ts.hour)
    dow = target_ts.dayofweek
    holidays = _get_holidays(target_ts.year)
    fields = {
        "station_id": station_id,  # 모델 feature는 아니지만 출력 식별용으로 그대로 둔다
        "station_no": int(station_row["station_no"]),
        "capacity": int(station_row["capacity"]),  # 거치대 수 — 항상 정수(model_contract 참고)
        "lat": float(station_row["lat"]),
        "lon": float(station_row["lon"]),
        "temp": temp,
        "precip": precip,
        "pop_total": pop_total,
        "hour": target_hour,  # 모델 feature는 아니지만 출력 식별용으로 그대로 둔다(minute이 대체)
        "minute": minute_of_day(target_ts),
        "dow": dow,
        "is_holiday": int(target_date in holidays or dow >= 5),
        "day": day_index(target_ts.date()),
        "rental_exposure": config.EXPOSURE_STOCKOUT_VALUE if stockout else 1.0,
        "horizon": horizon,
        "date": target_date,
    }
    return fields, population_fallback


def _build_feature_record(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    population: float | None,
    stockout: bool,
    skip_rental_recent: bool = False,
    minute: int = 0,
    horizon: int = 1,
) -> tuple[dict, list[str], bool]:
    """예측 1건에 필요한 feature 값을 dict로 조립한다(DataFrame 생성/dtype 캐스팅은 안 함).

    lag는 anchor_ts(date+hour+minute, "지금" T0) 기준으로, 날씨/캘린더/타겟은
    target_ts=anchor_ts+(horizon-1)시간 기준으로 계산한다 — horizon=1(기본값)이면
    anchor_ts==target_ts라 이전 동작과 완전히 동일하다(`_build_target_time_fields()`
    참고).

    `_build_feature_row()`(단일 정류소용, 이 함수를 감싸서 1행 DataFrame으로 반환)가
    쓴다 — `predict_demand_multi_hour()`/`predict_demand_multi_hour_all_stations()`는
    lag를 horizon마다 다시 계산하지 않도록 이 함수를 거치지 않고
    `_lag_rolling_features()`/`_build_target_time_fields()`를 직접 조합한다.

    args/returns: `_build_feature_row()`와 동일한 의미, 반환은 (record dict,
        fallback_fields, population_fallback) 3개.
    raises:
        ValueError: station_id가 station_master에 없거나 hour/minute이 범위를
            벗어나거나(`_target_timestamp()` 참고) horizon이 1~HORIZON_COUNT 밖일 때
    """
    master = _get_station_master()
    if station_id not in master.index:
        raise ValueError(f"알 수 없는 station_id: {station_id!r} (최신 보강 station master에 없음)")
    station_row = _validated_station_row(station_id, master.loc[station_id])

    if not (1 <= horizon <= config.HORIZON_COUNT):
        raise ValueError(f"horizon은 1~{config.HORIZON_COUNT} 사이여야 함: {horizon}")

    anchor_ts = _target_timestamp(date, hour, minute)
    target_ts = anchor_ts + pd.Timedelta(hours=horizon - 1)

    lag_features, fallback_fields = _lag_rolling_features(
        station_id, int(station_row["station_no"]), anchor_ts, skip_rental_recent=skip_rental_recent
    )
    target_fields, population_fallback = _build_target_time_fields(
        station_id, station_row, target_ts, temp, precip, population, stockout, horizon,
    )
    record = {**target_fields, **lag_features}

    missing = [c for c in _ALL_FEATURE_COLUMNS if c not in record]
    assert not missing, f"feature 누락: {missing}"  # RENTAL/RETURN_FEATURE_COLUMNS와 안 맞으면 여기서 바로 발견됨

    return record, fallback_fields, population_fallback


def _build_feature_row(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    population: float | None,
    stockout: bool,
    skip_rental_recent: bool = False,
    minute: int = 0,
    horizon: int = 1,
) -> pd.DataFrame:
    """예측 1건에 필요한 feature 1행짜리 DataFrame을 조립한다.

    args:
        station_id: 정류소 ID
        date: "YYYY-MM-DD"
        hour: 0~23
        temp: 기온(°C)
        precip: 강수량(mm)
        population: 그 정류소가 속한 250m 격자의 생활인구 합계(pop_total). None이면
            격자 평소 인구(population_hourly_profile)로 대체
        stockout: 그 시각 대여 가능한 자전거가 없었는지 여부
        skip_rental_recent: `_lag_rolling_features()` 참고 — True면 rental_lag_1h를
            비싼 실시간 조회 없이 NaN으로 두고, 호출부가 바로 덮어쓸 걸 전제한다
        minute: 0~59 중 `config.SERVING_TICK_MINUTES`의 배수 (기본값 0 — 정시).
            모델 학습 grid가 20분이어도 point-in-time lag는 이 요청 시각을 정확히
            사용한다(`_target_timestamp()` 참고).
        horizon: 몇 시간 뒤를 예측할지(1~HORIZON_COUNT, 기본값 1). lag는
            date+hour+minute("지금") 기준으로 고정하고, 날씨/캘린더/타겟만
            horizon만큼 미래로 이동한다(`_build_feature_record()` 참고).
    returns:
        pd.DataFrame: RENTAL_FEATURE_COLUMNS + RETURN_FEATURE_COLUMNS를 모두
            포함하는 1행 DataFrame. attrs["fallback_fields"]/attrs["population_fallback"]에
            fallback 사용 여부가 담김
    raises:
        ValueError: station_id가 station_master에 없거나 hour/minute/horizon이
            범위를 벗어날 때(`_build_feature_record()` 참고)
    """
    record, fallback_fields, population_fallback = _build_feature_record(
        station_id, date, hour, temp, precip, population, stockout, skip_rental_recent,
        minute=minute, horizon=horizon,
    )
    df = pd.DataFrame([record])
    df.attrs["fallback_fields"] = fallback_fields
    df.attrs["population_fallback"] = population_fallback

    # Python 스칼라로 조립한 행이라 기본 float64/int64로 들어와 있다 — 학습 데이터
    # (feature_engine이 다운캐스트한 float32/int8/int16, ml_core.model_contract의
    # RENTAL/RETURN_FEATURE_COLUMN_DTYPES)와 dtype을 맞춘다. 값 자체는 바뀌지 않지만
    # (LightGBM은 어차피 내부적으로 캐스팅해서 예측 결과에 영향 없음) 학습/서빙
    # 스키마가 정확히 일치해야 한다는 이 프로젝트의 원칙(model_contract.py 모듈
    # docstring)을 dtype까지 지키기 위함.
    df = df.astype(_COMBINED_FEATURE_COLUMN_DTYPES)
    df["rental_exposure"] = df["rental_exposure"].astype(RENTAL_EXPOSURE_DTYPE)
    return df


def _predict_at(model_name: str, exposure_col: str | None, **kwargs) -> dict:
    """feature 행을 만들고 예측한 뒤, fallback 정보를 포함한 결과 dict를 만든다.

    args:
        model_name: "rental" 또는 "return"
        exposure_col: predict()에 전달할 exposure 컬럼명 (반납은 None)
        **kwargs: _build_feature_row()에 그대로 전달할 인자 (station_id 포함 — 항상
            키워드로 넘어온다)
    returns:
        dict: station_id, date, hour, minute, horizon, pred_mean, pred_p10, pred_p50,
            pred_p90, lag_fallback_used, lag_data_freshness, population_source
    """
    df = _build_feature_row(**kwargs)
    fallback_fields = df.attrs.get("fallback_fields", [])
    population_fallback = df.attrs.get("population_fallback", False)
    try:
        pred = predict(df, model_name, exposure_col=exposure_col)
    finally:
        # 공개 단건 API와 기본 CLI도 장수 프로세스에서 booster 4개를 계속 들고
        # 있지 않도록 성공/실패와 무관하게 해당 모델 채점 직후 비운다.
        _release_booster_cache()
    row = pred.iloc[0]
    # predict()의 출력은 station_no만 담는다(ml_core.scoring.predict() docstring
    # 참고) — station_id는 이 함수 자신의 kwargs에 이미 있으므로 거기서 그대로 쓴다.
    return {
        "station_id": kwargs["station_id"],
        "date": row["date"],
        "hour": int(row["hour"]),
        "minute": int(kwargs.get("minute", 0)),
        "horizon": int(kwargs.get("horizon", 1)),
        "pred_mean": float(row["pred_mean"]),
        "pred_p10": float(row["pred_p10"]),
        "pred_p50": float(row["pred_p50"]),
        "pred_p90": float(row["pred_p90"]),
        "lag_fallback_used": fallback_fields,
        "lag_data_freshness": round(1 - len(fallback_fields) / N_LAG_ROLLING_FEATURES, 3),
        "population_source": "fallback" if population_fallback else "provided",
    }


def _release_booster_cache() -> None:
    """혼합 모델 채점 사이와 종료 뒤 booster 캐시 메모리를 즉시 회수한다."""
    scoring_io.load_boosters.cache_clear()
    gc.collect()


def _resolve_weather_for_horizon(value: float | Sequence[float], h: int, n_hours: int, name: str) -> float:
    """날씨 인자가 스칼라(전체 horizon 재사용)인지 길이 n_hours 시퀀스(horizon별 예보)인지
    판별해 h번째(1-based) horizon에 적용할 값을 꺼낸다.

    실제 시간대별 예보 데이터가 있으면 시퀀스로 넘기고, 없으면(예보 API 미연동, 알려진
    단순화 — 이전과 동일) 스칼라로 넘겨 전체 horizon에 재사용한다.

    args:
        value: 스칼라 또는 길이 n_hours인 시퀀스(list/tuple/np.ndarray/pd.Series)
        h: 1-based horizon (1~n_hours)
        n_hours: 전체 horizon 개수 — 시퀀스 길이가 이와 같아야 함
        name: 에러 메시지에 쓸 인자 이름(예: "temp")
    returns:
        float: h번째 horizon에 적용할 값
    raises:
        ValueError: 시퀀스인데 길이가 n_hours와 다를 때
    """
    if isinstance(value, (list, tuple, np.ndarray, pd.Series)):
        if len(value) != n_hours:
            raise ValueError(f"{name}이 배열이면 길이가 n_hours({n_hours})와 같아야 함: {len(value)}")
        return float(value[h - 1])
    return float(value)


def _rental_recent_batch(
    station_ids: list[str],
    station_no_by_id: dict[str, int],
    anchor_ts: pd.Timestamp,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """전체 정류소의 rental_lag_1h를 anchor_ts(T0) 기준으로 한 번에 계산한다.

    `_censored_rental_recent()`를 정류소 축이 아니라 anchor 축으로 벡터화한 버전 —
    `predict_demand_multi_hour_all_stations()`가 정류소마다 이 계산을 반복하던 것
    (history.md 22/23번 항목 최적화 이후에도 여전히 병목의 상당수)을
    `_rental_visible_batch_all_stations()`로 anchor 개수만큼만 반복하도록
    바꿨다(24번 항목, 패러다임 전환 — station-outer/anchor-inner를
    anchor-outer/station-vectorized로 뒤집음). horizon-as-feature 전환(18번 항목)
    이후로는 lag가 항상 anchor_ts(T0) 기준 하나뿐이라 이 함수도 station당
    (horizon 개수와 무관하게) 딱 한 번만 호출하면 된다.

    args:
        station_ids: 대상 정류소 목록
        station_no_by_id: station_id -> station_no (profile fallback 조회용,
            `_profile_stat()`이 이제 station_no로 키를 잡으므로 필요)
        anchor_ts: "지금(T0)" — 값이 없으면 정류소별 `_profile_stat()` fallback을 쓴다
    returns:
        tuple[pd.DataFrame, dict[str, list[str]]]: (index=station_id, columns=rental_lag_1h,
            station_id -> profile fallback을 쓴 항목 이름 목록)
    """
    combined = pd.DataFrame(_rental_visible_batch_all_stations(station_ids, [anchor_ts]))

    station_index = pd.Index(station_ids, name="station_id")
    out = pd.DataFrame(index=station_index)
    fallback: dict[str, list[str]] = {sid: [] for sid in station_ids}

    lag1_vals = combined[anchor_ts].copy()
    missing = lag1_vals.isna()
    for sid in station_index[missing]:
        lag1_vals.loc[sid] = _profile_stat(station_no_by_id[sid], anchor_ts, "rental_mean")
        fallback[sid].append("rental_lag_1h")
    out["rental_lag_1h"] = lag1_vals

    return out, fallback


def predict_demand_multi_hour(
    station_id: str,
    date: str,
    hour: int,
    temp: float | Sequence[float] | None = None,
    precip: float | Sequence[float] | None = None,
    population: float | None = None,
    *,
    minute: int = 0,
    stockout: bool | None = None,
    n_hours: int = 1,
) -> list[dict]:
    """(date, hour, minute)를 "지금(T0)"으로 놓고, horizon=1..n_hours를 한 번에 예측한다.

    lag(직전 실적)는 T0 기준으로 딱 한 번만 계산하고, horizon(몇 시간 뒤인지)을
    feature로 모델에 직접 알려준다(history.md 18번 항목 — "horizon을 feature로") —
    재귀적으로 예측값을 다음 스텝에 먹이지 않으므로 horizon이 커져도 오차가 누적되지
    않는다. n_hours개 행을 한 번에 조립해 `predict()`도 한 번만 호출한다.

    날씨는 스칼라(전체 horizon에 재사용, 예보 API 미연동일 때의 기존 단순화)와 길이
    n_hours 시퀀스(horizon별 실제 예보가 있으면 그대로 사용) 둘 다 받는다
    (`_resolve_weather_for_horizon()` 참고). 인구는 `population`을 안 주면 target_ts
    기준 격자 평소 인구로 자동 대체되므로 horizon마다 자동으로 달라진다.

    args:
        station_id: 정류소 ID
        date, hour: "지금(T0)"을 가리키는 날짜/시각
        temp, precip: 날씨 — 스칼라 또는 길이 n_hours 시퀀스. None이면
            Silver `weather_forecast`에서 매 horizon의 target_ts 기준으로 실시간 조회
        population: 생활인구 합계(pop_total). None이면 매 horizon target_ts 기준
            격자 평소 인구(hour, dow 기준이라 horizon마다 달라짐)로 자동 대체
        minute: `predict_rental_demand()` 참고 — 0~59 중 `config.SERVING_TICK_MINUTES`의
            배수 (기본값 0), T0의 운영 앵커
        stockout: 전체 horizon 공통 재고 없음으로 가정할지 (대여 exposure 보정). None이면
            Silver `bike_station_realtime`에서 anchor_ts(T0) 기준 실시간 조회
        n_hours: 몇 개 horizon(1~HORIZON_COUNT)을 예측할지 (1이면 predict_rental/return_demand와 동일)
    returns:
        list[dict]: 길이 n_hours. 각 원소는
            {station_id, date, hour, minute, horizon, rental: {pred_mean/p10/p50/p90,
            lag_fallback_used, lag_data_freshness}, return: {pred_mean/p10/p50/p90},
            population_source, stockout_source("provided"|"fallback" — 실시간 재고
            데이터가 없어 "품절 아님"으로 보수적 기본값을 썼는지, `_stockout_from_status()`
            참고)}
    raises:
        ValueError: station_id가 station_master에 없거나 hour/minute이 범위를 벗어나거나
            n_hours가 1~HORIZON_COUNT 밖이거나 날씨 시퀀스 길이가 n_hours와 다를 때
    """
    master = _get_station_master()
    if station_id not in master.index:
        raise ValueError(f"알 수 없는 station_id: {station_id!r} (최신 보강 station master에 없음)")
    station_row = _validated_station_row(station_id, master.loc[station_id])
    if not (1 <= n_hours <= config.HORIZON_COUNT):
        raise ValueError(f"n_hours는 1~{config.HORIZON_COUNT} 사이여야 함: {n_hours}")

    anchor_ts = _target_timestamp(date, hour, minute)
    lag_features, lag_fallback_fields = _lag_rolling_features(station_id, int(station_row["station_no"]), anchor_ts)
    stockout, stockout_fallback = _resolve_live_stockout(station_id, anchor_ts, stockout)

    records = []
    population_fallbacks = []
    for h in range(1, n_hours + 1):
        target_ts = anchor_ts + pd.Timedelta(hours=h - 1)
        h_temp = None if temp is None else _resolve_weather_for_horizon(temp, h, n_hours, "temp")
        h_precip = None if precip is None else _resolve_weather_for_horizon(precip, h, n_hours, "precip")
        h_temp, h_precip = _resolve_live_weather(target_ts, anchor_ts, h_temp, h_precip)
        target_fields, population_fallback = _build_target_time_fields(
            station_id, station_row, target_ts, h_temp, h_precip, population, stockout, h,
        )
        record = {**target_fields, **lag_features}
        missing = [c for c in _ALL_FEATURE_COLUMNS if c not in record]
        assert not missing, f"feature 누락: {missing}"
        records.append(record)
        population_fallbacks.append(population_fallback)

    batch_df = pd.DataFrame(records).astype(_COMBINED_FEATURE_COLUMN_DTYPES)
    batch_df["rental_exposure"] = batch_df["rental_exposure"].astype(RENTAL_EXPOSURE_DTYPE)
    try:
        rental_batch = predict(batch_df, "rental", exposure_col="rental_exposure")
    finally:
        _release_booster_cache()
    try:
        return_batch = predict(batch_df, "return", exposure_col=None)
    finally:
        _release_booster_cache()

    results = []
    for i, record in enumerate(records):
        rr, rt = rental_batch.iloc[i], return_batch.iloc[i]
        results.append({
            "station_id": station_id,
            "date": record["date"],
            "hour": int(record["hour"]),
            "minute": minute,
            "horizon": int(record["horizon"]),
            "rental": {
                "pred_mean": float(rr["pred_mean"]),
                "pred_p10": float(rr["pred_p10"]),
                "pred_p50": float(rr["pred_p50"]),
                "pred_p90": float(rr["pred_p90"]),
                "lag_fallback_used": lag_fallback_fields,
                "lag_data_freshness": round(1 - len(lag_fallback_fields) / N_LAG_ROLLING_FEATURES, 3),
            },
            "return": {
                "pred_mean": float(rt["pred_mean"]),
                "pred_p10": float(rt["pred_p10"]),
                "pred_p50": float(rt["pred_p50"]),
                "pred_p90": float(rt["pred_p90"]),
            },
            "population_source": "fallback" if population_fallbacks[i] else "provided",
            "stockout_source": "fallback" if stockout_fallback else "provided",
        })

    return results


def predict_demand_multi_hour_all_stations(
    date: str,
    hour: int,
    temp: float | Sequence[float] | None = None,
    precip: float | Sequence[float] | None = None,
    *,
    minute: int = 0,
    station_ids: list[str] | None = None,
    stockout: bool | None = None,
    n_hours: int = 1,
    on_progress=None,
) -> dict:
    """전체(또는 지정한) 정류소를 station×horizon 전체 배치로 묶어서 한 번에 예측한다.

    날씨(temp/precip)는 서울 전체가 관측소 하나를 공유하는 실제 데이터 구조
    (`feature_engine/DATA_CATALOG.md` 1.4절)와 같은 이유로 모든 정류소에 동일하게
    적용한다(horizon별 값은 허용 — `_resolve_weather_for_horizon()` 참고). 인구는
    정류소마다 속한 250m 격자가 달라 하나의 값을 공유할 수 없으므로 항상
    `population=None`(정류소별 격자 평소 인구로 자동 대체)으로 둔다.

    **재귀 없음, lag/rolling은 station당 한 번만 계산**: lag/rolling(직전 실적)은
    horizon과 무관하게 anchor_ts(T0) 기준으로 고정이라(history.md 18번 항목 —
    "horizon을 feature로"), 정류소마다 딱 한 번만 계산해두고 모든 horizon이
    재사용한다 — 예전 재귀 구현이 h마다 반복하던 계산이 통째로 없어졌다. 날씨/
    캘린더/타겟만 horizon마다 다시 만들어 station×horizon 전체를 하나의
    DataFrame으로 모은 뒤 `predict()`를 rental/return 각 한 번씩만 부른다
    (history.md 22번 항목 — 정류소 수만큼 `predict()`를 반복하면 그 고정
    오버헤드만으로 5분 주기 갱신을 못 맞출 정도로 느려졌던 문제의 연장선).

    args:
        date, hour: "지금(T0)" — 전체 정류소에 공통 적용
        temp, precip: 날씨 — 스칼라(전체 정류소·horizon 공통) 또는
            길이 n_hours 시퀀스(horizon별, 전체 정류소 공통). None이면 Silver
            `weather_forecast`에서 매 horizon의 target_ts 기준으로 실시간 조회(서울
            전체가 공유하는 값이라 station마다 다시 조회하지 않고 horizon당 한 번만 조회)
        minute: `predict_rental_demand()` 참고 — 0~59 중 `config.SERVING_TICK_MINUTES`의
            배수 (기본값 0), T0의 운영 앵커
        station_ids: None이면 학습된 모델이 실제로 아는 정류소 전체(아래 참고)
        stockout: 전체 n_hours·전체 정류소에 공통 적용할 값을 직접 줄 때만 사용.
            None(기본값)이면 Silver `bike_station_realtime`에서 anchor_ts(T0) 기준으로
            전체 정류소를 한 번에 조회해 정류소별 실제 재고 현황을 반영한다
        n_hours: 몇 개 horizon(1~HORIZON_COUNT)을 예측할지
        on_progress: (완료된 horizon h, 전체 n_hours)를 받는 콜백 — CLI 진행률
            표시용, None이면 호출 안 함
    returns:
        dict: {
            "results": predict_demand_multi_hour()과 같은 형태의 원소를 정류소별로
                이어붙인 것(각 원소에 station_id가 있어 구분 가능) — 실패한
                station은 전체 horizon이 빠져 있다,
            "failed": [{station_id, date, hour, minute, n_hours_skipped, error}, ...] —
                station_master 조회/lag_rolling 계산이 예외로 실패해 그 station의
                모든 horizon을 건너뛴 항목(원인 포함) — lag/rolling이 station당
                한 번만 계산되므로 실패도 station 단위다,
            "expected_count": len(station_ids) * n_hours,
            "actual_count": len(results),
        }
        `actual_count < expected_count`면 일부 정류소가 조용히 누락된 partial
        결과라는 뜻이다 — Gold 적재 등 downstream은 이 필드로 완결성을 판단해야
        한다("failed"가 비어 있어도 `actual_count`만으로 partial 여부를 알 수
        있게 둘 다 반환한다).
    raises:
        ValueError: n_hours가 1~HORIZON_COUNT 밖이거나 날씨 시퀀스 길이가 n_hours와
            다를 때(station_id 관련 오류는 station 단위로 격리돼 "failed"에 쌓인다)
    """
    master = _get_station_master()
    if station_ids is None:
        # 최신 보강 station master(2,977개)에는 2025년에 트립이 없어 학습 데이터/
        # station_hourly_profile에 아예 없는 정류소가 395개 섞여 있다 — 그 395개는
        # fallback도 없어 매번 NaN + "Mean of empty slice" 경고만 내면서 시간을
        # 낭비한다. 모델이 실제로 학습한 카테고리(load_station_dtype)만 쓴다 —
        # 어차피 학습 안 된 station_id는 예측 자체가 의미 없다. load_station_dtype()의
        # 카테고리는 이제 station_no(정수)라 station_master로 station_id(텍스트)로
        # 되돌려야 이 함수 나머지 부분(전부 station_id 기준)이 그대로 동작한다.
        trained_station_nos = set(load_station_dtype("rental").categories)
        station_nos = pd.to_numeric(master["station_no"], errors="coerce")
        integral_station_nos = station_nos.notna() & np.isfinite(station_nos) & (station_nos % 1 == 0)
        trained = integral_station_nos & station_nos.isin(trained_station_nos)
        # 깨진 station_no는 모델 카테고리와 비교할 수 없지만, 조용히 목록에서
        # 사라지게 두면 완결성 검사가 누락을 알 수 없다. 후보에 포함해 아래
        # station별 검증에서 명시적인 failed로 기록한다.
        station_ids = sorted(master.index[trained | ~integral_station_nos])
    if not (1 <= n_hours <= config.HORIZON_COUNT):
        raise ValueError(f"n_hours는 1~{config.HORIZON_COUNT} 사이여야 함: {n_hours}")

    anchor_ts = _target_timestamp(date, hour, minute)
    failed: list[dict] = []

    # 1) station별 lag/rolling(anchor_ts 기준, horizon과 무관 — 한 번만 계산). 실패하면
    #    그 station의 모든 horizon을 통째로 건너뛴다(호출부가 stderr 로그+반환값
    #    "failed"로 원인을 알 수 있음 — 배치 중 하나가 죽어서 전체가 죽으면 안 됨).
    base_lag_features: dict[str, dict] = {}
    base_fallback: dict[str, list[str]] = {}
    station_rows: dict[str, pd.Series] = {}
    alive_station_ids: list[str] = []
    for sid in station_ids:
        try:
            if sid not in master.index:
                raise ValueError(f"알 수 없는 station_id: {sid!r} (최신 보강 station master에 없음)")
            station_row = _validated_station_row(sid, master.loc[sid])
            lag_features, fb = _lag_rolling_features(
                sid, station_row["station_no"], anchor_ts, skip_rental_recent=True
            )
        except Exception as exc:  # noqa: BLE001 — 정류소 하나 실패로 전체 배치가 죽으면 안 됨(로그로 원인 남김)
            print(f"[predict_demand_multi_hour_all_stations] SKIP station={sid} — {type(exc).__name__}: {exc}", file=sys.stderr)
            failed.append({
                "station_id": sid, "date": date, "hour": hour, "minute": minute,
                "n_hours_skipped": n_hours, "error": f"{type(exc).__name__}: {exc}",
            })
            continue
        base_lag_features[sid] = lag_features
        base_fallback[sid] = fb
        station_rows[sid] = station_row
        alive_station_ids.append(sid)

    expected_count = len(station_ids) * n_hours
    if not alive_station_ids:
        return {"results": [], "failed": failed, "expected_count": expected_count, "actual_count": 0}

    # 2) rental_lag_1h는 살아남은 정류소 전체를 anchor_ts 기준 한 번에 벡터화한다
    #    (history.md 24번 항목 — anchor 축으로 뒤집는 최적화, horizon과 무관하므로
    #    이것도 한 번만).
    station_no_by_id = {sid: row["station_no"] for sid, row in station_rows.items()}
    rental_recent_df, rental_fallback_by_station = _rental_recent_batch(alive_station_ids, station_no_by_id, anchor_ts)
    for sid in alive_station_ids:
        base_lag_features[sid] = {**base_lag_features[sid], **rental_recent_df.loc[sid].to_dict()}
        base_fallback[sid] = base_fallback[sid] + rental_fallback_by_station[sid]

    # stockout을 직접 안 주면 anchor_ts 기준 전체 정류소 재고 현황을 한 번에 조회해
    # (station마다 다시 조회하지 않음 — 설계 원칙 6번) 정류소별로 lookup만 한다.
    bike_status = None if stockout is not None else _get_recent_bike_status(anchor_ts)

    # 3) horizon마다 날씨/캘린더/타겟만 새로 만들어 station×horizon 전체를 한 번에 모은다.
    all_records: list[dict] = []
    row_stations: list[str] = []
    row_horizons: list[int] = []
    population_fallback_by_row: list[bool] = []
    stockout_fallback_by_row: list[bool] = []
    for h in range(1, n_hours + 1):
        target_ts = anchor_ts + pd.Timedelta(hours=h - 1)
        t_temp = None if temp is None else _resolve_weather_for_horizon(temp, h, n_hours, "temp")
        t_precip = None if precip is None else _resolve_weather_for_horizon(precip, h, n_hours, "precip")
        t_temp, t_precip = _resolve_live_weather(target_ts, anchor_ts, t_temp, t_precip)
        for sid in alive_station_ids:
            sid_stockout, sid_stockout_fallback = _stockout_from_status(sid, bike_status, stockout)
            target_fields, population_fallback = _build_target_time_fields(
                sid, station_rows[sid], target_ts, t_temp, t_precip, None, sid_stockout, h,
            )
            all_records.append({**target_fields, **base_lag_features[sid]})
            row_stations.append(sid)
            row_horizons.append(h)
            population_fallback_by_row.append(population_fallback)
            stockout_fallback_by_row.append(sid_stockout_fallback)
        if on_progress is not None:
            on_progress(h, n_hours)

    # dict를 다 모은 뒤 DataFrame 생성/dtype 캐스팅/predict()를 딱 한 번만 한다 —
    # 정류소×horizon마다 반복하면 그 자체가 병목이었다(history.md 23번 항목).
    batch_df = pd.DataFrame(all_records).astype(_COMBINED_FEATURE_COLUMN_DTYPES)
    batch_df["rental_exposure"] = batch_df["rental_exposure"].astype(RENTAL_EXPOSURE_DTYPE)
    try:
        rental_batch = predict(batch_df, "rental", exposure_col="rental_exposure")
    finally:
        # rental/return booster 4개씩을 동시에 캐시에 두면 전체 정류소 배치의 peak
        # memory가 거의 두 배가 되므로 return 모델군을 읽기 전에 먼저 회수한다.
        _release_booster_cache()
    try:
        return_batch = predict(batch_df, "return", exposure_col=None)
    finally:
        # 프로세스 재사용 시 다음 호출이 cached return booster 4개를 안고 rental
        # booster 4개를 추가 로드하지 않도록 실패 여부와 무관하게 종료 뒤에도 비운다.
        _release_booster_cache()

    results = []
    for i, record in enumerate(all_records):
        sid = row_stations[i]
        rr, rt = rental_batch.iloc[i], return_batch.iloc[i]
        fb = base_fallback[sid]
        results.append({
            "station_id": sid,
            "date": record["date"],
            "hour": int(record["hour"]),
            "minute": minute,
            "horizon": row_horizons[i],
            "rental": {
                "pred_mean": float(rr["pred_mean"]), "pred_p10": float(rr["pred_p10"]),
                "pred_p50": float(rr["pred_p50"]), "pred_p90": float(rr["pred_p90"]),
                "lag_fallback_used": fb,
                "lag_data_freshness": round(1 - len(fb) / N_LAG_ROLLING_FEATURES, 3),
            },
            "return": {
                "pred_mean": float(rt["pred_mean"]), "pred_p10": float(rt["pred_p10"]),
                "pred_p50": float(rt["pred_p50"]), "pred_p90": float(rt["pred_p90"]),
            },
            "population_source": "fallback" if population_fallback_by_row[i] else "provided",
            "stockout_source": "fallback" if stockout_fallback_by_row[i] else "provided",
        })

    return {
        "results": results,
        "failed": failed,
        "expected_count": expected_count,
        "actual_count": len(results),
    }


def _resolve_live_weather(
    target_ts: pd.Timestamp, anchor_ts: pd.Timestamp, temp: float | None, precip: float | None
) -> tuple[float, float]:
    """둘 중 하나라도 None이면 Silver 날씨로 나머지도 같이 채운다.

    (둘을 한 번에 같이 조회하는 이유: 실제로 둘 다 안 주고 전부 실시간으로 받는 게
    정상적인 호출 방식이고, 일부만 주는 건 테스트/디버깅용 — 그런 섞어 쓰기에서도
    같은 관측 하나에서 나온 값끼리 일관되게 채워지도록 조회는 항상 한 번만 한다.)

    **관측 vs 예보 분기(2026-08)**: target_ts가 anchor_ts(호출부의 "지금", T0)보다
    미래면(horizon>1) 그 시각의 날씨는 아직 관측되지 않았으므로 예보
    (`_get_forecast_weather()`)를 먼저 시도하고, 예보를 못 찾을 때만 anchor_ts 기준
    관측치(`_get_recent_weather()`, 사실상 "지금 날씨" 재사용)로 대체한다. 미래
    target_ts로 관측을 찾으면 아직 존재하지 않는 S3 키부터 훑어 불필요한 공백이
    생기므로 fallback 기준은 반드시 anchor_ts다. target_ts가
    anchor_ts와 같거나 과거면(horizon=1 또는 수집 지연 재현) 처음부터 관측치만
    쓴다 — 실제 wall-clock(`pd.Timestamp.now()`)을 안 쓰고 항상 호출부가 넘긴
    anchor_ts와 비교하는 이유는, 이 모듈의 다른 함수들처럼 "지금"을 인자로만
    받아 테스트 가능하게 유지하기 위함이다(과거 날짜로도 결정적으로 재현 가능).
    """
    if temp is None or precip is None:
        weather = None
        if target_ts > anchor_ts:
            weather = _get_forecast_weather(target_ts)
        if weather is None:
            weather = _get_recent_weather(anchor_ts)
        temp = weather["temp"] if temp is None else temp
        precip = weather["precip"] if precip is None else precip
    return temp, precip


def _stockout_from_status(station_id: str, status: pd.DataFrame | None, stockout: bool | None) -> tuple[bool, bool]:
    """이미 조회해둔 재고 현황(status)에서 station_id의 stockout 여부를 찾는다.

    조회 자체는 하지 않는다 — `predict_demand_multi_hour_all_stations()`처럼
    여러 station에 같은 배치 조회 결과를 재사용하려는 호출부가 쓴다.

    `population_source`(population_fallback)와 같은 이유로 fallback 여부를 같이
    반환한다 — 재고 정보가 없어 "품절 아님"으로 기본값을 쓴 경우, rental_exposure가
    1.0(정상)으로 들어가 실제로 품절이었을 수도 있는 시간대의 수요를 과대평가하게
    된다. 이 값을 호출부가 반환값에 그대로 실어야(`stockout_source`) 그 왜곡이
    조용히 묻히지 않는다.

    args:
        station_id: 정류소 ID
        status: `_get_recent_bike_status()` 결과, 또는 stockout이 이미 주어져
            조회 자체를 안 했으면 None
        stockout: 호출부가 직접 준 값(주면 그대로 사용 — fallback 아님)
    returns:
        tuple[bool, bool]: (stockout 값, stockout_fallback — status에 station_id가
            없어 보수적 기본값을 썼는지)
    """
    if stockout is not None:
        return stockout, False
    if status is not None and station_id in status.index:
        return bool(status.loc[station_id, "stockout_flag"]), False
    return False, True  # 재고 정보 자체가 없으면 "품절 아님"으로 보수적 기본값


def _resolve_live_stockout(station_id: str, anchor_ts: pd.Timestamp, stockout: bool | None) -> tuple[bool, bool]:
    """stockout이 None이면 anchor_ts 기준 Silver 실시간 재고 현황을 그 자리에서 조회해 대체한다.

    returns:
        tuple[bool, bool]: `_stockout_from_status()`와 동일 — (stockout 값, stockout_fallback)
    """
    if stockout is not None:
        return stockout, False
    return _stockout_from_status(station_id, _get_recent_bike_status(anchor_ts), None)


def predict_rental_demand(
    station_id: str,
    date: str,
    hour: int,
    temp: float | None = None,
    precip: float | None = None,
    population: float | None = None,
    *,
    minute: int = 0,
    horizon: int = 1,
    stockout: bool | None = None,
) -> dict:
    """그 시점의 대여 수요를 예측한다.

    args:
        station_id: 정류소 ID (예: "ST-2000")
        date, hour, minute: "지금(T0)"을 가리키는 날짜/시각/분 — horizon=1(기본값)이면
            바로 이 시각의 수요를 예측하고, horizon>1이면 이 시각을 "지금"으로 두고
            그로부터 (horizon-1)시간 뒤 구간을 예측한다(lag는 항상 T0 기준으로
            고정 — `_build_feature_record()` 참고)
        temp: 기온(°C) — target_ts(T0+(horizon-1)시간) 시점 기준. None이면 Silver
            `weather_forecast`에서 target_ts 기준으로 실시간 조회
        precip: 강수량(mm). None이면 실시간 조회
        population: 그 정류소가 속한 250m 격자의 생활인구 합계(pop_total). None이면
            Silver `living_population_normalized`에서 실시간 조회를 먼저 시도하고,
            그마저 없으면 그 격자의 평소 인구(hour, dow 기준)로 자동 대체된다
        minute: 0~59 중 `config.SERVING_TICK_MINUTES`의 배수 (기본값 0) —
            모델 학습 grid가 20분이어도 point-in-time lag와 실시간 입력은 이 요청
            시각을 정확히 사용한다.
        horizon: 몇 시간 뒤를 예측할지(1~HORIZON_COUNT, 기본값 1). 여러 horizon을
            한 번에 물어보려면(재귀 없이, predict()도 한 번만 호출) 이 함수를 반복
            호출하는 대신 `predict_demand_multi_hour()`를 쓸 것.
        stockout: 그 시각 정류소에 대여 가능한 자전거가 없었으면 True (품절 보정).
            None이면 Silver `bike_station_realtime`에서 anchor_ts(T0) 기준 실시간 조회
    returns:
        dict: station_id, date, hour, minute, horizon, pred_mean, pred_p10, pred_p50,
            pred_p90, lag_fallback_used, lag_data_freshness, population_source,
            stockout_source("provided"|"fallback" — 실시간 재고 데이터가 없어
            "품절 아님"으로 보수적 기본값을 썼는지, `_stockout_from_status()` 참고)
    raises:
        ValueError: station_id가 station_master에 없거나 hour/minute/horizon이
            범위를 벗어날 때(`_build_feature_record()` 참고)
    """
    anchor_ts = _target_timestamp(date, hour, minute)
    target_ts = anchor_ts + pd.Timedelta(hours=horizon - 1)
    temp, precip = _resolve_live_weather(target_ts, anchor_ts, temp, precip)
    stockout, stockout_fallback = _resolve_live_stockout(station_id, anchor_ts, stockout)
    result = _predict_at(
        "rental",
        "rental_exposure",
        station_id=station_id,
        date=date,
        hour=hour,
        minute=minute,
        horizon=horizon,
        temp=temp,
        precip=precip,
        population=population,
        stockout=stockout,
    )
    result["stockout_source"] = "fallback" if stockout_fallback else "provided"
    return result


def predict_return_demand(
    station_id: str,
    date: str,
    hour: int,
    temp: float | None = None,
    precip: float | None = None,
    population: float | None = None,
    *,
    minute: int = 0,
    horizon: int = 1,
) -> dict:
    """그 시점의 반납 수요를 예측한다 (거치대 상태와 무관 — exposure 미적용).

    args:
        station_id: 정류소 ID (예: "ST-2000")
        date, hour: "지금(T0)"을 가리키는 날짜/시각
        temp: 기온(°C) — target_ts(T0+(horizon-1)시간) 시점 기준. None이면
            Silver `weather_forecast`에서 target_ts 기준으로 실시간 조회
        precip: 강수량(mm). None이면 실시간 조회
        population: 그 정류소가 속한 250m 격자의 생활인구 합계(pop_total). None이면
            Silver `living_population_normalized`에서 실시간 조회를 먼저 시도하고,
            그마저 없으면 그 격자의 평소 인구(hour, dow 기준)로 자동 대체된다
        minute: `predict_rental_demand()` 참고 — 0~59 중 `config.SERVING_TICK_MINUTES`의
            배수 (기본값 0)
        horizon: `predict_rental_demand()` 참고 — 1~HORIZON_COUNT (기본값 1)
    returns:
        dict: station_id, date, hour, minute, horizon, pred_mean, pred_p10, pred_p50,
            pred_p90, lag_fallback_used, lag_data_freshness, population_source
    raises:
        ValueError: station_id가 station_master에 없거나 hour/minute/horizon이
            범위를 벗어날 때(`_build_feature_record()` 참고)
    """
    anchor_ts = _target_timestamp(date, hour, minute)
    target_ts = anchor_ts + pd.Timedelta(hours=horizon - 1)
    temp, precip = _resolve_live_weather(target_ts, anchor_ts, temp, precip)
    return _predict_at(
        "return",
        None,
        station_id=station_id,
        date=date,
        minute=minute,
        horizon=horizon,
        hour=hour,
        temp=temp,
        precip=precip,
        population=population,
        stockout=False,
    )


def _parse_weather_arg(raw: str) -> float | list[float]:
    """"20.5" 같은 스칼라 또는 "20,21,21.5,..." 같은 콤마구분 배열을 파싱한다.

    배열이면 `predict_demand_multi_hour[_all_stations]()`가 길이를 `--n-hours`와
    대조해 검증한다(`_resolve_weather_for_horizon()` 참고) — 여기서는 파싱만 한다.
    """
    if "," in raw:
        return [float(v) for v in raw.split(",")]
    return float(raw)


def main(argv: list[str] | None = None) -> None:
    """단일 시점 대여/반납 수요 예측 CLI 엔트리포인트."""
    import argparse
    import json

    parser = argparse.ArgumentParser(description="단일 시점 대여/반납 수요 예측")
    parser.add_argument("--station-id", default=None, help="--all-stations와 동시 사용 불가")
    parser.add_argument(
        "--all-stations", action="store_true",
        help="최신 보강 station master의 전체 정류소를 한 번에 예측(인구는 정류소별 자동 대체, "
        "날씨는 전체 공통) — --n-hours와 함께 쓰면 horizon=1..n_hours를 재귀 없이 한 번에 예측",
    )
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument(
        "--minute", type=int, default=0,
        help="0~59 중 SERVING_TICK_MINUTES의 배수 (기본값 0 — 정시). 모델 학습 "
        "grid가 20분이어도 운영 요청은 5분 간격으로 받는다.",
    )
    parser.add_argument(
        "--horizon", type=int, default=1,
        help="--n-hours를 안 쓸 때(단발 예측) 몇 시간 뒤를 예측할지 (1~HORIZON_COUNT, 기본값 1)",
    )
    parser.add_argument(
        "--temp", type=_parse_weather_arg, default=None,
        help="기온(°C) — 스칼라(전체 horizon 재사용) 또는 --n-hours와 길이가 같은 콤마구분 배열(예: 20,21,21.5). "
        "생략하면 미래 시각은 Silver `weather_short_term_forecast`(예보), 그 외엔 "
        "`weather_ultra_short_live`(관측)에서 실시간 조회(운영 시 실제 호출 방식)",
    )
    parser.add_argument("--precip", type=_parse_weather_arg, default=None, help="강수량(mm) — --temp와 동일 형식·기본값")
    parser.add_argument(
        "--population", type=float, default=None,
        help="생략하면 Silver 실시간 인구를 먼저 시도하고, 없으면 격자 평소 인구로 대체(--all-stations는 항상 자동 대체)",
    )
    stockout_group = parser.add_mutually_exclusive_group()
    stockout_group.add_argument(
        "--stockout", dest="stockout", action="store_true", default=None,
        help="재고 없음으로 강제 고정. 생략하면 Silver `bike_station_realtime`에서 실시간 조회",
    )
    stockout_group.add_argument("--no-stockout", dest="stockout", action="store_false", help="재고 있음으로 강제 고정")
    parser.add_argument(
        "--n-hours", type=int, default=1,
        help="1보다 크면 horizon=1..n_hours를 한 번에 예측한다(predict_demand_multi_hour — "
        "lag/rolling은 T0 기준 한 번만 계산, 재귀 없음)",
    )
    parser.add_argument(
        "--out", default=None,
        help="결과 저장 S3 키(parquet). 미지정시 기본 S3 경로(단일: single_prediction_key, 전체: predictions_key)",
    )
    args = parser.parse_args(argv)

    if bool(args.station_id) == bool(args.all_stations):
        raise SystemExit("--station-id와 --all-stations 중 정확히 하나만 지정해야 합니다.")

    if args.all_stations:
        import time

        start = time.perf_counter()

        def _progress(done: int, total: int) -> None:
            print(f"  {done}/{total} 시간대 완료 ({time.perf_counter() - start:.1f}s)", flush=True)

        outcome = predict_demand_multi_hour_all_stations(
            date=args.date, hour=args.hour, minute=args.minute, temp=args.temp, precip=args.precip,
            stockout=args.stockout, n_hours=args.n_hours, on_progress=_progress,
        )
        elapsed = time.perf_counter() - start
        result, failed = outcome["results"], outcome["failed"]
        print(
            f"전체 {outcome['expected_count']:,}건 기대 / {outcome['actual_count']:,}건 성공"
            f"({len(failed):,}건 실패), {elapsed:.1f}초 소요"
        )

        rows = []
        for r in result:
            rows.append({
                "station_id": r["station_id"], "date": r["date"], "hour": r["hour"], "minute": r["minute"],
                "horizon": r["horizon"],
                "rental_pred_mean": r["rental"]["pred_mean"], "rental_pred_p10": r["rental"]["pred_p10"],
                "rental_pred_p50": r["rental"]["pred_p50"], "rental_pred_p90": r["rental"]["pred_p90"],
                "return_pred_mean": r["return"]["pred_mean"], "return_pred_p10": r["return"]["pred_p10"],
                "return_pred_p50": r["return"]["pred_p50"], "return_pred_p90": r["return"]["pred_p90"],
                "lag_data_freshness": r["rental"]["lag_data_freshness"],
                "population_source": r["population_source"],
                "stockout_source": r["stockout_source"],
            })
        out_df = pd.DataFrame(rows)
        window_start = _target_timestamp(args.date, args.hour, args.minute)
        out_path = args.out or silver_schema.predictions_key(window_start)
        failed_path = (
            silver_schema.predictions_failed_key(window_start)
            if not args.out
            else out_path.removesuffix(".parquet") + "_failed.json"
        )

        # 완전 실패 때 빈 DataFrame으로 같은 tick의 기존 정상 결과를 덮어쓰면 복구할
        # 수 없다. 성공 행이 있을 때만 parquet을 갱신한다. 부분 성공 parquet은 원인
        # 분석용으로 남기되 아래 nonzero 종료로 downstream 적재는 fail-closed한다.
        if outcome["actual_count"] > 0:
            s3_io.write_parquet(out_df, out_path)
            print(f"결과 저장: {out_path}")
        else:
            print(f"성공 결과가 없어 기존 결과를 보존함: {out_path}", file=sys.stderr)

        # 매 실행마다 sidecar를 써야 이전 partial 실행의 stale 실패 목록이 완전 성공
        # 뒤에도 남지 않는다. 완전 성공이면 명시적으로 빈 목록으로 덮어쓴다.
        s3_io.write_json(failed_path, failed)

        incomplete = (
            outcome["actual_count"] == 0
            or outcome["actual_count"] != outcome["expected_count"]
            or bool(failed)
        )
        if incomplete:
            print(f"실패 {len(failed):,}건 목록 저장: {failed_path}", file=sys.stderr)
            # Gold 적재기는 sidecar를 읽지 않으므로 partial을 exit 0으로 통과시키면
            # 누락 행을 정상 결과처럼 적재한다. 진단 산출물은 남기되 비정상 종료해
            # downstream을 막는다.
            raise SystemExit(1)
        raise SystemExit(0)

    common = {
        "station_id": args.station_id,
        "date": args.date,
        "hour": args.hour,
        "minute": args.minute,
        "temp": args.temp,
        "precip": args.precip,
        "population": args.population,
    }
    window_start = _target_timestamp(args.date, args.hour, args.minute)
    out_path = args.out or silver_schema.single_prediction_key(args.station_id, window_start)

    if args.n_hours > 1:
        result = predict_demand_multi_hour(**common, stockout=args.stockout, n_hours=args.n_hours)
        rows = []
        for r in result:
            rows.append({
                "station_id": r["station_id"], "date": r["date"], "hour": r["hour"], "minute": r["minute"],
                "horizon": r["horizon"],
                "rental_pred_mean": r["rental"]["pred_mean"], "rental_pred_p10": r["rental"]["pred_p10"],
                "rental_pred_p50": r["rental"]["pred_p50"], "rental_pred_p90": r["rental"]["pred_p90"],
                "return_pred_mean": r["return"]["pred_mean"], "return_pred_p10": r["return"]["pred_p10"],
                "return_pred_p50": r["return"]["pred_p50"], "return_pred_p90": r["return"]["pred_p90"],
                "lag_data_freshness": r["rental"]["lag_data_freshness"],
                "population_source": r["population_source"],
                "stockout_source": r["stockout_source"],
            })
        out_df = pd.DataFrame(rows)
        s3_io.write_parquet(out_df, out_path)
        print(f"결과 저장: {out_path}", file=sys.stderr)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        raise SystemExit(0)

    rental_res = predict_rental_demand(**common, stockout=args.stockout)
    return_res = predict_return_demand(**common)
    result = {
        "rental": rental_res,
        "return": return_res,
    }
    rows = [{
        "station_id": rental_res["station_id"],
        "date": rental_res["date"],
        "hour": rental_res["hour"],
        "minute": rental_res["minute"],
        "horizon": rental_res["horizon"],
        "rental_pred_mean": rental_res["pred_mean"],
        "rental_pred_p10": rental_res["pred_p10"],
        "rental_pred_p50": rental_res["pred_p50"],
        "rental_pred_p90": rental_res["pred_p90"],
        "return_pred_mean": return_res["pred_mean"],
        "return_pred_p10": return_res["pred_p10"],
        "return_pred_p50": return_res["pred_p50"],
        "return_pred_p90": return_res["pred_p90"],
        "lag_data_freshness": rental_res["lag_data_freshness"],
        "population_source": rental_res["population_source"],
        "stockout_source": rental_res["stockout_source"],
    }]
    out_df = pd.DataFrame(rows)
    s3_io.write_parquet(out_df, out_path)
    print(f"결과 저장: {out_path}", file=sys.stderr)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
