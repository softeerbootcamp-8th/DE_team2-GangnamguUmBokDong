"""단일 시점 입력 기반 수요 예측 모듈.

정류소ID + 날짜/시각 + 날씨를 넣으면 그 시점의 대여/반납 수요(점추정 +
P10/P50/P90)를 반환한다. 생활인구(`population`)는 있으면 넣고, 없으면
(인구 데이터 피드가 끊긴 경우 등) 생략해도 자동으로 대체된다.

    from inference.predict_single import predict_rental_demand
    predict_rental_demand(
        station_id="ST-2000", date="2025-06-01", hour=8,
        temp=22.5, precip=0.0, wind=2.1, humidity=55,
        population=3200,   # 없으면 생략 가능 — 격자 평소 패턴으로 대체됨
    )

설계 메모 — 실제 데이터 수집(날씨/인구 API, 실시간 트립 카운트)은 이 모듈의
책임이 아니다. 다만 모델이 가장 중요하게 쓰는 feature는 lag/rolling(직전
실적)이라, 그걸 계산하려면 "최근 실적 히스토리"가 필요하다. 히스토리 소스는
두 개로 나뉜다: (1) `_get_history_by_station()` — 시간 단위로 이미 집계된
`station_hour_merged_2025.parquet`, 반납(return_count) 전체와 대여의 lag_24h/168h처럼
지연 관측 문제가 없거나 이미 해소된 피처에 쓴다. (2) `_get_rental_events_by_station()` —
트립 단위(start_dt/end_dt) 원본, 대여의 "직전 1시간" 피처(lag_1h, roll_mean/std_3h/24h)에
쓴다 — 대여는 반납이 완료돼야 로그에 잡히는 지연 관측 문제(REALTIME_FEATURES.md)가
있어서, 시간 단위 집계만으로는 그 시점에 실제로 관측 가능했던 값을 재현할 수 없기
때문이다(`rolling_window_features.count_visible_in_window()`로 계산). 실제 서비스로
갈 때는 이 두 함수를 각각 대응하는 실시간 소스(집계 스토어 / 트립 이벤트 버퍼)로
교체하면 나머지 로직은 그대로 재사용된다.

**실시간 데이터가 끊기거나 지연될 때의 동작 (fallback)**: lag/rolling 계산에
필요한 특정 시각의 실적이 히스토리에 없으면, 그 값을 무작정 NaN으로 두지 않고
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

**인구 데이터도 같은 방식으로 대비된다**: `population`을 안 주면
`population_hourly_profile.parquet`(격자×시간×요일별 2025년 평균,
`build_population_profile.py`로 생성)에서 그 정류소가 속한 격자의 평소
인구로 대체한다. 인구 프로필은 **month을 키에 넣지 않는다** — 실측 기준
생활인구는 월별로는 거의 안 변하고(최대/최소 1.05배) 시간대별로만 크게
변해서(1.42배, 출퇴근 패턴), station 프로필처럼 월을 나누면 표본만 줄고
얻는 게 적다. 대체 여부는 반환값의 `population_source`(`"provided"` 또는
`"fallback"`)로 확인할 수 있다.

한계: 정류소/격자 자체가 2025년에 데이터가 없었거나(신규 정류소 등) 프로필도
없는 경우엔 fallback도 NaN이 된다. LightGBM은 결측을 네이티브로 처리하므로
예측은 나오지만 정확도는 더 떨어진다.
"""

import sys

import numpy as np
import pandas as pd
from ml_common.model_contract import (
    FEATURE_COLUMN_DTYPES,
    FEATURE_COLUMNS,
    RENTAL_EXPOSURE_DTYPE,
    load_station_dtype,
)
from ml_common.scoring import predict
from ml_common.trip_events import load_rental_trip_events

from . import config

_history_by_station: dict[str, pd.DataFrame] | None = None
_rental_events_by_station: dict[str, pd.DataFrame] | None = None
_rental_events_sorted_by_station: dict[str, tuple] = {}  # station_id -> (start_dt 정렬된 numpy 배열, 그 순서로 정렬된 end_dt 배열) — _rental_visible_at() 캐시
_all_rental_events_sorted: tuple | None = None  # (station_id 배열, start_dt로 정렬된 배열, 같은 순서의 end_dt 배열) — 전체 정류소 통합, _rental_visible_batch_all_stations() 캐시
_rental_events_coverage: tuple[pd.Timestamp, pd.Timestamp] | None = None
_station_profile: dict[tuple[str, int, int, int], dict[str, float]] | None = None
_population_profile: dict[tuple[str, int, int], dict[str, float]] | None = None
_station_master: pd.DataFrame | None = None
_holidays: set[str] | None = None
_rental_completion_ratio: float | None = None

N_LAG_ROLLING_FEATURES = 2 * (len(config.LAG_HOURS) + 2 * len(config.ROLLING_WINDOWS))  # rental+return 합계


def _get_history_by_station() -> dict[str, pd.DataFrame]:
    """station_id -> (rental_count, return_count)를 hour_ts로 인덱싱한 DataFrame.

    실제 서비스로 갈 때는 이 함수만 실시간 트립 카운트 소스로 교체하면 된다.

    returns:
        dict[str, pd.DataFrame]: station_id별 hour_ts 인덱스 실적 테이블 (모듈 전역에 캐시)
    """
    global _history_by_station
    if _history_by_station is None:
        # hour_ts는 build_merged_table.py가 만든 5분 tick 정밀도 그대로 써야 한다 —
        # date+hour로 재구성하면 분 정보가 날아가 시간당 12개 tick이 전부 같은
        # hour_ts로 뭉개지고, roll_mean/std가 tick 전부를 평균하는 dense 학습 정의
        # (features.py 참고)와 어긋나 train-serve skew가 생긴다.
        df = pd.read_parquet(config.MERGED_TABLE_PARQUET, columns=["station_id", "hour_ts", "rental_count", "return_count"])
        _history_by_station = {
            sid: g.set_index("hour_ts")[["rental_count", "return_count"]].sort_index()
            for sid, g in df.groupby("station_id", sort=False)
        }
    return _history_by_station


def _get_rental_events_by_station() -> dict[str, pd.DataFrame]:
    """station_id -> 트립 단위 대여 이벤트(station_id, start_dt, end_dt), 2025년 전체.

    rolling_window_features.count_visible_in_window()에 그대로 넘길 수 있는 형태다.
    실제 서비스로 갈 때는 이 함수만 실시간 트립 이벤트 버퍼(최근 window+embargo 분량)로
    교체하면 된다 — _get_history_by_station()과는 별개의 교체 지점.

    returns:
        dict[str, pd.DataFrame]: station_id별 (station_id, start_dt, end_dt) 이벤트
            (모듈 전역에 캐시). 커버리지 범위(최소/최대 start_dt)는
            _rental_events_coverage 전역에 함께 캐시된다.
    """
    global _rental_events_by_station, _rental_events_coverage, _all_rental_events_sorted
    if _rental_events_by_station is None:
        trips = load_rental_trip_events(verbose=False)
        _rental_events_coverage = (trips["start_dt"].min(), trips["start_dt"].max())
        _rental_events_by_station = {
            sid: g[["station_id", "start_dt", "end_dt"]].reset_index(drop=True)
            for sid, g in trips.groupby("station_id", sort=False)
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

    각 anchor가 요구하는 윈도우(`[anchor-90분, anchor-30분)`)가 로드된 트립 데이터의
    커버리지(2025년 전체) 밖이면 NaN(데이터 없음 — 호출부가 fallback으로 판단),
    커버리지 안이면 실제 카운트(트립 0건도 유효한 관측값이라 NaN이 아님)를 채운다.

    **왜 `count_visible_in_window()`를 anchor마다 그대로 안 부르는가**: 그 함수는
    "소량의 최근 이벤트 버퍼"를 전제로 설계돼 있어서, anchor 하나당 그 station의
    (여기서는 연간 전체) 트립을 매번 다시 boolean mask로 스캔한다 — `roll_mean_24h`
    하나에만 anchor 288개(5분 tick 기준)가 필요하고, `predict_demand_multi_hour()`의
    재귀 스텝도 anchor 수십 개를 필요로 해서, anchor마다 전체 재스캔은 station 1개
    예측에도 초 단위로 느렸다(실측: 정류소 1개×12시간 재귀 예측에 약 2초 —
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
    events = _get_rental_events_by_station().get(station_id)
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
    events_by_station = _get_rental_events_by_station()  # _all_rental_events_sorted 캐시를 이 호출이 채워둠
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
    """station_master.parquet을 station_id 인덱스로 캐시해 반환한다.

    returns:
        pd.DataFrame: station_id로 인덱싱된 정류소 마스터 (capacity, lat, lon 등)
    """
    global _station_master
    if _station_master is None:
        _station_master = pd.read_parquet(config.STATION_MASTER_PARQUET).set_index("station_id")
    return _station_master


def _get_holidays() -> set[str]:
    """2025년 공휴일 목록을 캐시해 반환한다.

    returns:
        set[str]: 'YYYY-MM-DD' 형식의 2025년 공휴일 집합
    """
    global _holidays
    if _holidays is None:
        _holidays = config.load_holidays_2025()
    return _holidays


def _get_station_profile() -> dict[tuple[str, int, int, int], dict[str, float]]:
    """station_hourly_profile.parquet을 (station_id, hour, dow, month) 키의 dict로 캐시해 반환한다.

    month을 키에 포함하는 이유: 계절에 따라 대여량 자체가 크게 달라져서
    (실측 1월 대비 6월 약 2.44배), station x hour x dow로만 묶으면
    1월 결측과 6월 결측이 똑같은 연간 평균으로 채워지는 문제가 생긴다.

    returns:
        dict[tuple[str, int, int, int], dict[str, float]]: (station_id, hour, dow, month) ->
            {rental_mean, rental_std, return_mean, return_std}
    """
    global _station_profile
    if _station_profile is None:
        df = pd.read_parquet(config.STATION_HOURLY_PROFILE_PARQUET)
        _station_profile = {
            (r.station_id, r.hour, r.dow, r.month): {
                "rental_mean": r.rental_mean,
                "rental_std": r.rental_std,
                "return_mean": r.return_mean,
                "return_std": r.return_std,
            }
            for r in df.itertuples()
        }
    return _station_profile


def _profile_stat(station_id: str, ts: pd.Timestamp, stat_key: str) -> float:
    """특정 시각(ts)의 (hour, dow, month)에 해당하는 station 평소 패턴 통계값을 조회한다.

    args:
        station_id: 정류소 ID
        ts: 조회할 시각 (hour/dayofweek/month를 사용 — month으로 계절성 반영)
        stat_key: "rental_mean" / "rental_std" / "return_mean" / "return_std" 중 하나
    returns:
        float: 프로필 값. 해당 station의 프로필이 아예 없으면 NaN
    """
    entry = _get_station_profile().get((station_id, ts.hour, ts.dayofweek, ts.month))
    return entry[stat_key] if entry is not None else np.nan


def _get_population_profile() -> dict[tuple[str, int, int], dict[str, float]]:
    """population_hourly_profile.parquet을 (grid_id, hour, dow) 키의 dict로 캐시해 반환한다.

    station 프로필과 달리 month을 키에 넣지 않는다 — 생활인구는 월별 변동이
    미미하고(1.05배) 시간대별 변동이 지배적이라(1.42배, 출퇴근 패턴) month을
    추가해도 얻는 게 적고 표본만 station 프로필처럼 줄어든다.

    returns:
        dict[tuple[str, int, int], dict[str, float]]: (grid_id, hour, dow) ->
            {pop_resd_mean, pop_long_foreign_mean, pop_short_foreign_mean, pop_total_mean}
    """
    global _population_profile
    if _population_profile is None:
        df = pd.read_parquet(config.POPULATION_HOURLY_PROFILE_PARQUET)
        _population_profile = {
            (r.grid_id, r.hour, r.dow): {
                "pop_resd_mean": r.pop_resd_mean,
                "pop_long_foreign_mean": r.pop_long_foreign_mean,
                "pop_short_foreign_mean": r.pop_short_foreign_mean,
                "pop_total_mean": r.pop_total_mean,
            }
            for r in df.itertuples()
        }
    return _population_profile


def _population_fallback(grid_id: str, ts: pd.Timestamp) -> dict[str, float]:
    """population 인자가 없을 때 그 격자의 평소 인구(hour, dow 기준)로 대체한다.

    args:
        grid_id: 정류소가 속한 250m 격자 ID
        ts: 예측하려는 시각 (hour/dayofweek만 사용)
    returns:
        dict[str, float]: pop_resd, pop_long_foreign, pop_short_foreign, pop_total
            (프로필이 없으면 전부 NaN)
    """
    entry = _get_population_profile().get((grid_id, ts.hour, ts.dayofweek))
    if entry is None:
        return {"pop_resd": np.nan, "pop_long_foreign": np.nan, "pop_short_foreign": np.nan, "pop_total": np.nan}
    return {
        "pop_resd": entry["pop_resd_mean"],
        "pop_long_foreign": entry["pop_long_foreign_mean"],
        "pop_short_foreign": entry["pop_short_foreign_mean"],
        "pop_total": entry["pop_total_mean"],
    }


def _lag_rolling_features(
    station_id: str, target_ts: pd.Timestamp, skip_rental_recent: bool = False
) -> tuple[dict[str, float], list[str]]:
    """실시간 히스토리에서 lag/rolling을 계산하되, 없는 값은 station 평소 패턴(profile)으로 대체한다.

    반납(return, 7개 전부)과 대여의 lag_24h/168h는 시간 단위 집계 히스토리
    (_get_history_by_station)로 그대로 계산한다 — 지연 관측 문제가 없거나(반납) 예측
    시점엔 이미 완전히 해소된 값(24h/168h 전)이기 때문이다. 대여의 "직전 1시간" 4개
    (lag_1h, roll_mean/std_3h/24h)는 _censored_rental_recent()가 트립 단위 원본으로
    따로 계산한다 (features.py의 _rental_visible/_add_rental_lag_rolling과 같은 원칙 —
    배치/실시간 두 경로로 나뉘어 있을 뿐. 자세한 배경은 REALTIME_FEATURES.md).

    args:
        station_id: 정류소 ID
        target_ts: 예측하려는 시각
        skip_rental_recent: True면 `_censored_rental_recent()`(트립 단위 dense
            조회, 이 시각 하나에도 anchor 300여 개 스캔이 필요해 비쌈)를 안
            부르고 그 5개(rental_lag_1h, roll_mean/std_3h/24h)를 NaN placeholder로
            둔다 — `predict_demand_multi_hour()`가 h>=2에서 이 값을 어차피
            `_recursive_lag_rolling_features()`의 결과로 덮어쓰므로, 미리 비싼
            계산을 할 필요가 없을 때만 쓴다.
    returns:
        tuple[dict[str, float], list[str]]: (14개 lag/rolling feature dict, fallback을
            쓴 feature 이름 목록 — 비어있으면 전부 실시간 데이터를 그대로 썼다는 뜻)
    """
    history = _get_history_by_station().get(station_id)
    out: dict[str, float] = {}
    fallback_fields: list[str] = []

    for count_col, prefix in [("rental_count", "rental"), ("return_count", "return")]:
        series = history[count_col] if history is not None else pd.Series(dtype=float)
        mean_key, std_key = f"{prefix}_mean", f"{prefix}_std"

        for lag in config.LAG_HOURS:
            if prefix == "rental" and lag == 1:
                continue  # _censored_rental_recent()가 트립 단위로 대체 계산 (LAG_HOURS[0]==1 가정)
            ts = target_ts - pd.Timedelta(hours=lag)
            val = series.get(ts, np.nan)
            if pd.isna(val):
                val = _profile_stat(station_id, ts, mean_key)
                fallback_fields.append(f"{prefix}_lag_{lag}h")
            out[f"{prefix}_lag_{lag}h"] = val

        if prefix == "rental":
            continue  # rolling도 _censored_rental_recent()가 대체 계산

        for window in config.ROLLING_WINDOWS:
            # [target_ts-window, target_ts) 안의 5분 tick 전부 평균한다 — features.py의
            # dense 정의(hourly 지점 몇 개가 아니라 윈도우 안 모든 tick)와 반드시 같아야
            # train-serve skew가 안 생긴다.
            idx = pd.date_range(
                target_ts - pd.Timedelta(hours=window), target_ts, freq=f"{config.GRID_TICK_MINUTES}min", inclusive="left"
            )
            vals = series.reindex(idx)  # 일부만 있어도 skipna로 자연스럽게 처리됨

            mean_val = vals.mean()
            if pd.isna(mean_val):  # 윈도우 전체가 비어있을 때만 fallback
                mean_val = float(np.nanmean([_profile_stat(station_id, t, mean_key) for t in idx]))
                fallback_fields.append(f"{prefix}_roll_mean_{window}h")
            out[f"{prefix}_roll_mean_{window}h"] = mean_val

            std_val = vals.std()
            if pd.isna(std_val):  # 표본이 0~1개라 표준편차를 못 구할 때도 포함
                std_val = float(np.nanmean([_profile_stat(station_id, t, std_key) for t in idx]))
                fallback_fields.append(f"{prefix}_roll_std_{window}h")
            out[f"{prefix}_roll_std_{window}h"] = std_val

    if skip_rental_recent:
        for key in ("rental_lag_1h", "rental_roll_mean_3h", "rental_roll_std_3h", "rental_roll_mean_24h", "rental_roll_std_24h"):
            out[key] = np.nan  # 호출부(predict_demand_multi_hour)가 곧 실제 값으로 덮어씀
    else:
        _censored_rental_recent(station_id, target_ts, out, fallback_fields)

    return out, fallback_fields


def _censored_rental_recent(
    station_id: str, target_ts: pd.Timestamp, out: dict[str, float], fallback_fields: list[str]
) -> None:
    """대여의 "직전 1시간" 피처(lag_1h, roll_mean/std_3h/24h) 4개를 point-in-time censored 값으로 채운다.

    대여는 반납이 완료돼야 로그에 잡히므로, 시간 단위 집계 히스토리로는 실제 서빙
    시점에 관측 가능했던 값을 재현할 수 없다 — 트립 단위 이벤트에
    rolling_window_features.count_visible_in_window()를 직접 적용한다.
    `rental_lag_1h`는 target_ts 자체에서 계산한 값(추가 shift 불필요 — 이미
    [target_ts-90분, target_ts-30분) 이전 정보만 씀), roll_mean/std_{window}h는
    (target_ts-window, target_ts] 안의 5분 tick 전부(anchor)를 평균/표준편차한다 —
    features.py의 `_add_rental_lag_rolling`이 배치로 계산하는 dense 정의와 반드시 같아야
    train-serve skew가 안 생긴다.

    args:
        station_id: 정류소 ID
        target_ts: 예측하려는 시각
        out: 채워질 feature dict (in-place)
        fallback_fields: fallback 쓴 필드 이름이 append되는 리스트 (in-place)
    """
    mean_key, std_key = "rental_mean", "rental_std"

    visible_now = _rental_visible_at(station_id, [target_ts]).iloc[0]
    if pd.isna(visible_now):
        visible_now = _profile_stat(station_id, target_ts, mean_key)
        fallback_fields.append("rental_lag_1h")
    out["rental_lag_1h"] = visible_now

    for window in config.ROLLING_WINDOWS:
        # (target_ts-window, target_ts] 안의 5분 tick 전부 — features.py의 dense 정의와 동일.
        anchors = pd.date_range(
            target_ts - pd.Timedelta(hours=window) + pd.Timedelta(minutes=config.GRID_TICK_MINUTES),
            target_ts,
            freq=f"{config.GRID_TICK_MINUTES}min",
        )
        vals = _rental_visible_at(station_id, anchors)

        mean_val = vals.mean()
        if pd.isna(mean_val):  # anchor 전체가 데이터 커버리지 밖일 때만 fallback
            mean_val = float(np.nanmean([_profile_stat(station_id, t, mean_key) for t in anchors]))
            fallback_fields.append(f"rental_roll_mean_{window}h")
        out[f"rental_roll_mean_{window}h"] = mean_val

        std_val = vals.std()
        if pd.isna(std_val):  # 표본이 0~1개라 표준편차를 못 구할 때도 포함
            std_val = float(np.nanmean([_profile_stat(station_id, t, std_key) for t in anchors]))
            fallback_fields.append(f"rental_roll_std_{window}h")
        out[f"rental_roll_std_{window}h"] = std_val


def _build_feature_record(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    population: float | None,
    pop_resd: float | None,
    pop_long_foreign: float,
    pop_short_foreign: float,
    stockout: bool,
    skip_rental_recent: bool = False,
) -> tuple[dict, list[str], bool]:
    """예측 1건에 필요한 feature 값을 dict로 조립한다(DataFrame 생성/dtype 캐스팅은 안 함).

    `_build_feature_row()`(단일 정류소용, 이 함수를 감싸서 1행 DataFrame으로 반환)와
    `predict_demand_multi_hour_all_stations()`(정류소 수천 개용, 이 함수로 dict만
    모아뒀다가 마지막에 한 번만 DataFrame을 만들고 캐스팅)가 같이 쓴다 — DataFrame
    생성+dtype 캐스팅을 정류소마다 반복하면(원래 `_build_feature_row`가 그랬음)
    그 자체가 병목이 되기 때문(history.md 23번 항목 참고).

    args/returns: `_build_feature_row()`와 동일한 의미, 반환은 (record dict,
        fallback_fields, population_fallback) 3개.
    raises:
        ValueError: station_id가 station_master에 없거나 hour가 0~23 범위를 벗어날 때
    """
    master = _get_station_master()
    if station_id not in master.index:
        raise ValueError(f"알 수 없는 station_id: {station_id!r} (station_master.parquet에 없음)")
    station_row = master.loc[station_id]

    if not (0 <= hour <= 23):
        raise ValueError(f"hour는 0~23 사이여야 함: {hour}")

    target_ts = pd.Timestamp(date) + pd.Timedelta(hours=hour)

    population_fallback = population is None
    if population_fallback:
        pop_values = _population_fallback(station_row["grid_id"], target_ts)
        pop_resd = pop_values["pop_resd"]
        pop_long_foreign = pop_values["pop_long_foreign"]
        pop_short_foreign = pop_values["pop_short_foreign"]
        pop_total = pop_values["pop_total"]
    else:
        # 세부 국적별 인구를 안 주면 population(총합)을 내국인으로 간주 — 원 모델은
        # 4개 컬럼(pop_resd/long/short/total)을 쓰지만 이 인터페이스는 "인구 수" 한 값만
        # 받는 걸 기본으로 하고, 필요하면 세부 breakdown을 옵션으로 줄 수 있게 열어둠.
        if pop_resd is None:
            pop_resd = max(population - pop_long_foreign - pop_short_foreign, 0.0)
        pop_total = pop_resd + pop_long_foreign + pop_short_foreign

    lag_features, fallback_fields = _lag_rolling_features(station_id, target_ts, skip_rental_recent=skip_rental_recent)

    dow, month = target_ts.dayofweek, target_ts.month
    holidays = _get_holidays()
    next_date = (target_ts + pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    prev_date = (target_ts - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    record = {
        "station_id": station_id,
        "capacity": float(station_row["capacity"]),
        "lat": float(station_row["lat"]),
        "lon": float(station_row["lon"]),
        "temp": temp,
        "precip": precip,
        "wind": wind,
        "humidity": humidity,
        "pop_resd": pop_resd,
        "pop_long_foreign": pop_long_foreign,
        "pop_short_foreign": pop_short_foreign,
        "pop_total": pop_total,
        "hour": hour,
        "dow": dow,
        "month": month,
        "is_holiday": int(date in holidays),
        "is_weekend": int(dow >= 5),
        "is_next_day_off": int(next_date in holidays or (dow + 1) % 7 >= 5),
        "is_prev_day_off": int(prev_date in holidays or (dow + 6) % 7 >= 5),
        "hour_sin": np.sin(2 * np.pi * hour / 24),
        "hour_cos": np.cos(2 * np.pi * hour / 24),
        "dow_sin": np.sin(2 * np.pi * dow / 7),
        "dow_cos": np.cos(2 * np.pi * dow / 7),
        "rental_exposure": config.EXPOSURE_STOCKOUT_VALUE if stockout else 1.0,
        "date": date,
        **lag_features,
    }

    missing = [c for c in FEATURE_COLUMNS if c not in record]
    assert not missing, f"feature 누락: {missing}"  # BASE_FEATURE_COLUMNS/LAG_ROLLING과 안 맞으면 여기서 바로 발견됨

    return record, fallback_fields, population_fallback


def _build_feature_row(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    population: float | None,
    pop_resd: float | None,
    pop_long_foreign: float,
    pop_short_foreign: float,
    stockout: bool,
    skip_rental_recent: bool = False,
) -> pd.DataFrame:
    """예측 1건에 필요한 feature 1행짜리 DataFrame을 조립한다.

    args:
        station_id: 정류소 ID
        date: "YYYY-MM-DD"
        hour: 0~23
        temp: 기온(°C)
        precip: 강수량(mm)
        wind: 풍속(m/s)
        humidity: 습도(%)
        population: 그 정류소가 속한 250m 격자의 생활인구 합계. None이면 격자
            평소 인구(population_hourly_profile)로 대체
        pop_resd: 내국인 인구를 따로 줄 때 (None이면 population에서 역산)
        pop_long_foreign: 장기체류외국인 인구
        pop_short_foreign: 단기체류외국인 인구
        stockout: 그 시각 대여 가능한 자전거가 없었는지 여부
        skip_rental_recent: `_lag_rolling_features()` 참고 — True면 rental의
            "직전 실적" 5개를 비싼 실시간 조회 없이 NaN으로 두고, 호출부가
            바로 덮어쓸 걸 전제한다(predict_demand_multi_hour의 h>=2 전용 최적화)
    returns:
        pd.DataFrame: train_common.FEATURE_COLUMNS를 모두 포함하는 1행 DataFrame.
            attrs["fallback_fields"]/attrs["population_fallback"]에 fallback
            사용 여부가 담김
    raises:
        ValueError: station_id가 station_master에 없거나 hour가 0~23 범위를 벗어날 때
    """
    record, fallback_fields, population_fallback = _build_feature_record(
        station_id, date, hour, temp, precip, wind, humidity,
        population, pop_resd, pop_long_foreign, pop_short_foreign, stockout, skip_rental_recent,
    )
    df = pd.DataFrame([record])
    df.attrs["fallback_fields"] = fallback_fields
    df.attrs["population_fallback"] = population_fallback

    # Python 스칼라로 조립한 행이라 기본 float64/int64로 들어와 있다 — 학습 데이터
    # (feature_engineering이 다운캐스트한 float32/int8/int16, ml_common.model_contract.FEATURE_COLUMN_DTYPES)와
    # dtype을 맞춘다. 값 자체는 바뀌지 않지만(LightGBM은 어차피 내부적으로 캐스팅해서
    # 예측 결과에 영향 없음) 학습/서빙 스키마가 정확히 일치해야 한다는 이 프로젝트의
    # 원칙(model_contract.py 모듈 docstring)을 dtype까지 지키기 위함.
    df = df.astype(FEATURE_COLUMN_DTYPES)
    df["rental_exposure"] = df["rental_exposure"].astype(RENTAL_EXPOSURE_DTYPE)
    return df


def _predict_at(model_name: str, exposure_col: str | None, **kwargs) -> dict:
    """feature 행을 만들고 예측한 뒤, fallback 정보를 포함한 결과 dict를 만든다.

    args:
        model_name: "rental" 또는 "return"
        exposure_col: predict()에 전달할 exposure 컬럼명 (반납은 None)
        **kwargs: _build_feature_row()에 그대로 전달할 인자
    returns:
        dict: station_id, date, hour, pred_mean, pred_p10, pred_p50, pred_p90,
            lag_fallback_used, lag_data_freshness, population_source
    """
    df = _build_feature_row(**kwargs)
    fallback_fields = df.attrs.get("fallback_fields", [])
    population_fallback = df.attrs.get("population_fallback", False)
    pred = predict(df, model_name, exposure_col=exposure_col)
    row = pred.iloc[0]
    return {
        "station_id": row["station_id"],
        "date": row["date"],
        "hour": int(row["hour"]),
        "pred_mean": float(row["pred_mean"]),
        "pred_p10": float(row["pred_p10"]),
        "pred_p50": float(row["pred_p50"]),
        "pred_p90": float(row["pred_p90"]),
        "lag_fallback_used": fallback_fields,
        "lag_data_freshness": round(1 - len(fallback_fields) / N_LAG_ROLLING_FEATURES, 3),
        "population_source": "fallback" if population_fallback else "provided",
    }


RECURSIVE_FEATURE_KEYS = [
    f"{prefix}_{suffix}"
    for prefix in ("rental", "return")
    for suffix in ("lag_1h", "roll_mean_3h", "roll_std_3h", "roll_mean_24h", "roll_std_24h")
]  # predict_demand_multi_hour()가 재귀적으로 덮어쓰는 10개 — lag_24h/168h는 항상 실측이라 제외


def _get_rental_completion_ratio(n_samples: int = 300) -> float:
    """재귀 예측이 쓸 "완료율" 보정 계수를 현재 window/embargo 설정 기준으로 실측 추정한다.

    **문제**: `rental_count`(모델 학습 타겟, `feature_engineering/build_targets.py`의
    `future_rolling_counts()`)는 "그 시각부터 앞으로 1시간(`[T,T+60min)`)"의
    완결된 건수다. 재귀 스텝은 이전 스텝의 예측(`pred_mean`, 이 완결 건수의
    추정치)을 다음 스텝의 `rental_lag_1h`(실제로는 window=60/embargo=30분 기준
    `[T-90분,T-30분)` 구간에서 **T 시점까지 반납 완료된 것만** 세는 값) 자리에
    그대로 꽂아넣는데, 이 둘은 정의가 다르다 — 완결 건수를 "아직 다 안 끝난
    것처럼 보이는" 값 자리에 넣으면 실제보다 과대평가된 lag를 모델에 주게 된다.

    **보정**: `[T-60분,T)`(직전 스텝 예측이 커버하는 구간과 가장 가까운 실측
    윈도우)의 완결 건수 대비, 그 구간을 `_rental_visible_at()`(window=60/embargo=30
    적용)로 봤을 때 실제로 보이는 비율을 무작위 (station, 시각) 표본으로
    추정한다. 표본 하나씩 비율을 내서 평균내면(표본이 작을 때 나눗셈이 극단으로
    튐 — REALTIME_FEATURES.md의 "관측값÷완료율 보정 기각 사유"와 같은 문제) 잘못된
    추정이 나올 수 있어, `sum(관측)/sum(완결)`(비율의 평균이 아니라 합의 비율)로
    노이즈를 줄인다.

    **알려진 한계**: `[T-60,T)`와 실제 lag가 보는 `[T-90,T-30)`은 30분 어긋나
    있어(재귀 예측이 실제로 갖고 있는 값이 `[T-60,T)`뿐이라 어쩔 수 없음) 이
    보정도 근사치다 — 완료율(반납 지연) 효과만 보정하고, 30분 시간축 어긋남은
    그대로 남는다. 그래도 아예 안 보정하는 것보다는 낫다는 판단.

    args:
        n_samples: 비율 추정에 쓸 (station, 시각) 표본 수
    returns:
        float: rental_lag_1h 자리에 넣을 때 예측값(완결 건수 추정치)에 곱할 배수.
            표본이 부족하면 안전하게 1.0(보정 없음)을 반환
    """
    global _rental_completion_ratio
    if _rental_completion_ratio is not None:
        return _rental_completion_ratio

    history = _get_history_by_station()
    station_ids = [sid for sid, s in history.items() if not s.empty]
    if not station_ids:
        _rental_completion_ratio = 1.0
        return _rental_completion_ratio

    rng = np.random.RandomState(0)
    sampled_stations = rng.choice(station_ids, size=min(n_samples, len(station_ids) * 4), replace=True)

    true_sum, censored_sum = 0.0, 0.0
    for sid in sampled_stations:
        series = history[sid]["rental_count"]
        if len(series) < 2:
            continue
        ts = series.index[rng.randint(1, len(series))]  # index 0은 ts-1h 조회가 범위 밖일 수 있어 제외
        true_val = series.get(ts - pd.Timedelta(hours=1), np.nan)  # [ts-60분,ts) 완결 건수
        if pd.isna(true_val) or true_val <= 0:
            continue
        censored_val = _rental_visible_at(sid, [ts]).iloc[0]  # 같은 ts를 window/embargo로 본 값
        if pd.isna(censored_val):
            continue
        true_sum += float(true_val)
        censored_sum += float(censored_val)

    if true_sum <= 0:
        _rental_completion_ratio = 1.0  # 표본 부족 -> 안전하게 보정 없음
    else:
        _rental_completion_ratio = float(np.clip(censored_sum / true_sum, 0.05, 1.0))
    return _rental_completion_ratio


def _recursive_lag_rolling_features(
    station_id: str, target_ts: pd.Timestamp, synthetic: dict[pd.Timestamp, dict[str, float]]
) -> tuple[dict[str, float], list[str]]:
    """RECURSIVE_FEATURE_KEYS(직전 실적 계열 10개)를 1시간 간격 점 표본으로 근사 계산한다.

    `predict_demand_multi_hour()`가 두 번째 시간대(h>=2)부터 쓴다 — 그 시각들은
    아직 일어나지 않은 미래라 실측 데이터가 없으므로, 이전 스텝에서 이미 예측한
    값(`synthetic`)을 "직전 실적"처럼 재귀적으로 사용한다. 값이 필요한 시각이
    (1) 이전 스텝의 예측값(`synthetic`)에 있으면 그걸, (2) 없으면 실측 데이터를,
    (3) 그마저 없으면 `_profile_stat()`(정류소 평소 패턴)을 쓴다.

    **알려진 한계(의도적 단순화 — 정확도보다 구현 속도 우선)**: (1) 5분 tick
    dense 평균이 아니라 1시간 간격 점 표본으로 rolling을 근사한다. (2) 예측값을
    실측처럼 재사용하므로 horizon이 커질수록 오차가 누적된다 — history.md 18번
    항목에서 이미 검토 후 기각됐던 방식과 같은 한계다. 더 정확한 대안(horizon을
    feature로 추가해 재귀 없이 예측)은 `training/experiments/multi_horizon/`에
    이미 구현·검증돼 있으니, 정확도가 중요해지면 그쪽으로 교체할 것.

    args:
        station_id: 정류소 ID
        target_ts: 예측하려는 시각 (T0 + h시간, h>=2)
        synthetic: 이전 스텝들에서 이미 예측한 {시각: {rental_count, return_count}}
    returns:
        tuple[dict[str, float], list[str]]: (10개 feature 값, 그래도 profile
            fallback을 쓴 항목 이름 목록)
    """
    history = _get_history_by_station().get(station_id)

    # 이 스텝에서 필요한 모든 시각(lag_1h + 각 rolling window의 시간별 점)을 모아
    # rental 실측 조회를 한 번에 배치로 한다(_rental_visible_at()이 anchor 여러
    # 개를 한 번에 받아 station별 정렬+searchsorted로 처리 — docstring 참고).
    offsets = {1} | {k for window in config.ROLLING_WINDOWS for k in range(1, window + 1)}
    needed_ts = [target_ts - pd.Timedelta(hours=k) for k in offsets if (target_ts - pd.Timedelta(hours=k)) not in synthetic]
    rental_real = _rental_visible_at(station_id, needed_ts) if needed_ts else pd.Series(dtype=float)

    def point_value(prefix: str, ts: pd.Timestamp) -> float:
        entry = synthetic.get(ts)
        if entry is not None:
            val = entry[f"{prefix}_count"]
            if prefix == "rental":
                # entry는 이전 스텝의 완결 건수 예측치 — rental_lag_1h 자리는 원래
                # window/embargo로 아직 다 안 보이는 값이 들어가야 하므로 완료율만큼
                # 줄여서 넣는다(_get_rental_completion_ratio() docstring 참고).
                # return은 지연 관측 문제가 없어 보정하지 않는다.
                val *= _get_rental_completion_ratio()
            return val
        if prefix == "rental":
            return rental_real.get(ts, np.nan)
        return history[f"{prefix}_count"].get(ts, np.nan) if history is not None else np.nan

    out: dict[str, float] = {}
    fallback_fields: list[str] = []

    for prefix in ("rental", "return"):
        mean_key, std_key = f"{prefix}_mean", f"{prefix}_std"

        lag1_ts = target_ts - pd.Timedelta(hours=1)
        val = point_value(prefix, lag1_ts)
        if pd.isna(val):
            val = _profile_stat(station_id, lag1_ts, mean_key)
            fallback_fields.append(f"{prefix}_lag_1h")
        out[f"{prefix}_lag_1h"] = val

        for window in config.ROLLING_WINDOWS:
            pts = [target_ts - pd.Timedelta(hours=k) for k in range(1, window + 1)]
            vals = np.array([point_value(prefix, t) for t in pts], dtype=float)

            if np.all(np.isnan(vals)):
                out[f"{prefix}_roll_mean_{window}h"] = float(np.nanmean([_profile_stat(station_id, t, mean_key) for t in pts]))
                fallback_fields.append(f"{prefix}_roll_mean_{window}h")
            else:
                out[f"{prefix}_roll_mean_{window}h"] = float(np.nanmean(vals))

            if np.sum(~np.isnan(vals)) < 2:  # 표본 0~1개면 표준편차를 못 구함
                out[f"{prefix}_roll_std_{window}h"] = float(np.nanmean([_profile_stat(station_id, t, std_key) for t in pts]))
                fallback_fields.append(f"{prefix}_roll_std_{window}h")
            else:
                out[f"{prefix}_roll_std_{window}h"] = float(np.nanstd(vals, ddof=1))

    return out, fallback_fields


def _recursive_return_features(
    station_id: str, target_ts: pd.Timestamp, synthetic: dict[pd.Timestamp, dict[str, float]]
) -> tuple[dict[str, float], list[str]]:
    """RECURSIVE_FEATURE_KEYS 중 return_* 5개만 재귀적으로 계산한다.

    `_recursive_lag_rolling_features()`의 "return" 분기와 완전히 동일한 로직이다
    — rental은 `_rental_recent_batch()`가 전체 정류소를 한 번에 벡터화해서 따로
    처리하므로(history.md 24번 항목, 축을 station→anchor로 뒤집는 최적화) 이
    함수에서는 뺐다. return은 지연 관측 문제가 없어(REALTIME_FEATURES.md) 트립
    단위 재계산이 필요 없고 시간 단위 집계 dict 조회(O(1))만 하면 되므로,
    `_rental_visible_at()`처럼 비쌌던 부분이 원래 없다 — 정류소별로 이 작은
    계산을 유지해도 벡터화할 만큼의 이득이 없다.

    args/returns: `_recursive_lag_rolling_features()`와 동일(단, return_* 5개만)
    """
    history = _get_history_by_station().get(station_id)

    def point_value(ts: pd.Timestamp) -> float:
        entry = synthetic.get(ts)
        if entry is not None:
            return entry["return_count"]
        return history["return_count"].get(ts, np.nan) if history is not None else np.nan

    out: dict[str, float] = {}
    fallback_fields: list[str] = []
    mean_key, std_key = "return_mean", "return_std"

    lag1_ts = target_ts - pd.Timedelta(hours=1)
    val = point_value(lag1_ts)
    if pd.isna(val):
        val = _profile_stat(station_id, lag1_ts, mean_key)
        fallback_fields.append("return_lag_1h")
    out["return_lag_1h"] = val

    for window in config.ROLLING_WINDOWS:
        pts = [target_ts - pd.Timedelta(hours=k) for k in range(1, window + 1)]
        vals = np.array([point_value(t) for t in pts], dtype=float)

        mean_val = float(np.nanmean(vals)) if not np.all(np.isnan(vals)) else np.nan
        if pd.isna(mean_val):
            mean_val = float(np.nanmean([_profile_stat(station_id, t, mean_key) for t in pts]))
            fallback_fields.append(f"return_roll_mean_{window}h")
        out[f"return_roll_mean_{window}h"] = mean_val

        std_val = float(np.nanstd(vals, ddof=1)) if np.sum(~np.isnan(vals)) >= 2 else np.nan
        if pd.isna(std_val):
            std_val = float(np.nanmean([_profile_stat(station_id, t, std_key) for t in pts]))
            fallback_fields.append(f"return_roll_std_{window}h")
        out[f"return_roll_std_{window}h"] = std_val

    return out, fallback_fields


def _rental_recent_batch(
    station_ids: list[str],
    target_ts: pd.Timestamp,
    synthetic_rental: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """전체 정류소의 rental "직전 실적" 5개(lag_1h, roll_mean/std_3h/24h)를 한 번에 계산한다.

    `_censored_rental_recent()`(h=1, 5분 tick dense)와 `_recursive_lag_rolling_features()`의
    rental 분기(h>=2, 1시간 점 표본)를 정류소 축이 아니라 anchor 축으로 벡터화한
    버전 — `predict_demand_multi_hour_all_stations()`가 정류소마다 이 계산을
    반복하던 것(history.md 22/23번 항목 최적화 이후에도 여전히 병목의 상당수)을
    `_rental_visible_batch_all_stations()`로 anchor 개수만큼만 반복하도록
    바꿨다(24번 항목, 패러다임 전환 — station-outer/anchor-inner를
    anchor-outer/station-vectorized로 뒤집음).

    `synthetic_rental`이 비어 있으면(컬럼 0개) h=1로 간주해 5분 tick dense
    anchor를 쓰고, 아니면 h>=2로 간주해 1시간 간격 점 표본을 쓴다 — 값이 필요한
    시각이 (1) `synthetic_rental`에 있으면(완료율 보정 이미 적용된 상태로 호출부가
    채워둠) 그걸, (2) 없으면 실측(`_rental_visible_batch_all_stations()`)을,
    (3) 그마저 없으면 정류소별 `_profile_stat()` fallback을 쓴다 — 기존 두
    함수와 동일한 우선순위.

    args:
        station_ids: 대상 정류소 목록
        target_ts: 이번 시간대(전체 정류소 공통 "지금+h시간")
        synthetic_rental: index=station_id, columns=이전 스텝들의 target_ts,
            값=완료율 보정된 예측 rental_count(호출부가 이미 보정해서 넘김).
            컬럼이 없으면(h=1) 전부 실측으로 채운다.
    returns:
        tuple[pd.DataFrame, dict[str, list[str]]]: (index=station_id,
            columns=rental_lag_1h/roll_mean_3h/roll_std_3h/roll_mean_24h/roll_std_24h,
            station_id -> profile fallback을 쓴 항목 이름 목록)
    """
    is_dense = synthetic_rental.shape[1] == 0  # h==1이면 이전 예측이 아직 없음

    if is_dense:
        tick = pd.Timedelta(minutes=config.GRID_TICK_MINUTES)
        lag1_ts = target_ts
        window_anchors = {
            window: list(pd.date_range(target_ts - pd.Timedelta(hours=window) + tick, target_ts, freq=tick))
            for window in config.ROLLING_WINDOWS
        }
    else:
        lag1_ts = target_ts - pd.Timedelta(hours=1)
        window_anchors = {
            window: [target_ts - pd.Timedelta(hours=k) for k in range(1, window + 1)]
            for window in config.ROLLING_WINDOWS
        }

    all_anchors = sorted({lag1_ts} | {a for anchors in window_anchors.values() for a in anchors})
    real_df = pd.DataFrame(_rental_visible_batch_all_stations(station_ids, all_anchors))

    station_index = pd.Index(station_ids, name="station_id")
    if is_dense:
        combined = real_df
    else:
        syn = synthetic_rental.reindex(index=station_index, columns=all_anchors)
        combined = syn.where(syn.notna(), real_df)  # synthetic 우선, 없으면 실측

    out = pd.DataFrame(index=station_index)
    fallback: dict[str, list[str]] = {sid: [] for sid in station_ids}
    mean_key, std_key = "rental_mean", "rental_std"

    lag1_vals = combined[lag1_ts].copy()
    missing = lag1_vals.isna()
    for sid in station_index[missing]:
        lag1_vals.loc[sid] = _profile_stat(sid, lag1_ts, mean_key)
        fallback[sid].append("rental_lag_1h")
    out["rental_lag_1h"] = lag1_vals

    for window, anchors in window_anchors.items():
        sub = combined[anchors]

        mean_vals = sub.mean(axis=1)  # skipna 기본 True
        missing_mean = mean_vals.isna()
        for sid in station_index[missing_mean]:
            mean_vals.loc[sid] = float(np.nanmean([_profile_stat(sid, t, mean_key) for t in anchors]))
            fallback[sid].append(f"rental_roll_mean_{window}h")
        out[f"rental_roll_mean_{window}h"] = mean_vals

        std_vals = sub.std(axis=1)  # ddof=1 기본값, 표본 0~1개면 NaN(원본과 동일)
        missing_std = std_vals.isna()
        for sid in station_index[missing_std]:
            std_vals.loc[sid] = float(np.nanmean([_profile_stat(sid, t, std_key) for t in anchors]))
            fallback[sid].append(f"rental_roll_std_{window}h")
        out[f"rental_roll_std_{window}h"] = std_vals

    return out, fallback


def predict_demand_multi_hour(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    population: float | None = None,
    *,
    pop_resd: float | None = None,
    pop_long_foreign: float = 0.0,
    pop_short_foreign: float = 0.0,
    stockout: bool = False,
    n_hours: int = 1,
) -> list[dict]:
    """(date, hour)를 "지금(T0)"으로 놓고, 1시간 뒤부터 n_hours시간 뒤까지 1시간 간격으로 예측한다.

    두 번째 시간대(h=2)부터는 바로 이전 스텝에서 예측한 값을 그 다음 스텝의
    "직전 실적"(lag_1h, roll_mean/std_3h/24h)으로 재귀적으로 사용한다
    (`_recursive_lag_rolling_features()` 참고) — 미래 시점이라 실측이 없어
    어쩔 수 없이 예측값을 대신 쓰는 것이며, 그래서 horizon이 커질수록 오차가
    누적되는 한계가 있다(정확도보다 구현 속도를 우선한 의도적 선택 — 더 정확한
    대안은 `training/experiments/multi_horizon/` 참고). 날씨/인구는 시간마다
    다시 관측/예보되는 게 아니라 호출 시점에 준 값을 n_hours 내내 그대로
    재사용한다(예보 API 미연동, 알려진 단순화).

    args:
        station_id: 정류소 ID
        date, hour: "지금(T0)"을 가리키는 날짜/시각 — 예측은 이 다음 시각부터 시작
        temp, precip, wind, humidity: 날씨 (n_hours 내내 동일 값 재사용)
        population: 생활인구 합계. None이면 매 시간 격자 평소 인구(hour, dow
            기준이라 시간마다 달라짐)로 자동 대체
        pop_resd, pop_long_foreign, pop_short_foreign: 인구 세부 breakdown
        stockout: 전체 n_hours 동안 재고 없음으로 가정할지 (대여 exposure 보정)
        n_hours: 몇 시간 뒤까지 예측할지 (1이면 사실상 predict_rental/return_demand와 동일)
    returns:
        list[dict]: 길이 n_hours. 각 원소는
            {station_id, date, hour, rental: {pred_mean/p10/p50/p90,
            lag_fallback_used, lag_data_freshness}, return: {pred_mean/p10/p50/p90},
            population_source}
    """
    base_ts = pd.Timestamp(date) + pd.Timedelta(hours=hour)
    synthetic: dict[pd.Timestamp, dict[str, float]] = {}
    results = []

    for h in range(1, n_hours + 1):
        target_ts = base_ts + pd.Timedelta(hours=h)
        t_date, t_hour = target_ts.strftime("%Y-%m-%d"), int(target_ts.hour)

        df = _build_feature_row(
            station_id=station_id,
            date=t_date,
            hour=t_hour,
            temp=temp,
            precip=precip,
            wind=wind,
            humidity=humidity,
            population=population,
            pop_resd=pop_resd,
            pop_long_foreign=pop_long_foreign,
            pop_short_foreign=pop_short_foreign,
            stockout=stockout,
            skip_rental_recent=(h >= 2),  # h>=2는 아래서 재귀 계산으로 덮어쓰므로 여기서 비싼 계산을 안 함
        )
        fallback_fields = list(df.attrs.get("fallback_fields", []))

        if h >= 2:  # h==1의 lag_1h 원본 시각은 target_ts-1h == base_ts(지금) — 이미 실측 있음
            # h==1의 원래 fallback_fields 중 RECURSIVE_FEATURE_KEYS 10개는 이제
            # _recursive_lag_rolling_features()의 재귀 계산으로 대체되므로 빼고,
            # 그 계산이 새로 판단한 fallback만 다시 채운다.
            fallback_fields = [f for f in fallback_fields if f not in RECURSIVE_FEATURE_KEYS]
            recursive_vals, recursive_fallback = _recursive_lag_rolling_features(station_id, target_ts, synthetic)
            for key, val in recursive_vals.items():
                df[key] = np.float32(val)  # 대입 시 float64로 되돌아가지 않게 dtype 유지
            fallback_fields += recursive_fallback

        population_fallback = df.attrs.get("population_fallback", False)

        rental_row = predict(df, "rental", exposure_col="rental_exposure").iloc[0]
        return_row = predict(df, "return", exposure_col=None).iloc[0]

        results.append({
            "station_id": station_id,
            "date": t_date,
            "hour": t_hour,
            "rental": {
                "pred_mean": float(rental_row["pred_mean"]),
                "pred_p10": float(rental_row["pred_p10"]),
                "pred_p50": float(rental_row["pred_p50"]),
                "pred_p90": float(rental_row["pred_p90"]),
                "lag_fallback_used": fallback_fields,
                "lag_data_freshness": round(1 - len(fallback_fields) / N_LAG_ROLLING_FEATURES, 3),
            },
            "return": {
                "pred_mean": float(return_row["pred_mean"]),
                "pred_p10": float(return_row["pred_p10"]),
                "pred_p50": float(return_row["pred_p50"]),
                "pred_p90": float(return_row["pred_p90"]),
            },
            "population_source": "fallback" if population_fallback else "provided",
        })

        synthetic[target_ts] = {
            "rental_count": float(rental_row["pred_mean"]),
            "return_count": float(return_row["pred_mean"]),
        }

    return results


def predict_demand_multi_hour_all_stations(
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    *,
    station_ids: list[str] | None = None,
    stockout: bool = False,
    n_hours: int = 1,
    on_progress=None,
) -> list[dict]:
    """전체(또는 지정한) 정류소를 시간(h)마다 배치로 묶어서 한 번에 예측한다.

    날씨(temp/precip/wind/humidity)는 서울 전체가 관측소 하나를 공유하는
    실제 데이터 구조(`feature_engineering/DATA_CATALOG.md` 1.4절)와 같은 이유로 모든
    정류소에 동일하게 적용한다. 인구는 정류소마다 속한 250m 격자가 달라
    하나의 값을 공유할 수 없으므로 항상 `population=None`(정류소별 격자 평소
    인구로 자동 대체)으로 둔다.

    **왜 정류소별로 순차 호출하지 않는가**: 처음엔 `predict_demand_multi_hour()`를
    정류소마다 그대로 호출하는 루프였는데, `ml_common.scoring.predict()`가 호출당
    LightGBM `booster.predict()`를 8번(rental/return x poisson/q10/q50/q90) 부르고
    이 고정 오버헤드가 정류소 수(2,582개)만큼 쌓이면서 전체 실행이 5분 주기
    갱신에 못 맞을 정도로 느려졌다(history.md 22번 항목 — 정류소 1개도 이미
    벡터화 전에는 병목이었음). **시간(h)마다 전체 정류소를 한 DataFrame으로
    모아 `predict()`를 딱 한 번만 부르면** 이 오버헤드가 정류소 수와 무관하게
    h(최대 12)번만 발생한다 — feature 조립 자체는 정류소별로 그대로 하되
    (`_build_feature_row()`/`_recursive_lag_rolling_features()`, 검증된 로직
    그대로 재사용), 모델 채점만 배치로 묶는다.

    args:
        date, hour: "지금(T0)" — 전체 정류소에 공통 적용
        temp, precip, wind, humidity: 날씨 (전체 정류소 공통, n_hours 내내 재사용)
        station_ids: None이면 학습된 모델이 실제로 아는 정류소 전체(아래 참고)
        stockout: 전체 n_hours·전체 정류소에 공통 적용
        n_hours: 몇 시간 뒤까지 예측할지
        on_progress: (완료된 시간 h, 전체 n_hours)를 받는 콜백 — CLI 진행률
            표시용, None이면 호출 안 함
    returns:
        list[dict]: predict_demand_multi_hour()과 같은 형태의 원소를 정류소별로
            이어붙인 것(길이 = len(station_ids) * n_hours) — 각 원소에
            station_id가 있어 구분 가능
    """
    if station_ids is None:
        # station_master.parquet(2,977개)에는 2025년에 트립이 없어 학습 데이터/
        # station_hourly_profile에 아예 없는 정류소가 395개 섞여 있다 — 그 395개는
        # fallback도 없어 매번 NaN + "Mean of empty slice" 경고만 내면서 시간을
        # 낭비한다. 모델이 실제로 학습한 카테고리(load_station_dtype)만 쓴다 —
        # 어차피 학습 안 된 station_id는 예측 자체가 의미 없다.
        station_ids = sorted(load_station_dtype("rental").categories)

    base_ts = pd.Timestamp(date) + pd.Timedelta(hours=hour)
    synthetic: dict[str, dict[pd.Timestamp, dict[str, float]]] = {sid: {} for sid in station_ids}
    results_by_station: dict[str, list[dict]] = {sid: [] for sid in station_ids}
    station_index = pd.Index(station_ids, name="station_id")

    for h in range(1, n_hours + 1):
        target_ts = base_ts + pd.Timedelta(hours=h)
        t_date, t_hour = target_ts.strftime("%Y-%m-%d"), int(target_ts.hour)

        # rental "직전 실적" 5개는 정류소별로 반복하지 않고 전체를 한 번에
        # 벡터화해서 미리 계산해둔다(history.md 24번 항목 — anchor 축으로 뒤집는
        # 최적화, 정류소 수와 무관하게 anchor 개수만큼만 반복). synthetic에 쌓인
        # 값은 완결 건수 그대로라, 여기서 완료율을 곱해 point_value()와 동일한
        # 보정을 적용한다.
        ratio = _get_rental_completion_ratio() if h >= 2 else None
        if h == 1:
            synthetic_rental_df = pd.DataFrame(index=station_index)
        else:
            synthetic_rental_df = pd.DataFrame(
                {sid: {ts: v["rental_count"] * ratio for ts, v in synthetic[sid].items()} for sid in station_ids}
            ).T
            synthetic_rental_df.index.name = "station_id"
        rental_recent_df, rental_fallback_by_station = _rental_recent_batch(station_ids, target_ts, synthetic_rental_df)

        records = []
        batch_station_ids: list[str] = []  # 이번 시간대에 실제로 성공한 정류소만(실패분 제외)
        fallback_by_station: dict[str, list[str]] = {}
        population_fallback_by_station: dict[str, bool] = {}
        for sid in station_ids:
            try:
                record, fb, population_fallback = _build_feature_record(
                    station_id=sid, date=t_date, hour=t_hour,
                    temp=temp, precip=precip, wind=wind, humidity=humidity,
                    population=None, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0,
                    stockout=stockout, skip_rental_recent=True,
                )
                # skip_rental_recent=True라 fb엔 rental_* 5개가 원래 없다. h>=2일
                # 때만 return_*의 (재귀 아닌) 원래 fallback을 지운다 — 곧
                # _recursive_return_features()가 재귀 버전으로 다시 채운다.
                # h==1은 return이 재귀 없이 그대로 맞으므로 건드리지 않는다.
                if h >= 2:
                    fb = [f for f in fb if f not in RECURSIVE_FEATURE_KEYS]
                record.update(rental_recent_df.loc[sid].to_dict())
                fb += rental_fallback_by_station[sid]
                if h >= 2:
                    return_vals, return_fallback = _recursive_return_features(sid, target_ts, synthetic[sid])
                    record.update(return_vals)
                    fb += return_fallback
            except Exception as exc:  # noqa: BLE001 — 배치 중 한 정류소 실패로 전체가 죽으면 안 됨(아래 로그로 원인 남김)
                # 재시도하지 않고 그 자리에서 건너뛴다 — 다만 조용히 넘어가면 "왜
                # 이 정류소가 결과에 없는지" 나중에 알 방법이 없어지므로 눈에 띄게
                # 남긴다. 이 station은 다음 시간대(h+1)에서는 다시 시도된다(직전
                # 실패를 계속 물고 가지 않음 — synthetic[sid]에 이번 시간대 값이
                # 없으면 다음 스텝은 그냥 profile fallback으로 넘어감).
                print(
                    f"[predict_demand_multi_hour_all_stations] SKIP station={sid} h={h} "
                    f"({t_date} {t_hour}시) — {type(exc).__name__}: {exc}",
                    file=sys.stderr,
                )
                continue
            fallback_by_station[sid] = fb
            population_fallback_by_station[sid] = population_fallback
            records.append(record)
            batch_station_ids.append(sid)

        if not records:  # 이번 시간대에 전 정류소가 다 실패한 극단적인 경우 — 조회할 배치가 없음
            if on_progress is not None:
                on_progress(h, n_hours)
            continue

        # dict를 다 모은 뒤 DataFrame 생성/dtype 캐스팅을 여기서 딱 한 번만 한다 —
        # 정류소마다(_build_feature_row였다면 매번) 반복하면 그 자체가 병목이었다
        # (history.md 23번 항목, .astype()가 전체 실행 시간의 절반을 먹었었음).
        batch_df = pd.DataFrame(records).astype(FEATURE_COLUMN_DTYPES)
        batch_df["rental_exposure"] = batch_df["rental_exposure"].astype(RENTAL_EXPOSURE_DTYPE)
        rental_batch = predict(batch_df, "rental", exposure_col="rental_exposure")
        return_batch = predict(batch_df, "return", exposure_col=None)

        for i, sid in enumerate(batch_station_ids):
            rr, rt = rental_batch.iloc[i], return_batch.iloc[i]
            fb = fallback_by_station[sid]
            results_by_station[sid].append({
                "station_id": sid,
                "date": t_date,
                "hour": t_hour,
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
                "population_source": "fallback" if population_fallback_by_station[sid] else "provided",
            })
            synthetic[sid][target_ts] = {
                "rental_count": float(rr["pred_mean"]),
                "return_count": float(rt["pred_mean"]),
            }

        if on_progress is not None:
            on_progress(h, n_hours)

    results = []
    for sid in station_ids:
        results.extend(results_by_station[sid])
    return results


def predict_rental_demand(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    population: float | None = None,
    *,
    pop_resd: float | None = None,
    pop_long_foreign: float = 0.0,
    pop_short_foreign: float = 0.0,
    stockout: bool = False,
) -> dict:
    """그 시점의 대여 수요를 예측한다.

    args:
        station_id: 정류소 ID (예: "ST-2000")
        date: "YYYY-MM-DD"
        hour: 0~23
        temp: 기온(°C)
        precip: 강수량(mm)
        wind: 풍속(m/s)
        humidity: 습도(%)
        population: 그 정류소가 속한 250m 격자의 생활인구 합계. 인구 데이터
            피드가 끊겼으면 생략(None) — 그 격자의 평소 인구(hour, dow 기준)로
            자동 대체된다
        pop_resd: 내국인 인구를 세부적으로 줄 때 (기본값은 population에서 역산)
        pop_long_foreign: 장기체류외국인 인구
        pop_short_foreign: 단기체류외국인 인구
        stockout: 그 시각 정류소에 대여 가능한 자전거가 없었으면 True (품절 보정)
    returns:
        dict: station_id, date, hour, pred_mean, pred_p10, pred_p50, pred_p90,
            lag_fallback_used, lag_data_freshness, population_source
    raises:
        ValueError: station_id가 station_master에 없거나 hour가 0~23 범위를 벗어날 때
    """
    return _predict_at(
        "rental",
        "rental_exposure",
        station_id=station_id,
        date=date,
        hour=hour,
        temp=temp,
        precip=precip,
        wind=wind,
        humidity=humidity,
        population=population,
        pop_resd=pop_resd,
        pop_long_foreign=pop_long_foreign,
        pop_short_foreign=pop_short_foreign,
        stockout=stockout,
    )


def predict_return_demand(
    station_id: str,
    date: str,
    hour: int,
    temp: float,
    precip: float,
    wind: float,
    humidity: float,
    population: float | None = None,
    *,
    pop_resd: float | None = None,
    pop_long_foreign: float = 0.0,
    pop_short_foreign: float = 0.0,
) -> dict:
    """그 시점의 반납 수요를 예측한다 (거치대 상태와 무관 — exposure 미적용).

    args:
        station_id: 정류소 ID (예: "ST-2000")
        date: "YYYY-MM-DD"
        hour: 0~23
        temp: 기온(°C)
        precip: 강수량(mm)
        wind: 풍속(m/s)
        humidity: 습도(%)
        population: 그 정류소가 속한 250m 격자의 생활인구 합계. 인구 데이터
            피드가 끊겼으면 생략(None) — 그 격자의 평소 인구(hour, dow 기준)로
            자동 대체된다
        pop_resd: 내국인 인구를 세부적으로 줄 때 (기본값은 population에서 역산)
        pop_long_foreign: 장기체류외국인 인구
        pop_short_foreign: 단기체류외국인 인구
    returns:
        dict: station_id, date, hour, pred_mean, pred_p10, pred_p50, pred_p90,
            lag_fallback_used, lag_data_freshness, population_source
    raises:
        ValueError: station_id가 station_master에 없거나 hour가 0~23 범위를 벗어날 때
    """
    return _predict_at(
        "return",
        None,
        station_id=station_id,
        date=date,
        hour=hour,
        temp=temp,
        precip=precip,
        wind=wind,
        humidity=humidity,
        population=population,
        pop_resd=pop_resd,
        pop_long_foreign=pop_long_foreign,
        pop_short_foreign=pop_short_foreign,
        stockout=False,
    )


if __name__ == "__main__":
    import argparse
    import json

    parser = argparse.ArgumentParser(description="단일 시점 대여/반납 수요 예측")
    parser.add_argument("--station-id", default=None, help="--all-stations와 동시 사용 불가")
    parser.add_argument(
        "--all-stations", action="store_true",
        help="station_master.parquet의 전체 정류소를 한 번에 예측(인구는 정류소별 자동 대체, "
        "날씨는 전체 공통) — --n-hours와 함께 쓰면 정류소마다 n시간씩 재귀 예측",
    )
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--hour", type=int, required=True)
    parser.add_argument("--temp", type=float, required=True)
    parser.add_argument("--precip", type=float, required=True)
    parser.add_argument("--wind", type=float, required=True)
    parser.add_argument("--humidity", type=float, required=True)
    parser.add_argument("--population", type=float, default=None, help="생략하면 격자 평소 인구로 대체(--all-stations는 항상 자동 대체)")
    parser.add_argument("--stockout", action="store_true")
    parser.add_argument(
        "--n-hours", type=int, default=1,
        help="1보다 크면 --hour 다음 시각부터 n시간 뒤까지 1시간 간격으로 재귀 예측 "
        "(predict_demand_multi_hour, 오차 누적 한계는 함수 docstring 참고)",
    )
    parser.add_argument("--out", default=None, help="--all-stations 결과 저장 경로(parquet). 미지정시 기본 경로")
    args = parser.parse_args()

    if bool(args.station_id) == bool(args.all_stations):
        raise SystemExit("--station-id와 --all-stations 중 정확히 하나만 지정해야 합니다.")

    if args.all_stations:
        import time

        start = time.perf_counter()

        def _progress(done: int, total: int) -> None:
            print(f"  {done}/{total} 시간대 완료 ({time.perf_counter() - start:.1f}s)", flush=True)

        result = predict_demand_multi_hour_all_stations(
            date=args.date, hour=args.hour, temp=args.temp, precip=args.precip,
            wind=args.wind, humidity=args.humidity, stockout=args.stockout,
            n_hours=args.n_hours, on_progress=_progress,
        )
        elapsed = time.perf_counter() - start
        n_stations = len(result) // args.n_hours
        print(f"전체 {n_stations:,}개 정류소 x {args.n_hours}시간 = {len(result):,}행, {elapsed:.1f}초 소요")

        rows = []
        for r in result:
            rows.append({
                "station_id": r["station_id"], "date": r["date"], "hour": r["hour"],
                "rental_pred_mean": r["rental"]["pred_mean"], "rental_pred_p10": r["rental"]["pred_p10"],
                "rental_pred_p50": r["rental"]["pred_p50"], "rental_pred_p90": r["rental"]["pred_p90"],
                "return_pred_mean": r["return"]["pred_mean"], "return_pred_p10": r["return"]["pred_p10"],
                "return_pred_p50": r["return"]["pred_p50"], "return_pred_p90": r["return"]["pred_p90"],
                "lag_data_freshness": r["rental"]["lag_data_freshness"],
                "population_source": r["population_source"],
            })
        out_df = pd.DataFrame(rows)
        out_path = args.out or f"inference_multi_hour_{args.date.replace('-', '')}_{args.hour:02d}.parquet"
        out_df.to_parquet(out_path, index=False)
        print(f"결과 저장: {out_path}")
        raise SystemExit(0)

    common = {
        "station_id": args.station_id,
        "date": args.date,
        "hour": args.hour,
        "temp": args.temp,
        "precip": args.precip,
        "wind": args.wind,
        "humidity": args.humidity,
        "population": args.population,
    }
    if args.n_hours > 1:
        result = predict_demand_multi_hour(**common, stockout=args.stockout, n_hours=args.n_hours)
        print(json.dumps(result, indent=2, ensure_ascii=False))
        raise SystemExit(0)

    result = {
        "rental": predict_rental_demand(**common, stockout=args.stockout),
        "return": predict_return_demand(**common),
    }
    print(json.dumps(result, indent=2, ensure_ascii=False))
