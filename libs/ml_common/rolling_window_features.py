"""point-in-time(관측 시점 기준) 정합 대여 rolling window 카운트.

**배경**: 실시간 서빙에서는 대여 트립 하나가 "반납 완료"돼야 로그에 잡힌다
(대여 시작 시점엔 아직 로그에 안 잡힘). 그래서 예측 기준 시점 T에서 "최근
한 시간 대여량" 같은 롤링 피처를 집계하면, 그 구간 동안 대여가 시작됐지만
아직 반납되지 않은 트립은 실제로는 존재하는데도 관측 카운트에서 빠진다 —
이게 시간이 지날수록(반납이 뒤늦게 로그에 반영되며) 관측치가 슬금슬금
올라가는 우측 절단(right-censoring) 구조다.

과거 이력으로 학습 데이터를 만들 때는 이미 몇 달~몇 년이 지나 트립이 전부
반납 완료된 상태라 이 문제가 안 보인다. 하지만 그렇게 만든 학습 데이터로
모델을 학습하면, 서빙 시점엔 항상 절단된(과소집계된) 값을 보게 되는
모델이라 **train-serving skew**가 생긴다.

**해법**: 보정(관측값 ÷ 완료율)은 완료율 자체의 표준편차가 커서(특히 절단이
심한 최근 구간) 나눗셈이 오차를 증폭시키므로 채택하지 않았다. 대신 학습
데이터를 만들 때도 서빙과 동일하게 "그 시점에 실제로 관측 가능했던 값"만
쓰도록 **의도적으로 가린다(censoring)** — 그러면 모델이 이 절단 패턴 자체를
학습 과정에서 자연스럽게 배운다.

**윈도우 설계 — "5분 단위"는 윈도우 폭이 아니라 갱신 주기다.** 서빙이 5분
틱마다 값을 다시 계산한다는 뜻이지, 각 계산이 보는 구간이 5분이어야 한다는
뜻이 아니다. 실제로 최신 5분(`[T-5,T)`)만 보면 버킷이 닫히는 순간 완료율이
4% 수준이라 사실상 노이즈다. 대신 **폭은 넓게(예: 1시간), 가장 최신 구간은
embargo로 건너뛴다**:

    윈도우 = [T - embargo - width, T - embargo)
    포함 조건: start_ts가 이 구간 안에 있고, end_ts가 결측이 아니며 end_ts <= T

기본값(config.py)은 `width=60분, embargo=30분` — "30분 전부터 1시간 30분
전까지"를 본다. 폭을 넓혀 표본을 늘리고, 가장 최신(완료율 낮은) 30분을
건너뛰어 신호 대 잡음비를 높인다. `embargo=0, width=tick`으로 두면 예전의
"버킷이 닫히는 순간" 방식과 동일해진다 (단위 테스트가 이 특수 케이스로
핵심 규칙을 검증한다).

핵심 규칙(학습·서빙 양쪽이 반드시 동일하게 지켜야 함)은 위 "포함 조건"
한 줄이다. 대용량 배치 계산(학습 데이터 생성)과 소규모 실시간 계산(서빙)
양쪽에서 이 파일의 함수를 그대로 재사용해야 한다 — 서빙 쪽 로직이 별도
언어/스택이라 이 파일을 직접 import할 수 없다면, 최소한 이 규칙을 코드
리뷰에서 대조해 어긋나지 않는지 확인해야 한다 (REALTIME_FEATURES.md 참고).
"""

import numpy as np
import pandas as pd


def floor_to_window(ts: pd.Series, window_minutes: int) -> pd.Series:
    """타임스탬프를 window_minutes 단위로 내림한다 (버킷 시작 시각).

    args:
        ts: 타임스탬프 Series (datetime64)
        window_minutes: 버킷 크기(분)
    returns:
        pd.Series: window_minutes 단위로 내림된 타임스탬프
    """
    freq = f"{window_minutes}min"
    return ts.dt.floor(freq)


def add_censored_visibility(
    trips: pd.DataFrame,
    window_minutes: int = 5,
    start_col: str = "start_dt",
    end_col: str = "end_dt",
) -> pd.DataFrame:
    """트립마다 (버킷 시작/종료, 그 버킷이 닫히는 순간 관측 가능했는지)를 붙인다.

    embargo=0인 특수 케이스(T=버킷 종료 시각) 전용 — 핵심 규칙을 가장 단순한
    형태로 보여주는 단위 테스트/개념 설명용이다. 실제 학습 feature 생성에는
    embargo를 두는 `censored_rolling_counts()`를 쓴다.

    args:
        trips: 최소한 start_col, end_col을 포함하는 트립 단위 DataFrame
            (반납 미완료 트립은 end_col이 NaT여도 됨)
        window_minutes: 버킷 크기(분), 기본 5분
        start_col: 대여 시작 시각 컬럼명
        end_col: 반납 완료 시각 컬럼명 (NaT 허용 — 아직 반납 안 된 트립)
    returns:
        pd.DataFrame: 원본에 `bucket_start`, `bucket_end`, `visible_at_close`
            3개 컬럼이 추가된 복사본
    """
    out = trips.copy()
    out["bucket_start"] = floor_to_window(out[start_col], window_minutes)
    out["bucket_end"] = out["bucket_start"] + pd.Timedelta(minutes=window_minutes)
    out["visible_at_close"] = out[end_col].notna() & (out[end_col] <= out["bucket_end"])
    return out


def count_visible_in_window(
    events: pd.DataFrame,
    as_of: pd.Timestamp,
    window_minutes: int = 60,
    embargo_minutes: int = 0,
    station_col: str = "station_id",
    start_col: str = "start_dt",
    end_col: str = "end_dt",
) -> pd.Series:
    """실시간 서빙용: as_of 기준 [as_of-embargo-window, as_of-embargo) 구간의 "관측 가능한" 대여 건수.

    배치 집계(censored_rolling_counts)와 같은 핵심 규칙(윈도우 안에 시작 +
    end<=as_of)을 쓰지만, 이쪽은 소량의 최근 이벤트 버퍼에 대해 임의의 단일
    시각 as_of로 즉시 계산하는 용도다.

    args:
        events: station_col/start_col/end_col을 포함하는 최근 이벤트 버퍼
            (end_col은 아직 반납 안 된 트립이면 NaT)
        as_of: 기준 시각 T (보통 "지금")
        window_minutes: 윈도우 폭(분), 기본 60분
        embargo_minutes: as_of에서 윈도우까지의 간격(분) — 완료율이 낮은 최신
            구간을 건너뛰기 위함, 기본 0(예전 방식과 호환)
        station_col: 정류소 ID 컬럼명
        start_col: 대여 시작 시각 컬럼명
        end_col: 반납 완료 시각 컬럼명
    returns:
        pd.Series: station_col별 관측 가능 대여 건수 (해당 station이 없으면 등장 안 함 — 0-fill은 호출부 책임)
    """
    window_end = as_of - pd.Timedelta(minutes=embargo_minutes)
    window_start = window_end - pd.Timedelta(minutes=window_minutes)
    in_window = (events[start_col] >= window_start) & (events[start_col] < window_end)
    visible = events[end_col].notna() & (events[end_col] <= as_of)
    return events.loc[in_window & visible].groupby(station_col).size()


def censored_rolling_counts(
    trips: pd.DataFrame,
    window_minutes: int,
    embargo_minutes: int,
    tick_minutes: int = 5,
    station_col: str = "station_id",
    start_col: str = "start_dt",
    end_col: str = "end_dt",
) -> pd.DataFrame:
    """배치용: station별로 [T-embargo-window, T-embargo) point-in-time 카운트를 모든 tick T에 대해 계산한다.

    트립 하나가 "카운트에 잡히는 T의 구간"은 (직관과 달리) 하나의 tick이
    아니라 **연속된 여러 tick**이다 — 윈도우가 그 트립의 시작 시각을 포함하는
    동안(embargo/폭에 따라 여러 tick), 그리고 end_ts<=T가 성립하는 동안만.
    이걸 매 트립마다 tick 단위로 펼치면(트립 수 × 윈도우/tick 배수) 느리므로,
    "카운트가 +1 되는 시작 tick"과 "-1 되는 종료+1 tick"만 기록한 뒤
    station별로 시간순 누적합(cumsum)하는 **차분 배열(difference array)**
    기법으로 O(트립 수)에 계산한다.

    args:
        trips: station_col/start_col/end_col을 포함하는 트립 단위 DataFrame
        window_minutes: 윈도우 폭(분)
        embargo_minutes: as_of에서 윈도우까지의 간격(분)
        tick_minutes: 서빙 갱신 주기(분), 기본 5분
        station_col: 정류소 ID 컬럼명
        start_col: 대여 시작 시각 컬럼명
        end_col: 반납 완료 시각 컬럼명
    returns:
        pd.DataFrame: station_col, tick, count — station별로 tick 오름차순
            정렬됨. **sparse한 step function**이다: 특정 tick에 대한 값을
            조회하려면 `lookup_count_at_ticks()`로 그 tick 이하 중 가장 최근
            행을 찾아야 한다 (다음 delta가 나오기 전까지 값이 유지되므로).
    """
    tick = pd.Timedelta(minutes=tick_minutes)
    embargo = pd.Timedelta(minutes=embargo_minutes)
    width = pd.Timedelta(minutes=window_minutes)

    lo_bound = trips[start_col] + embargo
    hi_bound = lo_bound + width
    # 포함 조건은 "start_ts < T-embargo" (엄격한 부등호, 위 docstring 참고) —
    # 즉 T > lo_bound인 가장 빠른 tick이 필요하다. lo_bound가 정확히 tick 위에
    # 있으면(예: 트립 시작 시각이 마침 5분 배수) ceil()은 그 값 자체를 돌려줘
    # "엄격히 큼" 조건을 깨뜨리고 그 트립을 한 tick 일찍 카운트한다 —
    # floor()+tick은 정렬 여부와 무관하게 항상 다음 tick을 반환해 경계값에서도
    # 정확하다 (future_rolling_counts()와 동일한 패턴).
    lo_t = lo_bound.dt.floor(tick) + tick
    hi_t = hi_bound.dt.floor(tick)  # 이쪽은 "<=" 비-엄격 조건이라 floor()가 그대로 맞음

    end = trips[end_col]
    vis_t = end.dt.ceil(tick)  # NaT -> NaT (아래에서 별도로 제외)

    effective_lo_t = lo_t.where(vis_t.isna() | (lo_t >= vis_t), vis_t)  # max(lo_t, vis_t)
    valid = end.notna() & (effective_lo_t <= hi_t)

    stations = trips.loc[valid, station_col]
    starts = pd.DataFrame({station_col: stations, "tick": effective_lo_t[valid], "delta": 1})
    ends = pd.DataFrame({station_col: stations, "tick": hi_t[valid] + tick, "delta": -1})

    deltas = pd.concat([starts, ends], ignore_index=True)
    agg = deltas.groupby([station_col, "tick"], observed=True)["delta"].sum().reset_index()
    agg = agg.sort_values([station_col, "tick"]).reset_index(drop=True)
    agg["count"] = agg.groupby(station_col, observed=True)["delta"].cumsum().astype("int32")
    return agg[[station_col, "tick", "count"]]


def future_rolling_counts(
    trips: pd.DataFrame,
    width_minutes: int,
    tick_minutes: int = 5,
    station_col: str = "station_id",
    event_col: str = "start_dt",
) -> pd.DataFrame:
    """타겟 생성용: station별로 "[T, T+width) 구간에 이벤트가 있었던 건수"를 모든 tick T에 대해 계산한다.

    `censored_rolling_counts()`가 "그 시점에 관측 가능했던 과거"(입력 피처)를 보는
    것과 정반대로, 이 함수는 "T를 기준으로 앞으로 width분 동안 실제로 일어난 진짜
    값"을 계산한다 — 타겟은 이미 몇 달~몇 년 지나 전부 확정된 과거 데이터로
    만들어지므로, 관측 가능성(censoring/embargo)을 따질 필요가 전혀 없다. 그래서
    인자도 이벤트 시각(event_col) 하나만 쓴다 — 대여 타겟이면 `start_dt`, 반납
    타겟이면 `end_dt`를 넘기면 된다(둘 다 같은 함수를 그대로 재사용).

    알고리즘은 동일한 차분 배열(difference array) 기법이다 — 이벤트 하나가 카운트에
    잡히는 tick T의 조건은 "T <= event < T+width"(동치: `event-width < T <= event`)
    이므로, 그 구간의 시작/끝(+1/-1) 델타만 기록한 뒤 station별 누적합으로 O(이벤트 수)에
    계산한다.

    args:
        trips: station_col/event_col을 포함하는 트립 단위 DataFrame
        width_minutes: 타겟 윈도우 폭(분) — "앞으로 몇 분간"(기본 설계: 60분=1시간)
        tick_minutes: grid 간격(분)
        station_col: 정류소 ID 컬럼명
        event_col: 이벤트 시각 컬럼명 (대여 타겟="start_dt", 반납 타겟="end_dt")
    returns:
        pd.DataFrame: station_col, tick, count — station별로 tick 오름차순 정렬됨.
            **sparse한 step function**이다 — `lookup_count_at_ticks()`로 조회한다.
    """
    tick = pd.Timedelta(minutes=tick_minutes)
    width = pd.Timedelta(minutes=width_minutes)

    start = trips[event_col]
    hi_t = start.dt.floor(tick)  # T <= start를 만족하는 가장 늦은 tick
    # T > start-width(엄격한 부등호)를 만족하는 가장 빠른 tick. ceil()이 아니라
    # floor()+tick을 쓰는 이유: start-width가 정확히 tick 위에 있을 때 ceil()은
    # 그 값 자체를 반환해 "엄격히 큼" 조건을 깨뜨린다 — floor()+tick은 항상
    # 다음 tick을 반환해 경계값에서도 조건을 정확히 지킨다.
    lo_t = (start - width).dt.floor(tick) + tick

    stations = trips[station_col]
    starts_df = pd.DataFrame({station_col: stations, "tick": lo_t, "delta": 1})
    ends_df = pd.DataFrame({station_col: stations, "tick": hi_t + tick, "delta": -1})

    deltas = pd.concat([starts_df, ends_df], ignore_index=True)
    agg = deltas.groupby([station_col, "tick"], observed=True)["delta"].sum().reset_index()
    agg = agg.sort_values([station_col, "tick"]).reset_index(drop=True)
    agg["count"] = agg.groupby(station_col, observed=True)["delta"].cumsum().astype("int32")
    return agg[[station_col, "tick", "count"]]


def lookup_count_at_ticks(
    cumulative: pd.DataFrame,
    query_ticks: pd.DataFrame,
    station_col: str = "station_id",
    tick_col: str = "tick",
    query_tick_col: str = "tick",
) -> pd.Series:
    """censored_rolling_counts()의 sparse step function을 원하는 tick 목록에서 조회한다.

    각 (station, query_tick)에 대해 "그 시각 이하에서 가장 최근 delta 이후의
    누적값"을 `pd.merge_asof`로 찾는다 (station별 전체 dense grid를 만들지
    않아도 되므로 station×tick 조합이 큰 경우에도 효율적).

    args:
        cumulative: censored_rolling_counts()의 결과 (station_col, tick_col, "count")
        query_ticks: station_col과 query_tick_col을 포함하는, 값을 조회하고
            싶은 (station, tick) 목록
        station_col: 정류소 ID 컬럼명
        tick_col: cumulative 쪽 tick 컬럼명
        query_tick_col: query_ticks 쪽 tick 컬럼명
    returns:
        pd.Series: query_ticks와 같은 순서의 count (해당 station에 그 시점
            이전 delta가 전혀 없으면 0)

    **구현 노트**: `pd.merge_asof(..., by=station_col)`로 구현했었으나, 268M행
    규모(실제 병합 테이블)에서 실측 30분+ 걸림을 발견했다 — 프로파일링(macOS
    `sample`)해보니 시간의 90%+가 `Int64HashTable`을 **한 번에 크기를 안 잡고
    점진적으로 resize/rehash**하는 데 쓰이고 있었다(merge_asof의 내부 구현
    문제로 보임, 이 함수의 로직 문제가 아님). station별로 이미 정렬된 배열에
    `np.searchsorted`(이진 탐색)를 직접 쓰는 것으로 대체 — merge_asof와 정확히
    같은 "backward" 의미(그 시각 이하 중 가장 최근 값)를 내면서, 실측
    268M행에서 30분+ -> 수십 초로 단축됐다.
    """
    cumulative = cumulative[[station_col, tick_col, "count"]].sort_values([station_col, tick_col])
    query = query_ticks[[station_col, query_tick_col]]

    by_station = {
        station: (group[tick_col].to_numpy(), group["count"].to_numpy())
        for station, group in cumulative.groupby(station_col, sort=False, observed=True)
    }

    result = np.zeros(len(query), dtype="int32")
    query_tick_values = query[query_tick_col].to_numpy()
    for station, positions in query.groupby(station_col, sort=False, observed=True).indices.items():
        ticks_arr, counts_arr = by_station.get(station, (None, None))
        if ticks_arr is None:
            continue  # 이 station은 cumulative에 delta가 전혀 없음 -> 전부 0 유지
        pos = np.searchsorted(ticks_arr, query_tick_values[positions], side="right") - 1
        valid = pos >= 0
        vals = np.zeros(len(positions), dtype="int32")
        vals[valid] = counts_arr[pos[valid]]
        result[positions] = vals

    return pd.Series(result, index=query_ticks.index)
