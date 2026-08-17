"""rolling_window_features.py의 point-in-time censoring 규칙 검증.

핵심 규칙: 트립은 (station, T) 카운트에 포함된다 <=> start가 [T-window, T)
안에 있고, end가 결측이 아니며 end<=T. 이 파일은 그 규칙이 정확히 지켜지는지
— 특히 "반납이 늦은 트립은 자신이 속한 버킷이 닫히는 순간엔 보이지 않는다"는
동작을 작은 합성 예시로 확인한다.
"""

import pandas as pd
import pytest

from ml_core.rolling_window_features import (
    add_censored_visibility,
    censored_rolling_counts,
    count_visible_in_window,
    future_rolling_counts,
    lookup_count_at_ticks,
)


def _trip(station, start, end=None):
    return {
        "station_id": station,
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end) if end is not None else pd.NaT,
    }


@pytest.fixture
def sample_trips() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _trip("A", "2025-06-01 10:02:00", "2025-06-01 10:04:00"),  # 같은 버킷 안에 반납 완료 -> 보임
            _trip("A", "2025-06-01 10:03:00", "2025-06-01 10:07:00"),  # 버킷(10:05) 이후에 반납 -> 안 보임
            _trip("A", "2025-06-01 10:01:00", None),  # 아직 반납 안 됨 -> 안 보임
            _trip("B", "2025-06-01 09:58:00", "2025-06-01 09:59:00"),  # 이전 버킷([09:55,10:00)) 소속
        ]
    )


def test_add_censored_visibility_bucket_assignment(sample_trips):
    out = add_censored_visibility(sample_trips, window_minutes=5)

    a_bucket_starts = out.loc[out.station_id == "A", "bucket_start"].tolist()
    assert all(bs == pd.Timestamp("2025-06-01 10:00:00") for bs in a_bucket_starts)

    b_bucket_start = out.loc[out.station_id == "B", "bucket_start"].iloc[0]
    assert b_bucket_start == pd.Timestamp("2025-06-01 09:55:00")


def test_fast_trip_visible_at_bucket_close(sample_trips):
    out = add_censored_visibility(sample_trips, window_minutes=5)
    fast_trip = out[(out.station_id == "A") & (out.end_dt == pd.Timestamp("2025-06-01 10:04:00"))]
    assert fast_trip["visible_at_close"].iloc[0] == True


def test_slow_trip_not_visible_at_own_bucket_close(sample_trips):
    """반납이 버킷 종료 시각(10:05)보다 늦은(10:07) 트립은 그 순간엔 관측되면 안 된다 — 이게 이 파일의 핵심."""
    out = add_censored_visibility(sample_trips, window_minutes=5)
    slow_trip = out[(out.station_id == "A") & (out.end_dt == pd.Timestamp("2025-06-01 10:07:00"))]
    assert slow_trip["visible_at_close"].iloc[0] == False


def test_incomplete_trip_never_visible(sample_trips):
    """반납 자체가 안 된(NaT) 트립은 어떤 시점을 봐도 관측되면 안 된다."""
    out = add_censored_visibility(sample_trips, window_minutes=5)
    incomplete = out[(out.station_id == "A") & out.end_dt.isna()]
    assert incomplete["visible_at_close"].iloc[0] == False


def test_censored_count_undercounts_true_count(sample_trips):
    """station A, 버킷 [10:00,10:05): 실제 트립 3건이지만 그 순간 보이는 건 1건뿐이어야 한다."""
    out = add_censored_visibility(sample_trips, window_minutes=5)
    bucket = out[(out.station_id == "A") & (out.bucket_start == pd.Timestamp("2025-06-01 10:00:00"))]

    true_count = len(bucket)
    censored_count = bucket["visible_at_close"].sum()

    assert true_count == 3
    assert censored_count == 1
    assert censored_count < true_count  # 우측 절단으로 인한 과소집계 확인


def test_count_visible_in_window_excludes_late_and_incomplete(sample_trips):
    """실시간 서빙 함수도 배치 함수와 동일한 결론(1건)을 내야 한다."""
    counts = count_visible_in_window(sample_trips, as_of=pd.Timestamp("2025-06-01 10:05:00"), window_minutes=5)
    assert counts.get("A", 0) == 1


def test_count_visible_in_window_sees_late_trip_once_it_returns():
    """같은 트립이라도 as_of가 반납 시각을 지나면 그제서야 보여야 한다 (지연 후 관측)."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 10:03:00", "2025-06-01 10:07:00")])

    at_close = count_visible_in_window(trips, as_of=pd.Timestamp("2025-06-01 10:05:00"), window_minutes=5)
    assert at_close.get("A", 0) == 0

    later = count_visible_in_window(trips, as_of=pd.Timestamp("2025-06-01 10:08:00"), window_minutes=10)
    assert later.get("A", 0) == 1


# --- censored_rolling_counts / lookup_count_at_ticks: width=60분, embargo=30분 ---
# "5분 단위"는 서빙 갱신 주기(tick)일 뿐 윈도우 폭이 아니다 — 실제 feature는
# "[T-90분, T-30분)에 시작되고 end_dt<=T인 대여 수"다. 아래 트립들의 시각은
# 5분 격자에 정확히 걸치지 않도록(초 단위 오프셋) 잡아서, 경계값 반올림 관련
# 애매함 없이 핵심 로직만 확인한다.


def _query(station_ids, ticks):
    return pd.DataFrame({"station_id": station_ids, "tick": ticks})


def test_censored_rolling_counts_fast_trip_window_membership():
    """빠른 트립(10분)은 embargo 때문에 09:35부터 세어지고, 창을 완전히 벗어나면(10:35) 더 이상 안 세어진다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33")])
    cum = censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5)

    ticks = pd.to_datetime(["2025-06-01 09:30:00", "2025-06-01 09:35:00", "2025-06-01 10:30:00", "2025-06-01 10:35:00"])
    counts = lookup_count_at_ticks(cum, _query(["A"] * 4, ticks))

    assert counts.tolist() == [0, 1, 1, 0]  # 09:30엔 아직(embargo), 09:35~10:30엔 보임, 10:35엔 창을 벗어남


def test_censored_rolling_counts_visibility_gates_mid_window():
    """대여는 창 안에 들어왔어도(09:35~) 반납이 늦으면(10:05) 그 전까지는 안 보여야 한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:07", "2025-06-01 10:00:10")])
    cum = censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5)

    ticks = pd.to_datetime(["2025-06-01 09:40:00", "2025-06-01 10:00:00", "2025-06-01 10:05:00", "2025-06-01 10:30:00"])
    counts = lookup_count_at_ticks(cum, _query(["A"] * 4, ticks))

    assert counts.tolist() == [0, 0, 1, 1]  # 창엔 들어왔지만 반납 전(09:40,10:00)엔 0, 반납 후(10:05~)엔 1


def test_censored_rolling_counts_excludes_trip_slower_than_window():
    """embargo+폭(90분)보다 반납이 더 늦은 트립은 이 윈도우 정의에서 영원히 안 잡혀야 한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:07", "2025-06-01 10:40:20")])
    cum = censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5)

    if cum.empty:
        return  # 아예 delta가 안 생기는 것도 유효한 결과 (완전히 제외됨)

    ticks = pd.date_range("2025-06-01 09:00:00", "2025-06-01 12:00:00", freq="5min")
    counts = lookup_count_at_ticks(cum, _query(["A"] * len(ticks), ticks))
    assert (counts == 0).all()


def test_censored_rolling_counts_matches_serving_function():
    """배치(censored_rolling_counts)와 서빙(count_visible_in_window)이 같은 T에서 같은 값을 내야 한다."""
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
            _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
        ]
    )
    as_of = pd.Timestamp("2025-06-01 10:05:00")

    batch = lookup_count_at_ticks(
        censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5),
        _query(["A"], [as_of]),
    ).iloc[0]
    serving = count_visible_in_window(trips, as_of=as_of, window_minutes=60, embargo_minutes=30).get("A", 0)

    assert batch == serving


def test_censored_rolling_counts_matches_serving_function_when_start_lands_exactly_on_tick():
    """트립 시작 시각이 정확히 tick 배수(+embargo도 tick 배수)일 때의 경계값 회귀 테스트.

    lo_t 계산에 floor()+tick 대신 ceil()을 쓰면, start+embargo가 마침 tick 위에 있을 때
    "start_ts < T-embargo"(엄격한 부등호) 조건을 깨고 그 트립을 한 tick 일찍(경계 tick
    자체에서) 카운트하는 조용한 버그가 생긴다 — 실제 트립 타임스탬프는 초 단위라 거의
    안 걸리지만, 그리드가 5분 tick이 되면서 정각 조회가 흔해져 실제로 드러난 사례다.
    """
    trips = pd.DataFrame([_trip("A", "2025-06-01 10:40:00", "2025-06-01 10:55:00")])  # start가 정확히 5분 배수
    as_of = pd.Timestamp("2025-06-01 11:10:00")  # start+embargo(30분)와 정확히 일치하는 경계 tick

    batch = lookup_count_at_ticks(
        censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5),
        _query(["A"], [as_of]),
    ).iloc[0]
    serving = count_visible_in_window(trips, as_of=as_of, window_minutes=60, embargo_minutes=30).get("A", 0)

    assert batch == serving == 0, "경계 tick 자체는 아직 window 밖(strict) — 카운트되면 안 됨"


def test_censored_rolling_counts_matches_serving_function_sweep_5min_ticks():
    """여러 트립 x 5분 간격 조밀한 sweep으로 배치/서빙이 전 구간에서 일치하는지 확인한다.

    hourly 간격만 스팟체크하면 tick 경계 off-by-one을 놓친다 (실제로 위 테스트의
    버그가 이 sweep 없이는 안 잡혔다) — 5분마다 대조해서 tick 경계 전부를 훑는다.
    """
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
            _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
            _trip("A", "2025-06-01 09:50:00", "2025-06-01 11:20:00"),
            _trip("A", "2025-06-01 10:40:00", "2025-06-01 10:55:00"),
        ]
    )
    query_ticks = pd.date_range("2025-06-01 08:00", "2025-06-01 13:00", freq="5min")
    batch = lookup_count_at_ticks(
        censored_rolling_counts(trips, window_minutes=60, embargo_minutes=30, tick_minutes=5),
        _query(["A"] * len(query_ticks), query_ticks),
    )

    for t, got in zip(query_ticks, batch):
        expected = count_visible_in_window(trips, as_of=t, window_minutes=60, embargo_minutes=30).get("A", 0)
        assert got == expected, f"{t}: batch={got} serving={expected}"


# --- future_rolling_counts: 타겟 생성용, "[T, T+width)에 시작된 대여 수" ---
# censored_rolling_counts와 정반대 방향(과거를 보는 입력 피처 vs 미래를 보는 타겟)이라
# end_dt/embargo 없이 start_dt만으로 계산한다.


def _brute_force_future_count(trips: pd.DataFrame, station: str, at: pd.Timestamp, width_minutes: int) -> int:
    lo, hi = at, at + pd.Timedelta(minutes=width_minutes)
    mask = (trips["station_id"] == station) & (trips["start_dt"] >= lo) & (trips["start_dt"] < hi)
    return int(mask.sum())


def test_future_rolling_counts_matches_brute_force_sweep():
    """여러 트립·여러 tick에 대해 직접 필터링한 값과 정확히 일치해야 한다."""
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 10:00:07"),
            _trip("A", "2025-06-01 10:20:00"),
            _trip("A", "2025-06-01 10:50:00"),
            _trip("A", "2025-06-01 09:00:00"),
        ]
    )
    cum = future_rolling_counts(trips, width_minutes=60, tick_minutes=5)

    query_ticks = pd.date_range("2025-06-01 08:00", "2025-06-01 12:00", freq="5min")
    looked_up = lookup_count_at_ticks(cum, pd.DataFrame({"station_id": ["A"] * len(query_ticks), "tick": query_ticks}))

    for t, got in zip(query_ticks, looked_up):
        expected = _brute_force_future_count(trips, "A", t, 60)
        assert got == expected, f"{t}: got={got} expected={expected}"


def test_future_rolling_counts_trip_exactly_at_reference_tick_is_included():
    """T=대여 시작 시각(정확히 tick 위)이면 그 T는 포함돼야 한다 (T<=start, 비-엄격 부등호)."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 10:20:00")])
    cum = future_rolling_counts(trips, width_minutes=60, tick_minutes=5)

    query_ticks = pd.to_datetime(["2025-06-01 10:20:00", "2025-06-01 09:20:00", "2025-06-01 09:25:00"])
    looked_up = lookup_count_at_ticks(cum, pd.DataFrame({"station_id": ["A"] * 3, "tick": query_ticks}))

    assert looked_up.iloc[0] == 1, "T=start는 포함돼야 함"
    assert looked_up.iloc[1] == 0, "T=start-width는 엄격히 제외돼야 함(T>start-width)"
    assert looked_up.iloc[2] == 1, "T=start-width+tick(엄격히 큼)은 포함돼야 함"


def test_future_rolling_counts_trip_start_not_on_tick_boundary():
    """대여 시작 시각이 tick 위가 아니어도(초 단위 오프셋) 경계 계산이 정확해야 한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 10:23:17")])
    cum = future_rolling_counts(trips, width_minutes=60, tick_minutes=5)

    query_ticks = pd.date_range("2025-06-01 09:00", "2025-06-01 11:00", freq="5min")
    looked_up = lookup_count_at_ticks(cum, pd.DataFrame({"station_id": ["A"] * len(query_ticks), "tick": query_ticks}))
    included = query_ticks[looked_up.to_numpy().astype(bool)]

    # T<=10:23:17이고 T>09:23:17을 만족하는 5분 tick: 09:25 ~ 10:20 (12개, width/tick=60/5)
    assert list(included) == list(pd.date_range("2025-06-01 09:25", "2025-06-01 10:20", freq="5min"))
    assert len(included) == 12


def test_future_rolling_counts_multiple_trips_matches_brute_force():
    """여러 트립이 겹치는 tick에서도 브루트포스와 정확히 일치해야 한다."""
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 10:00:00"),
            _trip("A", "2025-06-01 10:05:00"),
            _trip("A", "2025-06-01 10:10:00"),
        ]
    )
    cum = future_rolling_counts(trips, width_minutes=60, tick_minutes=5)

    query_ticks = pd.date_range("2025-06-01 08:30", "2025-06-01 11:30", freq="5min")
    looked_up = lookup_count_at_ticks(cum, pd.DataFrame({"station_id": ["A"] * len(query_ticks), "tick": query_ticks}))

    for t, got in zip(query_ticks, looked_up):
        expected = _brute_force_future_count(trips, "A", t, 60)
        assert got == expected, f"{t}: got={got} expected={expected}"

    at_10_00 = lookup_count_at_ticks(cum, _query(["A"], [pd.Timestamp("2025-06-01 10:00:00")])).iloc[0]
    assert at_10_00 == 3, "T=10:00 윈도우 [10:00,11:00)엔 세 트립 모두 시작됨"
