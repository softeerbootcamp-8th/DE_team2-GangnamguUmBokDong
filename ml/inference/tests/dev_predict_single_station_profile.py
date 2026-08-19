"""_build_station_profile_arrays()/_profile_stat()의 dense 배열 기반 구현을 검증한다
(PR 리뷰 지적 — 예전 dict[tuple, dict[str,float]] 캐시가 station x minute x dow x month
전체 조합(수천 정류소 기준 약 1,800만 항목)을 그대로 담아 프로세스당 수 GB를 먹었다,
predict_single.py의 `_build_station_profile_arrays()` 모듈 docstring 참고).

이 테스트는 그 결과가 예전 dict 기반 구현과 정확히 같은 값을 돌려주는지(값 자체는
바뀌면 안 됨 — 순수 내부 표현 교체이므로) 확인한다.
"""

import numpy as np
import pandas as pd
import pytest

from inference import config
from inference import predict_single as ps


@pytest.fixture(autouse=True)
def _reset_module_caches():
    names = ["_station_profile_station_index", "_station_profile_values"]
    saved = {n: getattr(ps, n) for n in names}
    yield
    for n, v in saved.items():
        setattr(ps, n, v)


def _profile_df(rows: list[dict]) -> pd.DataFrame:
    return pd.DataFrame(rows)


def test_build_station_profile_arrays_round_trips_exact_values():
    ts_a = pd.Timestamp("2026-01-05 00:00")  # 월요일(dow=0)
    ts_b = pd.Timestamp("2026-06-03 00:00") + pd.Timedelta(minutes=config.GRID_TICK_MINUTES)  # 수요일(dow=2)
    df = _profile_df(
        [
            {"station_no": 5, "minute": ts_a.hour * 60 + ts_a.minute, "dow": ts_a.dayofweek, "month": ts_a.month,
             "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25},
            {"station_no": 5, "minute": ts_b.hour * 60 + ts_b.minute, "dow": ts_b.dayofweek, "month": ts_b.month,
             "rental_mean": 9.0, "rental_std": 3.0, "return_mean": 4.0, "return_std": 1.5},
            {"station_no": 9, "minute": ts_a.hour * 60 + ts_a.minute, "dow": ts_a.dayofweek, "month": ts_a.month,
             "rental_mean": 100.0, "rental_std": 10.0, "return_mean": 50.0, "return_std": 5.0},
        ]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert ps._profile_stat(5, ts_a, "rental_mean") == pytest.approx(1.0)
    assert ps._profile_stat(5, ts_a, "return_std") == pytest.approx(0.25)
    assert ps._profile_stat(5, ts_b, "rental_mean") == pytest.approx(9.0)
    assert ps._profile_stat(9, ts_a, "return_mean") == pytest.approx(50.0)


def test_profile_stat_returns_nan_for_missing_combination():
    df = _profile_df(
        [{"station_no": 5, "minute": 0, "dow": 0, "month": 1,
          "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25}]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    # 같은 station이지만 등록되지 않은 (minute, dow, month) 조합
    assert np.isnan(ps._profile_stat(5, pd.Timestamp("2026-02-01 00:00"), "rental_mean"))


def test_profile_stat_returns_nan_for_unknown_station():
    df = _profile_df(
        [{"station_no": 5, "minute": 0, "dow": 0, "month": 1,
          "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25}]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    assert np.isnan(ps._profile_stat(999, pd.Timestamp("2026-01-01 00:00"), "rental_mean"))


def test_profile_stat_returns_nan_for_off_grid_timestamp():
    """예전 dict 기반 구현도 ts가 GRID_TICK_MINUTES 배수가 아니면 항상 dict miss(NaN)였다
    — dense 배열이 minute을 tick 인덱스로 나눠 압축하므로, 이 miss 동작을 그대로
    재현해야 한다(안 그러면 원래는 없던 값을 가장 가까운 tick 값으로 조용히
    채워주는 동작 변화가 생긴다)."""
    df = _profile_df(
        [{"station_no": 5, "minute": 0, "dow": 0, "month": 1,
          "rental_mean": 1.0, "rental_std": 0.5, "return_mean": 2.0, "return_std": 0.25}]
    )
    ps._station_profile_station_index, ps._station_profile_values = ps._build_station_profile_arrays(df)

    off_grid_ts = pd.Timestamp("2026-01-01 00:00") + pd.Timedelta(minutes=1)
    assert off_grid_ts.minute % config.GRID_TICK_MINUTES != 0
    assert np.isnan(ps._profile_stat(5, off_grid_ts, "rental_mean"))


def test_build_station_profile_arrays_uses_compact_dense_shape_not_per_combo_dict():
    """station 축은 실제로 등장한 station_no 개수만큼만 잡아야 한다(예: 정류소가
    2개뿐인데 station_no 값 자체는 1과 9000처럼 멀리 떨어져 있어도 배열 크기가
    9000이 되면 안 된다) — dense 배열 압축의 핵심 전제."""
    df = _profile_df(
        [
            {"station_no": 1, "minute": 0, "dow": 0, "month": 1,
             "rental_mean": 1.0, "rental_std": 0.0, "return_mean": 0.0, "return_std": 0.0},
            {"station_no": 9000, "minute": 0, "dow": 0, "month": 1,
             "rental_mean": 2.0, "rental_std": 0.0, "return_mean": 0.0, "return_std": 0.0},
        ]
    )
    station_index, values = ps._build_station_profile_arrays(df)

    assert values.shape[0] == 2  # station_no 값 범위(1~9000)가 아니라 실제 등장 개수
    assert set(station_index) == {1, 9000}
