"""predict_single.py의 대여(rental) point-in-time censoring 반영 검증.

predict_single.py의 캐시는 모듈 전역 변수라, 파일 I/O 없이 합성 트립/히스토리를
직접 주입해서 테스트한다 (tests/dev_rolling_window_features.py의 `_trip()` 합성
fixture 패턴 재사용). 각 테스트 후 전역을 리셋해 테스트 간 오염을 막는다.
"""

import numpy as np
import pandas as pd
import pytest
from ml_common.rolling_window_features import count_visible_in_window

from inference import config
from inference import predict_single as ps


def _trip(station, start, end=None):
    return {
        "station_id": station,
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end) if end is not None else pd.NaT,
    }


@pytest.fixture(autouse=True)
def _reset_module_caches():
    """각 테스트 전후로 predict_single.py의 모듈 전역 캐시를 리셋한다.

    `_rental_events_sorted_by_station`은 다른 캐시들과 달리 테스트가 통째로
    재할당하는 게 아니라 `_rental_visible_at()`이 station_id 키로 in-place
    mutate한다 — save/restore로 참조만 되돌리면 그 사이에 채워진 항목이 그대로
    남아있어(같은 station_id를 테스트마다 다른 트립으로 재사용하면) 이전
    테스트의 정렬 캐시가 새 테스트에 새어 들어간다. 그래서 이건 매번 새
    dict로 통째로 비운다.
    """
    names = [
        "_history_by_station",
        "_rental_events_by_station",
        "_rental_events_coverage",
        "_station_profile",
    ]
    saved = {n: getattr(ps, n) for n in names}
    ps._rental_events_sorted_by_station = {}
    yield
    for n, v in saved.items():
        setattr(ps, n, v)
    ps._rental_events_sorted_by_station = {}


def _set_rental_events(trips: pd.DataFrame, coverage: tuple[pd.Timestamp, pd.Timestamp] | None = None) -> None:
    """합성 트립을 주입한다. coverage를 안 주면 2025년 전체로 잡는다 — 실제 서비스에서
    _rental_events_coverage는 전체 트립(모든 station)의 min/max로, 개별 station의 트립
    분포와 무관하게 넓다. 개별 트립의 min/max로 좁게 잡으면 coverage 경계 테스트가
    아닌 다른 테스트에서 의도치 않게 fallback이 트리거된다."""
    ps._rental_events_by_station = {
        sid: g[["station_id", "start_dt", "end_dt"]].reset_index(drop=True) for sid, g in trips.groupby("station_id")
    }
    ps._rental_events_coverage = coverage or (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31 23:59:59"))


def _set_history(station_id: str, hours, rental_counts, return_counts) -> None:
    idx = pd.to_datetime(hours)
    ps._history_by_station = {
        station_id: pd.DataFrame({"rental_count": rental_counts, "return_count": return_counts}, index=idx)
    }


def _set_profile(entries: dict) -> None:
    """entries: {(station_id, hour, dow, month): {"rental_mean":..., "rental_std":..., ...}}"""
    ps._station_profile = entries


def test_rental_lag_1h_matches_count_visible_in_window():
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
            _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
            _trip("A", "2025-06-01 09:50:00", "2025-06-01 11:20:00"),
        ]
    )
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    _set_profile({})

    target_ts = pd.Timestamp("2025-06-01 10:00:00")
    out, fallback = ps._lag_rolling_features("A", target_ts)

    expected = count_visible_in_window(
        trips, as_of=target_ts, window_minutes=config.ROLLING_WINDOW_MINUTES, embargo_minutes=config.ROLLING_EMBARGO_MINUTES
    ).get("A", 0)

    assert out["rental_lag_1h"] == float(expected) == 2.0
    assert "rental_lag_1h" not in fallback


def test_rental_roll_mean_std_3h_matches_manual_anchors():
    trips = pd.DataFrame(
        [
            _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
            _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
            _trip("A", "2025-06-01 09:50:00", "2025-06-01 11:20:00"),
        ]
    )
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    _set_profile({})

    target_ts = pd.Timestamp("2025-06-01 10:00:00")
    out, fallback = ps._lag_rolling_features("A", target_ts)

    # dense 정의: (target_ts-3h, target_ts] 안의 5분 tick 전부(36개) — features.py와 동일.
    anchors = pd.date_range(
        target_ts - pd.Timedelta(hours=3) + pd.Timedelta(minutes=config.GRID_TICK_MINUTES),
        target_ts,
        freq=f"{config.GRID_TICK_MINUTES}min",
    )
    manual = [
        count_visible_in_window(
            trips, as_of=t, window_minutes=config.ROLLING_WINDOW_MINUTES, embargo_minutes=config.ROLLING_EMBARGO_MINUTES
        ).get("A", 0)
        for t in anchors
    ]

    assert out["rental_roll_mean_3h"] == pytest.approx(float(np.mean(manual)))
    assert out["rental_roll_std_3h"] == pytest.approx(float(np.std(manual, ddof=1)))
    assert "rental_roll_mean_3h" not in fallback
    assert "rental_roll_std_3h" not in fallback


def test_rental_recent_zero_trips_in_window_is_not_fallback():
    """윈도우 커버리지 안이지만 그 구간에 트립이 0건이면 정상값 0이지 fallback이 아니다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 06:00:00", "2025-06-01 06:05:00")])
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    _set_profile({})

    target_ts = pd.Timestamp("2025-06-01 10:00:00")  # 윈도우 [08:30,09:30) 안에 트립 없음
    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert out["rental_lag_1h"] == 0.0
    assert "rental_lag_1h" not in fallback


def test_rental_recent_fallback_when_target_outside_coverage():
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips, coverage=(pd.Timestamp("2025-06-01"), pd.Timestamp("2025-06-01 23:59:59")))
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    _set_profile(
        {
            ("A", h, dow, m): {"rental_mean": 7.0, "rental_std": 1.5, "return_mean": 0.0, "return_std": 0.0}
            for h in range(24)
            for dow in range(7)
            for m in range(1, 13)
        }
    )

    target_ts = pd.Timestamp("2026-08-01 08:00:00")  # 트립 커버리지(2025-06-01 하루)와 전혀 안 겹침
    out, fallback = ps._lag_rolling_features("A", target_ts)

    for name in ["rental_lag_1h", "rental_roll_mean_3h", "rental_roll_std_3h", "rental_roll_mean_24h", "rental_roll_std_24h"]:
        assert name in fallback, name
        assert out[name] == pytest.approx(7.0 if "mean" in name or name == "rental_lag_1h" else 1.5)


def test_rental_lag_24h_168h_unaffected_by_empty_trip_source():
    """트립 단위 소스가 완전히 비어 있어도 rental_lag_24h/168h는 기존 hourly 히스토리로 정상 계산된다."""
    ps._rental_events_by_station = {}
    ps._rental_events_coverage = None

    hours = pd.date_range("2025-06-01 00:00", periods=200, freq="h")
    rental_counts = list(range(200))
    _set_history("A", hours, rental_counts, [0] * 200)
    _set_profile({})

    target_ts = hours[199]
    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert out["rental_lag_24h"] == float(rental_counts[199 - 24])
    assert out["rental_lag_168h"] == float(rental_counts[199 - 168])
    assert "rental_lag_24h" not in fallback
    assert "rental_lag_168h" not in fallback


def test_return_features_exactly_unchanged():
    ps._rental_events_by_station = {}
    ps._rental_events_coverage = None

    hours = pd.date_range("2025-06-01 00:00", periods=200, freq="h")
    return_counts = [(i % 11) for i in range(200)]
    _set_history("A", hours, [0] * 200, return_counts)
    _set_profile({})

    target_ts = hours[199]
    out, fallback = ps._lag_rolling_features("A", target_ts)

    series = pd.Series(return_counts, index=hours)
    for lag in config.LAG_HOURS:
        expected = series.get(target_ts - pd.Timedelta(hours=lag))
        assert out[f"return_lag_{lag}h"] == expected
        assert f"return_lag_{lag}h" not in fallback

    for window in config.ROLLING_WINDOWS:
        idx = pd.date_range(target_ts - pd.Timedelta(hours=window), target_ts - pd.Timedelta(hours=1), freq="h")
        vals = series.reindex(idx)
        assert out[f"return_roll_mean_{window}h"] == pytest.approx(vals.mean())
        assert out[f"return_roll_std_{window}h"] == pytest.approx(vals.std())


def test_return_roll_mean_is_dense_average_over_5min_ticks():
    """history가 5분 tick 정밀도일 때 return_roll_mean_3h는 hourly 지점 3개가 아니라 tick 전부를 평균해야 한다."""
    ps._rental_events_by_station = {}
    ps._rental_events_coverage = None

    ticks = pd.date_range("2025-06-01 00:00", periods=12 * 4, freq="5min")  # 4시간 분량
    return_counts = list(range(len(ticks)))
    _set_history("A", ticks, [0] * len(ticks), return_counts)
    _set_profile({})

    target_ts = ticks[12 * 3]  # 03:00
    out, fallback = ps._lag_rolling_features("A", target_ts)

    expected = sum(return_counts[0:36]) / 36  # [00:00,03:00) 안 tick 36개 전부
    assert out["return_roll_mean_3h"] == pytest.approx(expected)
    assert "return_roll_mean_3h" not in fallback
