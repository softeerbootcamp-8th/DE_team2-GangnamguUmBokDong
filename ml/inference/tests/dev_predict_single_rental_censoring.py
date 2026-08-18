"""predict_single.py의 대여(rental_lag_1h)/반납(return_lag_1h) 계산 검증.

predict_single.py의 캐시는 모듈 전역 변수라, 파일 I/O 없이 합성 트립/히스토리를
직접 주입해서 테스트한다 (tests/dev_rolling_window_features.py의 `_trip()` 합성
fixture 패턴 재사용). 각 테스트 후 전역을 리셋해 테스트 간 오염을 막는다.

피처 축소 이후 lag는 한쪽당 1개(`rental_lag_1h`/`return_lag_1h`)뿐이다 — 예전
lag_24h/168h, roll_mean/std_3h/24h 테스트는 해당 컬럼 자체가 없어져 제거했다.
"""

import pandas as pd
import pytest
from ml_core.rolling_window_features import count_visible_in_window

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


def _set_return_history(station_id: str, point, return_count: float) -> None:
    """`_get_history_by_station()`이 캐시하는 형태 그대로 — 정확히 [target_ts-1시간]
    시점 하나만 담은 1행 DataFrame."""
    ps._history_by_station = {station_id: pd.DataFrame({"return_count": [return_count]}, index=[pd.Timestamp(point)])}


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
    _set_return_history("A", "2025-06-01 09:00:00", 0.0)
    _set_profile({})

    target_ts = pd.Timestamp("2025-06-01 10:00:00")
    out, fallback = ps._lag_rolling_features("A", target_ts)

    expected = count_visible_in_window(
        trips, as_of=target_ts, window_minutes=config.ROLLING_WINDOW_MINUTES, embargo_minutes=config.ROLLING_EMBARGO_MINUTES
    ).get("A", 0)

    assert out["rental_lag_1h"] == float(expected)
    assert "rental_lag_1h" not in fallback


def test_rental_recent_zero_trips_in_window_is_not_fallback():
    """윈도우 커버리지 안이지만 그 구간에 트립이 0건이면 정상값 0이지 fallback이 아니다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 06:00:00", "2025-06-01 06:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00", 0.0)
    _set_profile({})

    target_ts = pd.Timestamp("2025-06-01 10:00:00")  # 윈도우 [08:20,09:20) 안에 트립 없음
    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert out["rental_lag_1h"] == 0.0
    assert "rental_lag_1h" not in fallback


def test_rental_recent_fallback_when_target_outside_coverage():
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips, coverage=(pd.Timestamp("2025-06-01"), pd.Timestamp("2025-06-01 23:59:59")))
    _set_return_history("A", "2026-08-01 07:00:00", 0.0)
    _set_profile(
        {
            ("A", h, dow, m): {"rental_mean": 7.0, "rental_std": 1.5, "return_mean": 3.0, "return_std": 0.5}
            for h in range(24)
            for dow in range(7)
            for m in range(1, 13)
        }
    )

    target_ts = pd.Timestamp("2026-08-01 08:00:00")  # 트립 커버리지(2025-06-01 하루)와 전혀 안 겹침
    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert "rental_lag_1h" in fallback
    assert out["rental_lag_1h"] == pytest.approx(7.0)


def test_return_lag_1h_uses_exactly_one_hour_ago_history():
    """return_lag_1h는 _get_history_by_station()이 담아둔 [target_ts-1시간] 값을 그대로 쓴다."""
    target_ts = pd.Timestamp("2025-06-01 10:00:00")
    _set_rental_events(pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")]))
    _set_return_history("A", target_ts - pd.Timedelta(hours=1), 4.0)
    _set_profile({})

    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert out["return_lag_1h"] == 4.0
    assert "return_lag_1h" not in fallback


def test_return_lag_1h_falls_back_to_profile_when_missing():
    """history 캐시에 그 station이 아예 없으면(예: 반납 트립 자체가 없던 station) profile로 대체한다."""
    target_ts = pd.Timestamp("2025-06-01 10:00:00")
    _set_rental_events(pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")]))
    ps._history_by_station = {}  # station "A"에 대한 return_count 자체가 없음
    _set_profile(
        {
            ("A", h, dow, m): {"rental_mean": 0.0, "rental_std": 0.0, "return_mean": 9.0, "return_std": 2.0}
            for h in range(24)
            for dow in range(7)
            for m in range(1, 13)
        }
    )

    out, fallback = ps._lag_rolling_features("A", target_ts)

    assert "return_lag_1h" in fallback
    assert out["return_lag_1h"] == pytest.approx(9.0)
