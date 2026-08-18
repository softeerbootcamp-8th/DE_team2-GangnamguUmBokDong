"""stockout이 실시간 데이터 없이 조용히 "품절 아님"으로 기본값 처리되던 문제의
회귀 테스트 — `population_source`처럼 `stockout_source`로 그 여부가 항상 드러나야
한다(`_stockout_from_status()` 참고, `predict_single.py` 모듈 docstring 참고).
"""

import pandas as pd
import pytest

from inference import predict_single as ps

_EMPTY_POPULATION = pd.DataFrame(columns=["pop_total"])
_EMPTY_BIKE_STATUS = pd.DataFrame(columns=["bike_count", "capacity", "stockout_flag"])
_BIKE_STATUS_WITH_A = pd.DataFrame(
    {"bike_count": [0], "capacity": [10], "stockout_flag": [1]}, index=pd.Index(["A"], name="station_id")
)


@pytest.fixture(autouse=True)
def _reset_module_caches(monkeypatch):
    names = [
        "_history_by_station", "_rental_events_by_station", "_rental_events_coverage",
        "_all_rental_events_sorted", "_station_profile", "_population_profile",
        "_station_master", "_holidays_by_year",
    ]
    saved = {n: getattr(ps, n) for n in names}
    ps._rental_events_sorted_by_station = {}
    monkeypatch.setattr(ps, "_get_recent_population", lambda target_ts, lookback_hours=3: _EMPTY_POPULATION)
    yield
    for n, v in saved.items():
        setattr(ps, n, v)
    ps._rental_events_sorted_by_station = {}


def _set_rental_events(trips: pd.DataFrame) -> None:
    ps._rental_events_by_station = {
        sid: g[["station_id", "start_dt", "end_dt"]].reset_index(drop=True) for sid, g in trips.groupby("station_id")
    }
    ps._rental_events_coverage = (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31 23:59:59"))
    order = trips["start_dt"].to_numpy().argsort(kind="mergesort")
    ps._all_rental_events_sorted = (
        trips["station_id"].to_numpy()[order],
        trips["start_dt"].to_numpy()[order],
        trips["end_dt"].to_numpy()[order],
    )


def _set_return_history(station_id: str, point, return_count: float = 0.0) -> None:
    ps._history_by_station = {station_id: pd.DataFrame({"return_count": [return_count]}, index=[pd.Timestamp(point)])}


def _set_station_master(station_ids: list[str]) -> None:
    ps._station_master = pd.DataFrame(
        {"capacity": [10.0] * len(station_ids), "lat": [37.5] * len(station_ids), "lon": [127.0] * len(station_ids),
         "grid_id": ["G1"] * len(station_ids)},
        index=pd.Index(station_ids, name="station_id"),
    )


def _fake_predict(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    return pd.DataFrame({
        "station_id": df["station_id"].to_numpy(),
        "date": df["date"].to_numpy(),
        "hour": df["hour"].to_numpy(),
        "pred_mean": 1.0, "pred_p10": 0.5, "pred_p50": 1.0, "pred_p90": 1.5,
    })


def _setup_single_station(station_id: str = "A") -> None:
    trips = pd.DataFrame([{
        "station_id": station_id,
        "start_dt": pd.Timestamp("2025-06-01 09:00:00"),
        "end_dt": pd.Timestamp("2025-06-01 09:05:00"),
    }])
    _set_rental_events(trips)
    _set_return_history(station_id, "2025-06-01 09:00:00")
    ps._station_profile = {}
    ps._population_profile = {}
    _set_station_master([station_id])
    ps._holidays_by_year = {2025: set()}


# --- 순수 로직: _stockout_from_status --------------------------------------------


def test_stockout_from_status_uses_provided_value_without_fallback():
    value, fallback = ps._stockout_from_status("A", None, True)
    assert (value, fallback) == (True, False)


def test_stockout_from_status_reads_from_status_when_present():
    status = pd.DataFrame({"stockout_flag": [1]}, index=pd.Index(["A"], name="station_id"))
    value, fallback = ps._stockout_from_status("A", status, None)
    assert (value, fallback) == (True, False)


def test_stockout_from_status_falls_back_when_station_missing_from_status():
    """station이 실시간 재고 현황에 아예 없으면 '품절 아님'으로 기본값을 쓰되,
    fallback=True로 그 사실이 드러나야 한다 — 이게 조용히 사라지면 rental_exposure가
    실제로는 품절이었을 시간대에도 1.0(정상)으로 들어가 수요를 과대평가하게 된다."""
    status = pd.DataFrame({"stockout_flag": [1]}, index=pd.Index(["OTHER"], name="station_id"))
    value, fallback = ps._stockout_from_status("A", status, None)
    assert (value, fallback) == (False, True)


def test_stockout_from_status_falls_back_when_status_is_none():
    value, fallback = ps._stockout_from_status("A", None, None)
    assert (value, fallback) == (False, True)


# --- 공개 API 통합: stockout_source가 실제로 반환값에 실리는지 ---------------------


def test_predict_rental_demand_reports_fallback_when_no_live_stockout_data(monkeypatch):
    _setup_single_station("A")
    monkeypatch.setattr(ps, "predict", _fake_predict)
    monkeypatch.setattr(ps, "_get_recent_bike_status", lambda anchor_ts, lookback_hours=1.0: _EMPTY_BIKE_STATUS)

    result = ps.predict_rental_demand(station_id="A", date="2025-06-01", hour=10, temp=20.0, precip=0.0)

    assert result["stockout_source"] == "fallback"


def test_predict_rental_demand_reports_provided_when_stockout_given_explicitly(monkeypatch):
    _setup_single_station("A")
    monkeypatch.setattr(ps, "predict", _fake_predict)
    # 실시간 조회 자체가 호출되면 실패하게 만들어서, stockout을 직접 줬을 때 실제로
    # 조회를 건너뛰는지까지 같이 확인한다.
    monkeypatch.setattr(
        ps, "_get_recent_bike_status",
        lambda anchor_ts, lookback_hours=1.0: (_ for _ in ()).throw(AssertionError("호출되면 안 됨")),
    )

    result = ps.predict_rental_demand(
        station_id="A", date="2025-06-01", hour=10, temp=20.0, precip=0.0, stockout=True,
    )

    assert result["stockout_source"] == "provided"


def test_predict_demand_multi_hour_all_stations_reports_fallback_per_station(monkeypatch):
    """정류소마다 실시간 재고 현황 유무가 다를 수 있다 — B는 fallback, A는 아님을
    같은 호출 안에서 station별로 정확히 구분해야 한다."""
    trips = pd.DataFrame([
        {"station_id": sid, "start_dt": pd.Timestamp("2025-06-01 09:00:00"), "end_dt": pd.Timestamp("2025-06-01 09:05:00")}
        for sid in ("A", "B")
    ])
    _set_rental_events(trips)
    point = pd.Timestamp("2025-06-01 09:00:00")
    ps._history_by_station = {
        sid: pd.DataFrame({"return_count": [0.0]}, index=[point]) for sid in ("A", "B")
    }
    ps._station_profile = {}
    ps._population_profile = {}
    _set_station_master(["A", "B"])
    ps._holidays_by_year = {2025: set()}
    monkeypatch.setattr(ps, "predict", _fake_predict)
    # A만 실시간 현황에 있고 B는 없다.
    monkeypatch.setattr(ps, "_get_recent_bike_status", lambda anchor_ts, lookback_hours=1.0: _BIKE_STATUS_WITH_A)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0,
        station_ids=["A", "B"], n_hours=1,
    )

    by_station = {r["station_id"]: r["stockout_source"] for r in outcome["results"]}
    assert by_station == {"A": "provided", "B": "fallback"}
