"""predict_single.py의 horizon-as-feature 전환(재귀 예측 대체)이 올바른지 검증한다.

핵심 불변조건(history.md 18번 항목 — "horizon을 feature로"):
1. lag/rolling(직전 실적)은 anchor_ts(T0) 기준으로 고정 — horizon이 달라져도 안 바뀐다.
2. 날씨/캘린더/타겟(date/hour/dow/month/is_holiday/is_weekend/is_next_day_off/
   is_prev_day_off/hour_sin~dow_cos)만 target_ts=T0+(horizon-1)시간 기준으로 바뀐다.
3. 배치(`predict_demand_multi_hour*`)는 lag/rolling을 station당 딱 한 번만 계산한다
   (재귀 없음 — 예전에는 h마다 다시 계산/추정했음).
4. 날씨는 스칼라(전체 horizon 재사용) 또는 길이 n_hours 배열(horizon별) 둘 다 받는다.
"""

import pandas as pd
import pytest

from inference import predict_single as ps


def _trip(station, start, end=None):
    return {
        "station_id": station,
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end) if end is not None else pd.NaT,
    }


_EMPTY_POPULATION = pd.DataFrame(columns=["pop_resd", "pop_long_foreign", "pop_short_foreign", "pop_total"])
_EMPTY_BIKE_STATUS = pd.DataFrame(columns=["bike_count", "capacity", "stockout_flag"])


@pytest.fixture(autouse=True)
def _reset_module_caches(monkeypatch):
    names = [
        "_history_by_station",
        "_rental_events_by_station",
        "_rental_events_coverage",
        "_all_rental_events_sorted",
        "_station_profile",
        "_population_profile",
        "_station_master",
        "_holidays",
    ]
    saved = {n: getattr(ps, n) for n in names}
    ps._rental_events_sorted_by_station = {}
    # 이 테스트들은 horizon-as-feature 조립 로직만 검증한다 — population/stockout을 안
    # 준 호출도 S3(MinIO)를 실제로 두드리지 않도록 실시간 조회 함수를 "데이터 없음"으로 고정한다.
    monkeypatch.setattr(ps, "_get_recent_population", lambda target_ts, lookback_hours=3: _EMPTY_POPULATION)
    monkeypatch.setattr(ps, "_get_recent_bike_status", lambda anchor_ts, lookback_hours=1.0: _EMPTY_BIKE_STATUS)
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


def _set_history(station_id: str, hours, rental_counts, return_counts) -> None:
    idx = pd.to_datetime(hours)
    ps._history_by_station = {
        station_id: pd.DataFrame({"rental_count": rental_counts, "return_count": return_counts}, index=idx)
    }


def _set_station_master(station_ids: list[str]) -> None:
    ps._station_master = pd.DataFrame(
        {"capacity": [10.0] * len(station_ids), "lat": [37.5] * len(station_ids), "lon": [127.0] * len(station_ids),
         "grid_id": ["G1"] * len(station_ids)},
        index=pd.Index(station_ids, name="station_id"),
    )


def _fake_predict_echo_temp(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    """ml_common.scoring.predict() 대역 — pred_mean에 그 행의 temp를 그대로 담아 반환해서,
    배치 조립 단계에서 각 horizon에 실제로 어떤 temp 값이 들어갔는지 결과에서 역추적할 수
    있게 한다(날씨 배열 resolve가 올바른지 확인하는 용도)."""
    return pd.DataFrame({
        "station_id": df["station_id"].to_numpy(),
        "date": df["date"].to_numpy(),
        "hour": df["hour"].to_numpy(),
        "pred_mean": df["temp"].to_numpy(dtype=float),
        "pred_p10": df["temp"].to_numpy(dtype=float) - 1,
        "pred_p50": df["temp"].to_numpy(dtype=float),
        "pred_p90": df["temp"].to_numpy(dtype=float) + 1,
    })


def _setup_common(station_id="A"):
    trips = pd.DataFrame([_trip(station_id, "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_history(station_id, pd.date_range("2025-06-01 00:00", periods=400, freq="h"), [0] * 400, [0] * 400)
    ps._station_profile = {}
    ps._population_profile = {}
    _set_station_master([station_id])
    ps._holidays = set()


# --- 1. lag/rolling은 anchor_ts 고정, target 필드만 horizon에 따라 이동 -----------------


def test_build_feature_record_lag_rolling_fixed_across_horizon():
    _setup_common()

    record_h1, fb1, _ = ps._build_feature_record(
        station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
        horizon=1,
    )
    record_h5, fb5, _ = ps._build_feature_record(
        station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
        horizon=5,
    )

    from ml_common.model_contract import LAG_ROLLING_FEATURE_COLUMNS

    for col in LAG_ROLLING_FEATURE_COLUMNS:
        assert record_h1[col] == pytest.approx(record_h5[col]), col
    assert fb1 == fb5  # lag/rolling fallback 판정도 horizon과 무관하게 동일해야 함

    # target 쪽은 horizon=5 -> anchor(08:00)+4시간=12:00으로 이동해야 한다.
    assert record_h1["date"] == "2025-06-10"
    assert record_h1["hour"] == 8
    assert record_h5["date"] == "2025-06-10"
    assert record_h5["hour"] == 12
    assert record_h1["horizon"] == 1
    assert record_h5["horizon"] == 5


def test_build_feature_record_horizon_crossing_midnight_moves_date():
    _setup_common()
    # anchor 23:00 + horizon=3(=+2시간) -> 다음날 01:00로 넘어가야 한다.
    record, _, _ = ps._build_feature_record(
        station_id="A", date="2025-06-10", hour=23, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
        horizon=3,
    )
    assert record["date"] == "2025-06-11"
    assert record["hour"] == 1


def test_build_feature_record_rejects_horizon_out_of_range():
    _setup_common()
    with pytest.raises(ValueError):
        ps._build_feature_record(
            station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
            population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
            horizon=ps.config.HORIZON_COUNT + 1,
        )
    with pytest.raises(ValueError):
        ps._build_feature_record(
            station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
            population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
            horizon=0,
        )


def test_is_next_day_off_and_prev_day_off_reflect_target_ts_not_anchor():
    """2025-06-13은 금요일(다음날 토요일=휴일, 전날 목요일=평일)이다 — anchor를 하루 전(목,
    평일)에 두고 horizon으로 금요일까지 밀면 next/prev_day_off가 target_ts 기준으로
    바뀌어야 한다."""
    _setup_common()
    # anchor: 2025-06-12(목) 00:00. horizon=25 -> target_ts = 2025-06-13(금) 00:00.
    record, _, _ = ps._build_feature_record(
        station_id="A", date="2025-06-12", hour=0, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
        horizon=ps.config.HORIZON_COUNT,  # 12 -> target = 06-12 11:00 (아직 금요일 아님, 목요일)
    )
    assert record["date"] == "2025-06-12"
    assert record["is_next_day_off"] == 0  # 다음날(06-13, 금)은 평일
    assert record["is_prev_day_off"] == 0  # 전날(06-11, 수)도 평일

    # anchor를 금요일 자정으로 바로 잡으면 다음날(토)이 휴일이어야 한다.
    record_fri, _, _ = ps._build_feature_record(
        station_id="A", date="2025-06-13", hour=0, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0, stockout=False,
        horizon=1,
    )
    assert record_fri["is_next_day_off"] == 1  # 다음날 토요일
    assert record_fri["is_prev_day_off"] == 0  # 전날 목요일


# --- 2. predict_demand_multi_hour: lag/rolling 한 번만 계산, 재귀 없음 -------------------


def test_predict_demand_multi_hour_shares_lag_fallback_across_horizons(monkeypatch):
    _setup_common()
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    results = ps.predict_demand_multi_hour(
        station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        n_hours=3,
    )

    assert [r["horizon"] for r in results] == [1, 2, 3]
    assert [r["hour"] for r in results] == [8, 9, 10]
    # lag/rolling(및 그 fallback 판정)은 anchor_ts 한 번 계산 결과를 그대로 재사용하므로
    # 세 horizon 모두 동일해야 한다.
    freshness = {r["rental"]["lag_data_freshness"] for r in results}
    assert len(freshness) == 1


def test_predict_demand_multi_hour_weather_scalar_reused_across_horizons(monkeypatch):
    _setup_common()
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    results = ps.predict_demand_multi_hour(
        station_id="A", date="2025-06-10", hour=8, temp=15.0, precip=0.0, wind=1.0, humidity=50.0,
        n_hours=3,
    )
    assert [r["rental"]["pred_mean"] for r in results] == [15.0, 15.0, 15.0]


def test_predict_demand_multi_hour_weather_array_applied_per_horizon(monkeypatch):
    _setup_common()
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    results = ps.predict_demand_multi_hour(
        station_id="A", date="2025-06-10", hour=8, temp=[10.0, 20.0, 30.0], precip=0.0, wind=1.0, humidity=50.0,
        n_hours=3,
    )
    assert [r["rental"]["pred_mean"] for r in results] == [10.0, 20.0, 30.0]


def test_predict_demand_multi_hour_weather_array_length_mismatch_raises(monkeypatch):
    _setup_common()
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    with pytest.raises(ValueError):
        ps.predict_demand_multi_hour(
            station_id="A", date="2025-06-10", hour=8, temp=[10.0, 20.0], precip=0.0, wind=1.0, humidity=50.0,
            n_hours=3,
        )


def test_predict_demand_multi_hour_rejects_n_hours_out_of_range(monkeypatch):
    _setup_common()
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)
    with pytest.raises(ValueError):
        ps.predict_demand_multi_hour(
            station_id="A", date="2025-06-10", hour=8, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
            n_hours=ps.config.HORIZON_COUNT + 1,
        )


# --- 3. predict_demand_multi_hour_all_stations: station당 lag/rolling 한 번, 배열 날씨 --


def test_predict_demand_multi_hour_all_stations_weather_array_and_shared_lag(monkeypatch):
    _setup_common("A")
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-10", hour=8, temp=[11.0, 22.0], precip=0.0, wind=1.0, humidity=50.0,
        station_ids=["A"], n_hours=2,
    )

    assert outcome["failed"] == []
    assert outcome["expected_count"] == outcome["actual_count"] == 2
    by_horizon = {r["horizon"]: r for r in outcome["results"]}
    assert by_horizon[1]["rental"]["pred_mean"] == pytest.approx(11.0)
    assert by_horizon[2]["rental"]["pred_mean"] == pytest.approx(22.0)
    # station당 lag/rolling을 한 번만 계산하므로 두 horizon의 fallback 상태가 같아야 한다.
    assert by_horizon[1]["rental"]["lag_fallback_used"] == by_horizon[2]["rental"]["lag_fallback_used"]


def test_predict_demand_multi_hour_all_stations_partial_failure_reports_failed_and_counts(monkeypatch):
    """station "B"는 station_master에 없어 lag/rolling 계산 단계에서 예외가 나야 한다 —
    그래도 station "A"의 예측은 정상 반환되고, "B"는 전체 horizon이 통째로 "failed"에
    쌓이며 actual_count가 expected_count보다 작아져야 한다(부분실패 계약)."""
    _setup_common("A")
    monkeypatch.setattr(ps, "predict", _fake_predict_echo_temp)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-10", hour=8, temp=[11.0, 22.0], precip=0.0, wind=1.0, humidity=50.0,
        station_ids=["A", "B"], n_hours=2,
    )

    assert outcome["expected_count"] == 4  # 2 station * 2 horizon
    assert outcome["actual_count"] == 2  # "B"의 horizon 2개가 통째로 빠짐
    assert [r["station_id"] for r in outcome["results"]] == ["A", "A"]

    assert len(outcome["failed"]) == 1
    failure = outcome["failed"][0]
    assert failure["station_id"] == "B"
    assert failure["n_hours_skipped"] == 2
    assert "ValueError" in failure["error"]
