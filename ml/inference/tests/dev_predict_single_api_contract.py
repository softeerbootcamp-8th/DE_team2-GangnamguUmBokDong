"""predict_single.py의 두 가지 API 계약을 검증한다 (PR 리뷰에서 지적된 문제 재발 방지).

1. **5분 tick 단위 시각 지정** — 공개 API가 `date`+`hour`만 받으면 17:05/17:10/17:15
   같은 요청을 전부 17:00 기준으로 뭉개 계산하게 된다(feature_engineering의 학습
   그리드는 5분 tick인데 서빙 인터페이스가 그 정밀도를 못 받는 문제). `minute`
   인자가 실제로 target_ts/lag·rolling 앵커에 반영되는지 확인한다.
2. **배치 실패의 완결성 계약** — `predict_demand_multi_hour_all_stations()`가 일부
   station 실패를 조용히 skip하면 downstream(Gold 적재 등)이 "전체 성공"과
   "일부 누락"을 구분할 수 없다. 반환값에 실패 station 목록/기대·실제 건수가
   제대로 채워지는지 확인한다.
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
    """predict_single.py의 모듈 전역 캐시를 각 테스트 전후로 리셋한다 (station_master/
    holidays/population_profile까지 — dev_predict_single_rental_censoring.py의 목록에
    이 세 개를 추가로 리셋해야 이 파일의 테스트가 다른 테스트를 오염시키지 않는다).

    population/stockout을 생략한 호출(`predict_demand_multi_hour_all_stations`는
    population을 아예 받지 않아 항상 생략된 것과 같다)도 S3(MinIO)를 실제로 두드리지
    않도록 실시간 조회 함수를 "데이터 없음"으로 고정해 이 파일의 테스트를 hermetic하게 둔다."""
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


def _fake_predict(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    """ml_common.scoring.predict() 대역 — 실제 학습된 booster 파일 없이 배치 조립/실패
    처리 로직만 검증하려고 항상 고정값을 낸다."""
    return pd.DataFrame({
        "station_id": df["station_id"].to_numpy(),
        "date": df["date"].to_numpy(),
        "hour": df["hour"].to_numpy(),
        "pred_mean": 1.0,
        "pred_p10": 0.5,
        "pred_p50": 1.0,
        "pred_p90": 1.5,
    })


# --- 1. 5분 tick 단위 시각 지정 ---------------------------------------------------


def test_target_timestamp_combines_date_hour_minute():
    assert ps._target_timestamp("2025-06-01", 17, 35) == pd.Timestamp("2025-06-01 17:35:00")
    assert ps._target_timestamp("2025-06-01", 17) == pd.Timestamp("2025-06-01 17:00:00")  # minute 기본값 0


@pytest.mark.parametrize("hour,minute", [(24, 0), (-1, 0), (17, 7), (17, 60), (17, -5)])
def test_target_timestamp_rejects_out_of_range(hour, minute):
    """hour는 0~23, minute은 GRID_TICK_MINUTES(5분)의 배수(0~59)여야 한다."""
    with pytest.raises(ValueError):
        ps._target_timestamp("2025-06-01", hour, minute)


def test_lag_rolling_features_differ_by_minute_within_same_hour():
    """17:00과 17:05는 '같은 hour'지만 다른 5분 tick 앵커라 lag/rolling 값이 달라야 한다.

    hour만 받는 인터페이스라면 이 두 요청이 구분 불가능했을 것 — minute이 실제로
    앵커에 반영된다는 걸 값 차이로 직접 보인다.
    """
    # censoring 윈도우 = [T-90분, T-30분). T=17:00 -> [15:30,16:30), T=17:05 -> [15:35,16:35).
    # start_dt=16:32는 후자에만 들어간다 — 두 anchor가 정확히 이 트립 하나 때문에 갈린다.
    trips = pd.DataFrame([_trip("A", "2025-06-01 16:32:00", "2025-06-01 16:40:00")])
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    ps._station_profile = {}

    out_1700, fb_1700 = ps._lag_rolling_features("A", pd.Timestamp("2025-06-01 17:00:00"))
    out_1705, fb_1705 = ps._lag_rolling_features("A", pd.Timestamp("2025-06-01 17:05:00"))

    assert not ({"rental_lag_1h"} & set(fb_1700 + fb_1705))  # 둘 다 fallback 없이 계산됨
    assert out_1700["rental_lag_1h"] != out_1705["rental_lag_1h"]


def test_build_feature_record_honors_minute():
    """_build_feature_record(hour=17, minute=35)이 실제로 target_ts=17:35를 앵커로 써서
    _lag_rolling_features를 직접 부른 것과 같은 값을 내는지 확인한다 — 공개 API
    (predict_rental_demand 등)가 이 함수를 감싸기만 하므로, 여기서 맞으면 그 위층도
    그대로 맞는다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 17:10:00", "2025-06-01 17:20:00")])
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    ps._station_profile = {}
    _set_station_master(["A"])
    ps._holidays = set()

    record, fallback, population_fallback = ps._build_feature_record(
        station_id="A", date="2025-06-01", hour=17, minute=35,
        temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0,
        stockout=False,
    )

    target_ts = pd.Timestamp("2025-06-01 17:35:00")
    expected, expected_fallback = ps._lag_rolling_features("A", target_ts)

    assert record["rental_lag_1h"] == pytest.approx(expected["rental_lag_1h"])
    assert record["rental_roll_mean_3h"] == pytest.approx(expected["rental_roll_mean_3h"])
    assert set(fallback) == set(expected_fallback)
    assert population_fallback is False  # population을 명시적으로 줬으므로


def test_build_feature_record_rejects_invalid_minute():
    _set_station_master(["A"])
    ps._holidays = set()
    with pytest.raises(ValueError):
        ps._build_feature_record(
            station_id="A", date="2025-06-01", hour=17, minute=7,  # 5분 배수 아님
            temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
            population=3000.0, pop_resd=None, pop_long_foreign=0.0, pop_short_foreign=0.0,
            stockout=False,
        )


# --- 2. 배치 실패의 완결성 계약 -----------------------------------------------------


def test_predict_demand_multi_hour_all_stations_reports_failed_stations(monkeypatch):
    """station_master에 없는 station_id를 섞으면 그 station만 실패로 기록되고, 전체는
    죽지 않으며, 반환값에서 기대/실제 건수와 실패 목록으로 partial 여부를 알 수 있어야
    한다 (기존엔 print(stderr)만 하고 조용히 skip — downstream이 완결성을 알 방법이 없었음)."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    ps._station_profile = {}
    ps._population_profile = {}
    _set_station_master(["A"])  # "MISSING"은 일부러 마스터에 안 넣음
    ps._holidays = set()
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        station_ids=["A", "MISSING"], n_hours=1,
    )

    assert outcome["expected_count"] == 2
    assert outcome["actual_count"] == 1
    assert outcome["actual_count"] < outcome["expected_count"]  # partial 여부를 이 비교만으로 알 수 있음
    assert [r["station_id"] for r in outcome["results"]] == ["A"]
    assert [f["station_id"] for f in outcome["failed"]] == ["MISSING"]
    assert "알 수 없는 station_id" in outcome["failed"][0]["error"]


def test_predict_demand_multi_hour_all_stations_no_failures_when_all_known(monkeypatch):
    """실패가 하나도 없으면 expected_count == actual_count이고 failed는 비어 있어야 한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_history("A", pd.date_range("2025-06-01 00:00", periods=200, freq="h"), [0] * 200, [0] * 200)
    ps._station_profile = {}
    ps._population_profile = {}
    _set_station_master(["A"])
    ps._holidays = set()
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0, wind=1.0, humidity=50.0,
        station_ids=["A"], n_hours=1,
    )

    assert outcome["failed"] == []
    assert outcome["expected_count"] == outcome["actual_count"] == 1
