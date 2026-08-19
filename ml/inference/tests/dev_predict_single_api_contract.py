"""predict_single.py의 두 가지 API 계약을 검증한다 (PR 리뷰에서 지적된 문제 재발 방지).

1. **tick 단위 시각 지정** — 공개 API가 `date`+`hour`만 받으면 그 시간 안의 5분
   tick(예: 17:00/17:05/.../17:55) 요청을 전부 정시 기준으로
   뭉개 계산하게 된다(feature_engine의 학습 그리드가 그 tick 단위인데 서빙
   인터페이스가 그 정밀도를 못 받는 문제). `minute` 인자가 실제로 target_ts/lag
   앵커에 반영되는지 확인한다.
2. **배치 실패의 완결성 계약** — `predict_demand_multi_hour_all_stations()`가 일부
   station 실패를 조용히 skip하면 downstream(Gold 적재 등)이 "전체 성공"과
   "일부 누락"을 구분할 수 없다. 반환값에 실패 station 목록/기대·실제 건수가
   제대로 채워지는지 확인한다.
"""

import numpy as np
import pandas as pd
import pytest

from inference import predict_single as ps


def _trip(station, start, end=None):
    return {
        "station_id": station,
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end) if end is not None else pd.NaT,
    }


_EMPTY_POPULATION = pd.DataFrame(columns=["pop_total"])
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
        "_station_profile_station_index",
        "_station_profile_values",
        "_population_profile",
        "_station_master",
        "_holidays_by_year",
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


def _set_return_history(station_id: str, point, return_count: float = 0.0) -> None:
    """`_get_history_by_station()`이 캐시하는 형태 그대로 — 정확히 [target_ts-1시간]
    시점 하나만 담은 1행 DataFrame."""
    ps._history_by_station = {station_id: pd.DataFrame({"return_count": [return_count]}, index=[pd.Timestamp(point)])}


def _set_station_master(station_ids: list[str]) -> None:
    ps._station_master = pd.DataFrame(
        {"station_no": list(range(1, len(station_ids) + 1)),
         "capacity": [10.0] * len(station_ids), "lat": [37.5] * len(station_ids), "lon": [127.0] * len(station_ids),
         "grid_id": ["G1"] * len(station_ids)},
        index=pd.Index(station_ids, name="station_id"),
    )


def _fake_predict(df: pd.DataFrame, model_name: str, exposure_col: str | None = None) -> pd.DataFrame:
    """ml_core.scoring.predict() 대역 — 실제 학습된 booster 파일 없이 배치 조립/실패
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


# --- 1. tick 단위 시각 지정 ---------------------------------------------------


def test_target_timestamp_combines_date_hour_minute():
    assert ps._target_timestamp("2025-06-01", 17, 5) == pd.Timestamp("2025-06-01 17:05:00")
    assert ps._target_timestamp("2025-06-01", 17) == pd.Timestamp("2025-06-01 17:00:00")  # minute 기본값 0


@pytest.mark.parametrize("hour,minute", [(24, 0), (-1, 0), (17, 7), (17, 60), (17, -5)])
def test_target_timestamp_rejects_out_of_range(hour, minute):
    """hour는 0~23, minute은 GRID_TICK_MINUTES의 배수(0~59)여야 한다."""
    with pytest.raises(ValueError):
        ps._target_timestamp("2025-06-01", hour, minute)


def test_lag_rolling_features_differ_by_minute_within_same_hour():
    """17:00과 17:05는 '같은 hour'지만 다른 tick 앵커라 rental_lag_1h 값이 달라야 한다.

    hour만 받는 인터페이스라면 이 두 요청이 구분 불가능했을 것 — minute이 실제로
    앵커에 반영된다는 걸 값 차이로 직접 보인다.
    """
    # censoring 윈도우 = [T-100분, T-40분). T=17:00 -> [15:20,16:20), T=17:05 -> [15:25,16:25).
    # start_dt=16:22는 후자에만 들어간다 — 5분 차이가 실제 feature 차이로 이어져야 한다.
    trips = pd.DataFrame([_trip("A", "2025-06-01 16:22:00", "2025-06-01 16:30:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 16:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")

    out_1700, fb_1700 = ps._lag_rolling_features("A", 1, pd.Timestamp("2025-06-01 17:00:00"))
    out_1705, fb_1705 = ps._lag_rolling_features("A", 1, pd.Timestamp("2025-06-01 17:05:00"))

    assert not ({"rental_lag_1h"} & set(fb_1700 + fb_1705))  # 둘 다 fallback 없이 계산됨
    assert out_1700["rental_lag_1h"] != out_1705["rental_lag_1h"]


def test_build_feature_record_honors_minute():
    """_build_feature_record(hour=17, minute=5)가 실제로 target_ts=17:05를 앵커로 써서
    _lag_rolling_features를 직접 부른 것과 같은 값을 내는지 확인한다 — 공개 API
    (predict_rental_demand 등)가 이 함수를 감싸기만 하므로, 여기서 맞으면 그 위층도
    그대로 맞는다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 16:00:00", "2025-06-01 16:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 16:05:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    _set_station_master(["A"])

    record, fallback, population_fallback = ps._build_feature_record(
        station_id="A", date="2025-06-01", hour=17, minute=5,
        temp=20.0, precip=0.0, population=3000.0, stockout=False,
    )

    target_ts = pd.Timestamp("2025-06-01 17:05:00")
    expected, expected_fallback = ps._lag_rolling_features("A", 1, target_ts)

    assert record["rental_lag_1h"] == pytest.approx(expected["rental_lag_1h"])
    assert set(fallback) == set(expected_fallback)
    assert population_fallback is False  # population을 명시적으로 줬으므로


def test_build_feature_record_rejects_invalid_minute():
    _set_station_master(["A"])
    with pytest.raises(ValueError):
        ps._build_feature_record(
            station_id="A", date="2025-06-01", hour=17, minute=7,  # GRID_TICK_MINUTES 배수 아님
            temp=20.0, precip=0.0, population=3000.0, stockout=False,
        )


# --- 2. 배치 실패의 완결성 계약 -----------------------------------------------------


def test_predict_demand_multi_hour_all_stations_reports_failed_stations(monkeypatch):
    """station_master에 없는 station_id를 섞으면 그 station만 실패로 기록되고, 전체는
    죽지 않으며, 반환값에서 기대/실제 건수와 실패 목록으로 partial 여부를 알 수 있어야
    한다 (기존엔 print(stderr)만 하고 조용히 skip — downstream이 완결성을 알 방법이 없었음)."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A"])  # "MISSING"은 일부러 마스터에 안 넣음
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0,
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
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A"])
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0,
        station_ids=["A"], n_hours=1,
    )

    assert outcome["failed"] == []
    assert outcome["expected_count"] == outcome["actual_count"] == 1


@pytest.mark.parametrize(
    ("column", "bad_value"),
    [
        ("station_no", "broken"),
        ("capacity", np.nan),
        ("lat", np.inf),
        ("lon", 0.0),
        ("grid_id", "  "),
    ],
)
def test_predict_demand_multi_hour_all_stations_isolates_malformed_master_row(
    monkeypatch, column, bad_value
):
    """깨진 마스터 필드 하나가 전체 배치를 중단하지 않고 해당 station만 실패시킨다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A", "BROKEN"])
    if column == "station_no":
        ps._station_master["station_no"] = ps._station_master["station_no"].astype(object)
    ps._station_master.loc["BROKEN", column] = bad_value
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0,
        station_ids=["A", "BROKEN"], n_hours=1,
    )

    assert outcome["expected_count"] == 2
    assert outcome["actual_count"] == 1
    assert [row["station_id"] for row in outcome["results"]] == ["A"]
    assert [failure["station_id"] for failure in outcome["failed"]] == ["BROKEN"]
    assert column in outcome["failed"][0]["error"]


def test_default_all_stations_reports_malformed_station_no_instead_of_omitting_it(monkeypatch):
    """기본 station 선택도 파싱 불가 station_no를 조용히 필터링하지 않고 failed로 남긴다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A", "BROKEN"])
    ps._station_master["station_no"] = ps._station_master["station_no"].astype(object)
    ps._station_master.loc["BROKEN", "station_no"] = "broken"
    monkeypatch.setattr(ps, "load_station_dtype", lambda model_name: pd.CategoricalDtype(categories=[1]))
    monkeypatch.setattr(ps, "predict", _fake_predict)

    outcome = ps.predict_demand_multi_hour_all_stations(
        date="2025-06-01", hour=10, temp=20.0, precip=0.0, n_hours=1
    )

    assert outcome["expected_count"] == 2
    assert outcome["actual_count"] == 1
    assert [failure["station_id"] for failure in outcome["failed"]] == ["BROKEN"]


def test_all_stations_releases_rental_boosters_before_return_scoring(monkeypatch):
    """연속 호출도 각 모델 채점 뒤 캐시를 비워 booster 8개 동시 상주를 막는다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A"])

    events = []

    def _record_predict(df, model_name, exposure_col=None):
        events.append(f"predict:{model_name}")
        return _fake_predict(df, model_name, exposure_col)

    monkeypatch.setattr(ps, "predict", _record_predict)
    monkeypatch.setattr(ps.scoring_io.load_boosters, "cache_clear", lambda: events.append("cache_clear"))
    monkeypatch.setattr(ps.gc, "collect", lambda: events.append("gc"))

    for _ in range(2):
        ps.predict_demand_multi_hour_all_stations(
            date="2025-06-01", hour=10, temp=20.0, precip=0.0,
            station_ids=["A"], n_hours=1,
        )

    expected_cycle = ["predict:rental", "cache_clear", "gc", "predict:return", "cache_clear", "gc"]
    assert events == expected_cycle * 2


def test_multi_hour_releases_rental_boosters_before_return_scoring(monkeypatch):
    """단일 정류소 다중 horizon 경로도 return 채점 전 rental booster를 해제한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["A"])

    events = []

    def _record_predict(df, model_name, exposure_col=None):
        events.append(f"predict:{model_name}")
        return _fake_predict(df, model_name, exposure_col)

    monkeypatch.setattr(ps, "predict", _record_predict)
    monkeypatch.setattr(ps.scoring_io.load_boosters, "cache_clear", lambda: events.append("cache_clear"))
    monkeypatch.setattr(ps.gc, "collect", lambda: events.append("gc"))

    ps.predict_demand_multi_hour(
        station_id="A", date="2025-06-01", hour=10,
        temp=20.0, precip=0.0, n_hours=1,
    )

    assert events == ["predict:rental", "cache_clear", "gc", "predict:return", "cache_clear", "gc"]


def test_public_single_apis_release_boosters_after_each_score(monkeypatch):
    """공개 단건 API와 이를 쓰는 기본 CLI 경로가 모델별 booster를 즉시 해제한다."""
    trips = pd.DataFrame([_trip("A", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("A", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    _set_station_master(["A"])
    events = []

    def _record_predict(df, model_name, exposure_col=None):
        events.append(f"predict:{model_name}")
        return _fake_predict(df, model_name, exposure_col)

    monkeypatch.setattr(ps, "predict", _record_predict)
    monkeypatch.setattr(ps.scoring_io.load_boosters, "cache_clear", lambda: events.append("cache_clear"))
    monkeypatch.setattr(ps.gc, "collect", lambda: events.append("gc"))

    ps.predict_rental_demand(
        "A", "2025-06-01", 10, temp=20.0, precip=0.0, population=100.0, stockout=False
    )
    ps.predict_return_demand(
        "A", "2025-06-01", 10, temp=20.0, precip=0.0, population=100.0
    )

    assert events == ["predict:rental", "cache_clear", "gc", "predict:return", "cache_clear", "gc"]


def test_single_station_cli_saves_to_s3(monkeypatch):
    """단일 정류소 CLI 실행 시 single_prediction_key 경로로 parquet이 저장되는지 검증한다."""
    from core import s3 as s3_io

    trips = pd.DataFrame([_trip("ST-100", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("ST-100", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["ST-100"])
    monkeypatch.setattr(ps, "predict", _fake_predict)

    saved_calls = []
    monkeypatch.setattr(s3_io, "write_parquet", lambda df, key: saved_calls.append((df, key)))

    argv = [
        "--station-id", "ST-100",
        "--date", "2025-06-01",
        "--hour", "10",
        "--minute", "0",
        "--temp", "20.0",
        "--precip", "0.0",
        "--stockout",
    ]

    ps.main(argv)

    assert len(saved_calls) == 1
    df, key = saved_calls[0]
    assert key == "predictions/single/dt=2025-06-01/hh=10/ST-100_1000.parquet"
    assert len(df) == 1
    assert df["station_id"].iloc[0] == "ST-100"
    assert "rental_pred_mean" in df.columns
    assert "return_pred_mean" in df.columns


def _fake_all_stations_outcome(expected: int, actual: int) -> dict:
    results = [
        {
            "station_id": f"S{i}", "date": "2025-06-01", "hour": 10, "minute": 0, "horizon": 1,
            "rental": {"pred_mean": 1.0, "pred_p10": 0.5, "pred_p50": 1.0, "pred_p90": 1.5, "lag_data_freshness": 1.0},
            "return": {"pred_mean": 1.0, "pred_p10": 0.5, "pred_p50": 1.0, "pred_p90": 1.5},
            "population_source": "provided", "stockout_source": "provided",
        }
        for i in range(actual)
    ]
    failed = [{"station_id": f"F{i}", "error": "boom"} for i in range(expected - actual)]
    return {"results": results, "failed": failed, "expected_count": expected, "actual_count": actual}


def _run_all_stations_cli(monkeypatch, outcome: dict) -> tuple[int, list, list]:
    from core import s3 as s3_io

    monkeypatch.setattr(ps, "predict_demand_multi_hour_all_stations", lambda **kwargs: outcome)
    parquet_calls, json_calls = [], []
    monkeypatch.setattr(s3_io, "write_parquet", lambda df, key: parquet_calls.append((df, key)))
    monkeypatch.setattr(s3_io, "write_json", lambda key, data: json_calls.append((key, data)))

    argv = ["--all-stations", "--date", "2025-06-01", "--hour", "10", "--temp", "20.0", "--precip", "0.0"]
    try:
        ps.main(argv)
        code = 0
    except SystemExit as e:
        code = e.code
    return code, parquet_calls, json_calls


def test_all_stations_cli_exits_zero_on_full_success(monkeypatch):
    code, parquet_calls, json_calls = _run_all_stations_cli(monkeypatch, _fake_all_stations_outcome(3, 3))

    assert code == 0
    assert len(parquet_calls) == 1  # 성공 결과는 항상 저장
    assert len(json_calls) == 1
    assert json_calls[0][1] == []  # 이전 partial 실행의 stale sidecar를 명시적으로 정리


def test_all_stations_cli_exits_one_on_partial_failure_and_writes_diagnostics(monkeypatch):
    """부분 결과는 parquet/sidecar로 진단 가능하게 남기되 downstream 적재를 막는다."""
    code, parquet_calls, json_calls = _run_all_stations_cli(monkeypatch, _fake_all_stations_outcome(3, 2))

    assert code == 1
    assert len(parquet_calls) == 1
    assert len(json_calls) == 1  # 실패 1건은 별도 파일로 남음
    _failed_key, failed_data = json_calls[0]
    assert len(failed_data) == 1


def test_all_stations_cli_exits_one_on_total_failure(monkeypatch):
    """완전 실패는 기존 정상 parquet을 빈 결과로 덮어쓰지 않고 exit 1로 막는다."""
    code, parquet_calls, json_calls = _run_all_stations_cli(monkeypatch, _fake_all_stations_outcome(3, 0))

    assert code == 1
    assert parquet_calls == []
    assert len(json_calls) == 1


def test_all_stations_cli_treats_zero_expected_and_zero_actual_as_failure(monkeypatch):
    """기본 후보 계산이 비어도 '0건 완전 성공'으로 통과하거나 기존 결과를 지우지 않는다."""
    code, parquet_calls, json_calls = _run_all_stations_cli(monkeypatch, _fake_all_stations_outcome(0, 0))

    assert code == 1
    assert parquet_calls == []
    assert json_calls[0][1] == []


def test_single_station_multi_hour_cli_saves_to_s3(monkeypatch):
    """단일 정류소 다중 시간대(n_hours>1) CLI 실행 시 S3 저장 검증."""
    from core import s3 as s3_io

    trips = pd.DataFrame([_trip("ST-100", "2025-06-01 09:00:00", "2025-06-01 09:05:00")])
    _set_rental_events(trips)
    _set_return_history("ST-100", "2025-06-01 09:00:00")
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")
    ps._population_profile = {}
    _set_station_master(["ST-100"])
    monkeypatch.setattr(ps, "predict", _fake_predict)

    saved_calls = []
    monkeypatch.setattr(s3_io, "write_parquet", lambda df, key: saved_calls.append((df, key)))

    argv = [
        "--station-id", "ST-100",
        "--date", "2025-06-01",
        "--hour", "10",
        "--minute", "0",
        "--temp", "20.0",
        "--precip", "0.0",
        "--stockout",
        "--n-hours", "3",
    ]

    try:
        ps.main(argv)
    except SystemExit:
        pass

    assert len(saved_calls) == 1
    df, key = saved_calls[0]
    assert key == "predictions/single/dt=2025-06-01/hh=10/ST-100_1000.parquet"
    assert len(df) == 3
    assert list(df["horizon"]) == [1, 2, 3]
