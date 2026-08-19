"""silver_source.py가 실제 collector Silver 예시 데이터의 특이사항들을 올바르게
처리하는지 개별적으로 검증한다 (dev_spark_incremental.py의 파이프라인 전체 검증과
달리, 여기는 각 read_*() 함수의 경계 케이스만 좁게 본다).

- station_master 컬럼명 변환
- bike_rental_history: station_id가 이미 station_id 형식이라 크로스워크 없이
  매칭되는지, station_master에 없는 station은 걸러지는지, (bike_id, start_dt)
  중복 제거가 실제로 동작하는지(누적 스냅샷 시나리오 대비, ml-integration-requests.md
  10번)
- living_population_grid: 같은 (grid_id, hour_ts)가 서로 다른 수집일(dt=)
  파티션에 걸쳐 나오면 더 최근에 수집된 값이 이기는지(공표 지연, 8번)
- bike_station_realtime/weather_ultra_short_live: 파일 경로(dt=/hh=/HHMM)에서
  시각을 정확히 역추출하는지
"""

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_merged_table import (
    _forward_fill_weather_to_ticks,
    _weather_context_start,
)
from feature_engine.spark.run_pipeline import _refresh_primary_tables
from feature_engine.spark.silver_source import (
    read_population,
    read_rental_trips,
    read_station_master,
    read_station_status,
    read_weather,
)


def _write_parquet(path, df: pd.DataFrame) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)


@pytest.fixture(autouse=True)
def _silver_root(tmp_path, monkeypatch):
    monkeypatch.setattr(fe_config, "SILVER_ROOT", str(tmp_path / "silver"))
    return tmp_path / "silver"


def test_read_station_master_selects_latest_enriched_snapshot(spark, _silver_root):
    _write_parquet(
        _silver_root / "station_master_enriched" / "dt=2025-05-31" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-OLD", "station_no": 99, "station_name": "이전", "capacity": 8,
            "lat": 37.5, "lon": 127.0, "grid_id": "다사1",
        }]),
    )
    _write_parquet(
        _silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "역삼", "capacity": 12,
            "lat": 37.5, "lon": 127.0, "grid_id": "다사1",
        }]),
    )
    df = read_station_master(spark).toPandas()
    assert list(df.columns) == ["station_id", "station_no", "station_name", "capacity", "lat", "lon", "grid_id"]
    assert len(df) == 1
    assert df.iloc[0]["station_id"] == "ST-1"


def test_read_rental_trips_matches_by_station_id_directly_and_dedupes(spark, _silver_root):
    _write_parquet(
        _silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([
            {"station_id": "ST-1", "station_no": 1, "station_name": "a", "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"},
            {"station_id": "ST-2", "station_no": 2, "station_name": "b", "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"},
        ]),
    )
    # 같은 트립(BIKE-1, 같은 start_dt)이 두 tick 파일에 걸쳐 중복 등장 —
    # 누적(cumulative) 스냅샷 시나리오를 흉내낸다. station_master에 없는
    # "ST-9999"로 향하는 트립은 대여 쪽에서 걸러져야 한다.
    common_row = {
        "BIKE_ID": "BIKE-1", "RENT_DT": "2025-06-01 08:00:00", "RTN_DT": "2025-06-01 08:10:00",
        "RENT_STATION_ID": "ST-1", "RETURN_STATION_ID": "ST-2", "USE_MIN": "10", "USE_DST": "100.0",
    }
    unmatched_row = {
        "BIKE_ID": "BIKE-2", "RENT_DT": "2025-06-01 08:05:00", "RTN_DT": "2025-06-01 08:15:00",
        "RENT_STATION_ID": "ST-9999", "RETURN_STATION_ID": "ST-2", "USE_MIN": "10", "USE_DST": "100.0",
    }
    _write_parquet(
        _silver_root / "bike_rental_history" / "dt=2025-06-01" / "hh=08" / "0800.parquet",
        pd.DataFrame([common_row, unmatched_row]),
    )
    _write_parquet(
        _silver_root / "bike_rental_history" / "dt=2025-06-01" / "hh=08" / "0805.parquet",
        pd.DataFrame([common_row, unmatched_row]),  # 같은 내용 재수록(누적 스냅샷 흉내)
    )

    trips = read_rental_trips(spark).toPandas()
    assert len(trips) == 1  # BIKE-1 트립 하나만(중복 제거 + ST-9999 트립 배제)
    assert trips.iloc[0]["station_id"] == "ST-1"
    assert trips.iloc[0]["end_station_id"] == "ST-2"


def test_read_rental_trips_keeps_unmatched_return_as_null(spark, _silver_root):
    _write_parquet(
        _silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{"station_id": "ST-1", "station_no": 1, "station_name": "a", "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"}]),
    )
    _write_parquet(
        _silver_root / "bike_rental_history" / "dt=2025-06-01" / "hh=08" / "0800.parquet",
        pd.DataFrame([{
            "BIKE_ID": "BIKE-1", "RENT_DT": "2025-06-01 08:00:00", "RTN_DT": "2025-06-01 08:10:00",
            "RENT_STATION_ID": "ST-1", "RETURN_STATION_ID": "ST-9999", "USE_MIN": "10", "USE_DST": "100.0",
        }]),
    )
    trips = read_rental_trips(spark).toPandas()
    assert len(trips) == 1
    assert trips.iloc[0]["station_id"] == "ST-1"
    assert pd.isna(trips.iloc[0]["end_station_id"])


def test_read_population_prefers_most_recently_collected_snapshot(spark, _silver_root):
    # 같은 (grid_id=G1, hour_ts=2025-06-01 08:00)이 이틀에 걸쳐 수집됨 — 더 최근
    # 수집일(dt=2025-06-02)의 값이 이겨야 한다.
    _write_parquet(
        _silver_root / "living_population_grid" / "dt=2025-06-01" / "hh=09" / "0900.parquet",
        pd.DataFrame([{"YMD": "20250601", "TT": "08", "H_DNG_CD": "", "CELL_ID": "G1", "SPOP": 100.0}]),
    )
    _write_parquet(
        _silver_root / "living_population_grid" / "dt=2025-06-02" / "hh=09" / "0900.parquet",
        pd.DataFrame([{"YMD": "20250601", "TT": "08", "H_DNG_CD": "", "CELL_ID": "G1", "SPOP": 200.0}]),
    )
    df = read_population(spark).toPandas()
    assert len(df) == 1
    assert df.iloc[0]["pop_total"] == pytest.approx(200.0)
    assert df.iloc[0]["pop_resd"] == pytest.approx(200.0)
    assert df.iloc[0]["pop_long_foreign"] == 0.0
    assert df.iloc[0]["pop_short_foreign"] == 0.0


def test_read_station_status_and_weather_extract_tick_from_path(spark, _silver_root):
    _write_parquet(
        _silver_root / "bike_station_realtime" / "dt=2025-06-01" / "hh=08" / "0800.parquet",
        pd.DataFrame([{
            "stationId": "ST-1", "stationName": "a", "rackTotCnt": 10, "parkingBikeTotCnt": 3,
            "shared": 30, "stationLatitude": 37.5, "stationLongitude": 127.0,
        }]),
    )
    _write_parquet(
        _silver_root / "weather_ultra_short_live" / "dt=2025-06-01" / "hh=08" / "0800.parquet",
        pd.DataFrame([{"T1H": 25.5, "REH": 61.0, "WSD": 1.5, "RN1": 0.0, "PTY": 0}]),
    )

    status = read_station_status(spark).toPandas()
    assert status.iloc[0]["hour_ts"] == pd.Timestamp("2025-06-01 08:00:00")
    assert status.iloc[0]["bike_count"] == 3
    assert status.iloc[0]["stockout_flag"] == 0

    weather = read_weather(spark).toPandas()
    assert weather.iloc[0]["hour_ts"] == pd.Timestamp("2025-06-01 08:00:00")
    assert weather.iloc[0]["temp"] == pytest.approx(25.5)
    assert weather.iloc[0]["humidity"] == 61


def test_read_weather_preserves_each_tick_and_averages_all_valid_grids(spark, _silver_root):
    """실제 수집 tick을 보존하며 각 tick의 유효한 서울 격자 전체 평균을 만든다."""
    _write_parquet(
        _silver_root / "weather_ultra_short_live" / "dt=2025-06-01" / "hh=08" / "0800.parquet",
        pd.DataFrame([{"T1H": 10.0, "REH": 10.0, "WSD": 1.0, "RN1": 10.0, "PTY": 0}]),
    )
    _write_parquet(
        _silver_root / "weather_ultra_short_live" / "dt=2025-06-01" / "hh=08" / "0855.parquet",
        pd.DataFrame([
            {"T1H": 20.0, "REH": 60.0, "WSD": 2.0, "RN1": 0.0, "PTY": 0},
            {"T1H": 24.0, "REH": 70.0, "WSD": 4.0, "RN1": 2.0, "PTY": 0},
            {"T1H": 999.0, "REH": 99.0, "WSD": 99.0, "RN1": 3.0, "PTY": 0},
        ]),
    )

    weather = read_weather(spark).toPandas().sort_values("hour_ts").reset_index(drop=True)

    assert weather["hour_ts"].tolist() == [
        pd.Timestamp("2025-06-01 08:00:00"),
        pd.Timestamp("2025-06-01 08:55:00"),
    ]
    assert weather.iloc[0]["temp"] == pytest.approx(10.0)
    assert weather.iloc[1]["temp"] == pytest.approx(22.0)
    assert weather.iloc[1]["precip"] == pytest.approx(1.0)
    assert weather.iloc[1]["wind"] == pytest.approx(3.0)
    assert weather.iloc[1]["humidity"] == 65


def test_weather_forward_fill_never_uses_a_future_collection_tick(spark):
    """08:55 관측을 08:00~08:50에 역전파하지 않고 도착 시각부터만 사용한다."""
    weather = spark.createDataFrame(
        [
            (pd.Timestamp("2025-06-01 08:00:00").to_pydatetime(), 10.0, 0.0),
            (pd.Timestamp("2025-06-01 08:55:00").to_pydatetime(), 22.0, 1.0),
        ],
        ["hour_ts", "temp", "precip"],
    )

    expanded = (
        _forward_fill_weather_to_ticks(
            weather,
            tick_minutes=5,
            max_staleness_hours=3,
        )
        .toPandas()
        .set_index("hour_ts")
    )

    assert expanded.loc[pd.Timestamp("2025-06-01 08:00:00"), "temp"] == pytest.approx(10.0)
    assert expanded.loc[pd.Timestamp("2025-06-01 08:50:00"), "temp"] == pytest.approx(10.0)
    assert expanded.loc[pd.Timestamp("2025-06-01 08:55:00"), "temp"] == pytest.approx(22.0)
    before_new_tick = expanded.loc[expanded.index < pd.Timestamp("2025-06-01 08:55:00")]
    assert (before_new_tick["temp"] == 10.0).all()


def test_weather_context_includes_exact_three_hour_stale_boundary_only(spark):
    """window 첫 tick은 정확히 3시간 전 관측을 쓰되 3시간 5분 뒤에는 쓰지 않는다."""
    since = "2025-01-01 00:00:00"
    assert _weather_context_start(since) == "2024-12-31 21:00:00"
    weather = spark.createDataFrame(
        [(pd.Timestamp("2024-12-31 21:00:00").to_pydatetime(), 7.0, 0.0)],
        ["hour_ts", "temp", "precip"],
    )

    target_window = (
        _forward_fill_weather_to_ticks(
            weather,
            tick_minutes=5,
            max_staleness_hours=3,
        )
        .filter("hour_ts >= '2025-01-01 00:00:00'")
        .toPandas()
    )

    assert target_window["hour_ts"].tolist() == [pd.Timestamp("2025-01-01 00:00:00")]
    assert target_window.iloc[0]["temp"] == pytest.approx(7.0)


def test_exact_2025_refresh_uses_current_master_but_excludes_2026_time_series(
    spark,
    _silver_root,
    monkeypatch,
):
    """2026 current dimension은 쓰되 모든 시계열 산출물은 2025 upper bound를 지킨다."""
    _write_parquet(
        _silver_root / "station_master_enriched" / "dt=2026-08-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "current",
            "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1",
        }]),
    )
    _write_parquet(
        _silver_root / "weather_ultra_short_live" / "dt=2024-12-31" / "hh=21" / "2100.parquet",
        pd.DataFrame([{"T1H": 7.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0, "PTY": 0}]),
    )
    _write_parquet(
        _silver_root / "bike_rental_history" / "dt=2026-01-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([
            {
                "BIKE_ID": "IN-2025", "RENT_DT": "2025-12-31 23:50:00",
                "RTN_DT": "2025-12-31 23:55:00", "RENT_STATION_ID": "ST-1",
                "RETURN_STATION_ID": "ST-1", "USE_MIN": "5", "USE_DST": "100.0",
            },
            {
                "BIKE_ID": "OUT-2026", "RENT_DT": "2026-01-01 00:00:00",
                "RTN_DT": "2026-01-01 00:05:00", "RENT_STATION_ID": "ST-1",
                "RETURN_STATION_ID": "ST-1", "USE_MIN": "5", "USE_DST": "100.0",
            },
        ]),
    )
    for dt, hh, hhmm in (("2025-12-31", "23", "2355"), ("2026-01-01", "00", "0000")):
        _write_parquet(
            _silver_root / "bike_station_realtime" / f"dt={dt}" / f"hh={hh}" / f"{hhmm}.parquet",
            pd.DataFrame([{
                "stationId": "ST-1", "stationName": "current", "rackTotCnt": 10,
                "parkingBikeTotCnt": 3, "shared": 30, "stationLatitude": 37.5,
                "stationLongitude": 127.0,
            }]),
        )
        _write_parquet(
            _silver_root / "weather_ultra_short_live" / f"dt={dt}" / f"hh={hh}" / f"{hhmm}.parquet",
            pd.DataFrame([{"T1H": 20.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0, "PTY": 0}]),
        )
    _write_parquet(
        _silver_root / "living_population_grid" / "dt=2026-01-02" / "hh=09" / "0900.parquet",
        pd.DataFrame([
            {"YMD": "20251231", "TT": "23", "H_DNG_CD": "", "CELL_ID": "G1", "SPOP": 100.0},
            {"YMD": "20260101", "TT": "00", "H_DNG_CD": "", "CELL_ID": "G1", "SPOP": 200.0},
        ]),
    )

    output_paths = {
        "STATION_MASTER_PARQUET": _silver_root.parent / "master.parquet",
        "TARGETS_PARQUET": _silver_root.parent / "targets.parquet",
        "RETURN_TARGETS_PARQUET": _silver_root.parent / "return_targets.parquet",
        "STATION_STATUS_PARQUET": _silver_root.parent / "status.parquet",
        "WEATHER_PARQUET": _silver_root.parent / "weather.parquet",
        "POPULATION_PARQUET": _silver_root.parent / "population.parquet",
    }
    for name, path in output_paths.items():
        monkeypatch.setattr(fe_config, name, str(path))

    until = "2026-01-01 00:00:00"
    _refresh_primary_tables(
        spark,
        since="2025-01-01 00:00:00",
        until=until,
    )

    master = spark.read.parquet(str(output_paths["STATION_MASTER_PARQUET"])).toPandas()
    assert master.iloc[0]["station_name"] == "current"
    for name, timestamp_col in (
        ("TARGETS_PARQUET", "tick"),
        ("RETURN_TARGETS_PARQUET", "tick"),
        ("STATION_STATUS_PARQUET", "hour_ts"),
        ("WEATHER_PARQUET", "hour_ts"),
        ("POPULATION_PARQUET", "hour_ts"),
    ):
        output = spark.read.parquet(str(output_paths[name])).toPandas()
        assert not output.empty, name
        assert output[timestamp_col].max() < pd.Timestamp(until), name

    # 최종 2025 window 첫 tick에서 serving과 같은 <=3시간 as-of fallback을 할 수
    # 있도록 weather 중간 산출물만 정확히 3시간 앞선 source context를 보존한다.
    weather = spark.read.parquet(str(output_paths["WEATHER_PARQUET"])).toPandas()
    assert weather["hour_ts"].min() == pd.Timestamp("2024-12-31 21:00:00")

    # 23:00의 `[T,T+60분)`은 23:50 event를 정상 포함한다. 그보다 늦은 기준시각은
    # 2026 outcome 없이 완결할 수 없으므로 sparse target에도 남기지 않는다.
    targets = spark.read.parquet(str(output_paths["TARGETS_PARQUET"])).toPandas()
    complete_through = pd.Timestamp("2025-12-31 23:00:00")
    assert targets["tick"].max() <= complete_through
    assert targets.sort_values("tick").iloc[-1]["count"] == 1
