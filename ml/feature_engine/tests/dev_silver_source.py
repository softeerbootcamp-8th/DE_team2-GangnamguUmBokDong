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
    monkeypatch.setattr(fe_config, "TRAIN_YEAR", 2025)
    return tmp_path / "silver"


def test_read_station_master_renames_columns(spark, _silver_root):
    _write_parquet(
        _silver_root / "station" / "station_master.parquet",
        pd.DataFrame([{
            "sta_id": "ST-1", "sta_no": "00001", "sta_nm": "역삼", "hold_cnt": 12,
            "lat": 37.5, "lon": 127.0, "grid_id": "다사1",
        }]),
    )
    df = read_station_master(spark).toPandas()
    assert list(df.columns) == ["station_id", "station_no", "station_name", "capacity", "lat", "lon", "grid_id"]
    assert df.iloc[0]["station_id"] == "ST-1"


def test_read_rental_trips_matches_by_station_id_directly_and_dedupes(spark, _silver_root):
    _write_parquet(
        _silver_root / "station" / "station_master.parquet",
        pd.DataFrame([
            {"sta_id": "ST-1", "sta_no": "00001", "sta_nm": "a", "hold_cnt": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"},
            {"sta_id": "ST-2", "sta_no": "00002", "sta_nm": "b", "hold_cnt": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"},
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
        _silver_root / "station" / "station_master.parquet",
        pd.DataFrame([{"sta_id": "ST-1", "sta_no": "00001", "sta_nm": "a", "hold_cnt": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"}]),
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
