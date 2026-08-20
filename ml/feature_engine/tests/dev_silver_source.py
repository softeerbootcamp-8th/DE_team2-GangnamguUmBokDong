"""silver_source.py의 Archive fact/Silver current dimension 계약을 검증한다.

- station_master 컬럼명 변환
- fact 네 소스가 flat daily Archive 파일만 정확한 날짜 목록으로 읽는지
- 날짜 누락·필수 물리 컬럼 누락을 fail-closed하는지
- 파일 안 범위 밖 행을 실제 timestamp로 다시 제거하는지
- 재고 `_window_start`, 날씨 `baseDate+baseTime`, 인구 `YMD+TT` 계약
- 생활인구 actual/최신 우선 중복 규칙과 메타 없는 과거 실측 호환
"""

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_merged_table import (
    _forward_fill_weather_to_ticks,
    _weather_context_start,
)
from feature_engine.spark.build_targets import build_targets
from feature_engine.spark.rolling_window_features import lookup_count_at_ticks
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
def _data_roots(tmp_path, monkeypatch):
    monkeypatch.setattr(fe_config, "ARCHIVE_ROOT", str(tmp_path / "archive"))
    monkeypatch.setattr(fe_config, "SILVER_ROOT", str(tmp_path / "silver"))
    return {"archive": tmp_path / "archive", "silver": tmp_path / "silver"}


def test_read_station_master_selects_latest_enriched_snapshot(spark, _data_roots):
    silver_root = _data_roots["silver"]
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-05-31" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-OLD", "station_no": 99, "station_name": "이전", "capacity": 8,
            "lat": 37.5, "lon": 127.0, "grid_id": "다사1",
        }]),
    )
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "역삼", "capacity": 12,
            "lat": 37.5, "lon": 127.0, "grid_id": "다사1",
        }]),
    )
    df = read_station_master(spark).toPandas()
    assert list(df.columns) == ["station_id", "station_no", "station_name", "capacity", "lat", "lon", "grid_id"]
    assert len(df) == 1
    assert df.iloc[0]["station_id"] == "ST-1"


def test_read_rental_trips_matches_by_station_id_directly_and_dedupes(spark, _data_roots):
    silver_root = _data_roots["silver"]
    archive_root = _data_roots["archive"]
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
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
        archive_root / "bike_rental_history" / "dt=2025-06-01.parquet",
        pd.DataFrame([common_row, unmatched_row]),  # 같은 내용 재수록(누적 스냅샷 흉내)
    )

    # 같은 daily archive 안에서 같은 트립을 두 번 넣어 중복 제거를 검증한다.
    duplicated = pd.DataFrame([common_row, unmatched_row, common_row, unmatched_row])
    _write_parquet(archive_root / "bike_rental_history" / "dt=2025-06-01.parquet", duplicated)
    trips = read_rental_trips(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()
    assert len(trips) == 1  # BIKE-1 트립 하나만(중복 제거 + ST-9999 트립 배제)
    assert trips.iloc[0]["station_id"] == "ST-1"
    assert trips.iloc[0]["end_station_id"] == "ST-2"


def test_read_rental_trips_keeps_unmatched_return_as_null(spark, _data_roots):
    silver_root = _data_roots["silver"]
    archive_root = _data_roots["archive"]
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-06-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{"station_id": "ST-1", "station_no": 1, "station_name": "a", "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1"}]),
    )
    _write_parquet(
        archive_root / "bike_rental_history" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "BIKE_ID": "BIKE-1", "RENT_DT": "2025-06-01 08:00:00", "RTN_DT": "2025-06-01 08:10:00",
            "RENT_STATION_ID": "ST-1", "RETURN_STATION_ID": "ST-9999", "USE_MIN": "10", "USE_DST": "100.0",
        }]),
    )
    trips = read_rental_trips(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()
    assert len(trips) == 1
    assert trips.iloc[0]["station_id"] == "ST-1"
    assert pd.isna(trips.iloc[0]["end_station_id"])


def test_read_population_prefers_actual_then_latest_snapshot(spark, _data_roots):
    """동일 격자·시각에서는 actual이 estimated보다 우선하고 최신 actual이 이긴다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        pd.DataFrame([
            {
                "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515", "CELL_ID": "G1",
                "SPOP": 100.0, "is_estimated": False, "estimation_method": "actual",
                "_window_start": "2025-06-01T09:00:00+09:00",
            },
            {
                "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515", "CELL_ID": "G1",
                "SPOP": 999.0, "is_estimated": True, "estimation_method": "weighted_avg",
                "_window_start": "2025-06-01T23:00:00+09:00",
            },
        ]),
    )
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-02.parquet",
        pd.DataFrame([{
            "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515", "CELL_ID": "G1",
            "SPOP": 200.0, "is_estimated": False, "estimation_method": "actual",
            "_window_start": "2025-06-02T09:00:00+09:00",
        }]),
    )
    df = read_population(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-03 00:00:00",
    ).toPandas()
    assert len(df) == 1
    assert df.iloc[0]["pop_total"] == pytest.approx(200.0)
    assert df.iloc[0]["pop_resd"] == pytest.approx(200.0)
    assert df.iloc[0]["pop_long_foreign"] == 0.0
    assert df.iloc[0]["pop_short_foreign"] == 0.0


def test_read_population_sums_distinct_h_dng_components_after_revision_dedupe(
    spark,
    _data_roots,
):
    """행정동별 최신 revision만 고른 뒤 같은 격자의 서로 다른 component를 합산한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        pd.DataFrame([
            {
                "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515     ",
                "CELL_ID": "G1", "SPOP": 10.0, "is_estimated": True,
                "estimation_method": "weighted_avg", "_window_start": "2025-06-01T08:05:00+09:00",
            },
            {
                "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515",
                "CELL_ID": "G1", "SPOP": 20.0, "is_estimated": False,
                "estimation_method": "actual", "_window_start": "2025-06-01T09:05:00+09:00",
            },
            {
                "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110530",
                "CELL_ID": "G1", "SPOP": 7.0, "is_estimated": False,
                "estimation_method": "actual", "_window_start": "2025-06-01T09:05:00+09:00",
            },
        ]),
    )

    population = read_population(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()

    assert len(population) == 1
    assert population.iloc[0]["pop_total"] == pytest.approx(27.0)


def test_read_population_preserves_all_null_component_sum(spark, _data_roots):
    """격자·시각의 모든 행정동 SPOP가 마스킹되면 합계를 0이 아닌 null로 둔다."""
    archive_root = _data_roots["archive"]
    rows = pd.DataFrame([
        {"YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515", "CELL_ID": "G1", "SPOP": None},
        {"YMD": "20250601", "TT": "08", "H_DNG_CD": "11110530", "CELL_ID": "G1", "SPOP": None},
    ])
    rows["SPOP"] = rows["SPOP"].astype("float64")
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        rows,
    )

    population = read_population(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()

    assert len(population) == 1
    assert pd.isna(population.iloc[0]["pop_total"])
    assert pd.isna(population.iloc[0]["pop_resd"])


@pytest.mark.parametrize(
    ("ymd", "tt", "expected"),
    [
        (20250216, 0, pd.Timestamp("2025-02-16 00:00:00")),
        ("20250216", "0 ", pd.Timestamp("2025-02-16 00:00:00")),
        ("20250216", "9", pd.Timestamp("2025-02-16 09:00:00")),
    ],
)
def test_read_population_normalizes_integer_and_numeric_string_hours(
    spark,
    _data_roots,
    ymd,
    tt,
    expected,
):
    """실제 Archive의 `TT=0 ` 및 정수형 시각을 2자리로 정규화한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-02-16.parquet",
        pd.DataFrame([{
            "YMD": ymd,
            "TT": tt,
            "H_DNG_CD": "11110515",
            "CELL_ID": "G1",
            "SPOP": 123.0,
        }]),
    )

    population = read_population(
        spark,
        since="2025-02-16 00:00:00",
        until="2025-02-17 00:00:00",
    ).toPandas()

    assert len(population) == 1
    assert population.iloc[0]["hour_ts"] == expected


@pytest.mark.parametrize(
    ("ymd", "tt"),
    [
        ("20250216", "24"),
        ("20250216", "3.5"),
        ("20250230", "0"),
        ("2025-02-16", "0"),
    ],
)
def test_read_population_fails_closed_for_invalid_ymd_or_hour(spark, _data_roots, ymd, tt):
    """시각 계약 위반 행을 조용히 누락하지 않고 Spark job을 실패시킨다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-02-16.parquet",
        pd.DataFrame([{
            "YMD": ymd,
            "TT": tt,
            "H_DNG_CD": "11110515",
            "CELL_ID": "G1",
            "SPOP": 123.0,
        }]),
    )

    population = read_population(
        spark,
        since="2025-02-16 00:00:00",
        until="2025-02-17 00:00:00",
    )
    with pytest.raises(Exception, match="Archive living_population_grid YMD/TT"):
        population.collect()


@pytest.mark.parametrize("h_dng_cd", ["", "   ", "not-a-code"])
def test_read_population_fails_closed_for_invalid_h_dng_cd(
    spark,
    _data_roots,
    h_dng_cd,
):
    """required 행정동 코드가 비었거나 숫자 코드가 아니면 조용히 합치지 않는다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "YMD": "20250601", "TT": "08", "H_DNG_CD": h_dng_cd,
            "CELL_ID": "G1", "SPOP": 123.0,
        }]),
    )

    population = read_population(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    )
    with pytest.raises(Exception, match="Archive living_population_grid H_DNG_CD"):
        population.collect()


def test_archive_readers_use_status_and_weather_availability_time(spark, _data_roots):
    """재고·live 날씨는 `_window_start` availability 시각을 우선한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "bike_station_realtime" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "stationId": "ST-1", "stationName": "a", "rackTotCnt": 10, "parkingBikeTotCnt": 3,
            "shared": 30, "stationLatitude": 37.5, "stationLongitude": 127.0,
            "_window_start": "2025-06-01T08:05:00+09:00", "_source_kind": "collector",
        }]),
    )
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "baseDate": "20250601", "baseTime": "0800", "nx": 60, "ny": 127,
            "T1H": 25.5, "REH": 61.0, "WSD": 1.5, "RN1": 0.0, "PTY": 0,
            # 관측 기준은 08:00이지만 모델이 처음 알 수 있는 시각은 08:55다.
            # source_kind가 없는 legacy도 _window_start가 있으면 availability를 우선한다.
            "_window_start": "2025-06-01T08:55:00+09:00",
        }]),
    )

    status = read_station_status(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()
    assert status.iloc[0]["hour_ts"] == pd.Timestamp("2025-06-01 08:00:00")
    assert status.iloc[0]["bike_count"] == 3
    assert status.iloc[0]["stockout_flag"] == 0

    weather = read_weather(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()
    assert weather.iloc[0]["hour_ts"] == pd.Timestamp("2025-06-01 08:55:00")
    assert weather.iloc[0]["temp"] == pytest.approx(25.5)
    assert weather.iloc[0]["humidity"] == 61


def test_live_weather_revisions_keep_each_collection_availability_tick(spark, _data_roots):
    """같은 08:00 관측의 08:05/08:55 수정본을 각각 그 수집시각부터만 보이게 한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-06-01.parquet",
        pd.DataFrame([
            {
                "baseDate": "20250601", "baseTime": "0800", "nx": 60, "ny": 127,
                "T1H": 10.0, "REH": 50.0, "WSD": 1.0, "RN1": 0.0,
                "_window_start": "2025-06-01T08:05:00+09:00", "_source_kind": "collector",
            },
            {
                "baseDate": "20250601", "baseTime": "0800", "nx": 60, "ny": 127,
                "T1H": 20.0, "REH": 60.0, "WSD": 2.0, "RN1": 1.0,
                "_window_start": "2025-06-01T08:55:00+09:00", "_source_kind": "collector",
            },
        ]),
    )

    weather = read_weather(
        spark,
        since="2025-06-01 08:00:00",
        until="2025-06-01 09:00:00",
    )
    expanded = (
        _forward_fill_weather_to_ticks(weather.select("hour_ts", "temp", "precip"), 5, 3)
        .toPandas()
        .set_index("hour_ts")
    )

    assert pd.Timestamp("2025-06-01 08:00:00") not in expanded.index
    assert expanded.loc[pd.Timestamp("2025-06-01 08:05:00"), "temp"] == pytest.approx(10.0)
    assert expanded.loc[pd.Timestamp("2025-06-01 08:50:00"), "temp"] == pytest.approx(10.0)
    assert expanded.loc[pd.Timestamp("2025-06-01 08:55:00"), "temp"] == pytest.approx(20.0)


def test_bootstrap_weather_uses_physical_time_instead_of_date_only_window(spark, _data_roots):
    """ASOS bootstrap의 자정 `_window_start`가 00/01/23시 관측을 붕괴시키지 않는다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-06-01.parquet",
        pd.DataFrame([
            {
                "baseDate": "20250601", "baseTime": hour, "nx": 60, "ny": 127,
                "T1H": temp, "REH": 60.0, "WSD": 2.0, "RN1": 0.0,
                "_window_start": "2025-06-01T00:00:00+09:00", "_source_kind": "bootstrap",
            }
            for hour, temp in (("0000", 10.0), ("0100", 11.0), ("2300", 23.0))
        ]),
    )

    weather = read_weather(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas().sort_values("hour_ts").reset_index(drop=True)

    assert weather["hour_ts"].tolist() == [
        pd.Timestamp("2025-06-01 00:00:00"),
        pd.Timestamp("2025-06-01 01:00:00"),
        pd.Timestamp("2025-06-01 23:00:00"),
    ]


def test_read_weather_preserves_each_observation_and_averages_valid_grids(spark, _data_roots):
    """메타 없는 ASOS는 물리 관측시각을 쓰며 유효한 서울 격자 평균을 만든다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-06-01.parquet",
        pd.DataFrame([
            {"baseDate": "20250601", "baseTime": "0800", "nx": 60, "ny": 127,
             "T1H": 10.0, "REH": 10.0, "WSD": 1.0, "RN1": 10.0, "PTY": 0},
            {"baseDate": "20250601", "baseTime": "0855", "nx": 60, "ny": 127,
             "T1H": 20.0, "REH": 60.0, "WSD": 2.0, "RN1": 0.0, "PTY": 0},
            {"baseDate": "20250601", "baseTime": "0855", "nx": 61, "ny": 127,
             "T1H": 24.0, "REH": 70.0, "WSD": 4.0, "RN1": 2.0, "PTY": 0},
            {"baseDate": "20250601", "baseTime": "0855", "nx": 62, "ny": 127,
             "T1H": 999.0, "REH": 99.0, "WSD": 99.0, "RN1": 3.0, "PTY": 0},
        ]),
    )

    weather = read_weather(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas().sort_values("hour_ts").reset_index(drop=True)

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


def test_weather_forward_fill_resamples_five_minute_revisions_to_twenty_minute_grid(spark):
    """20분 grid tick은 그 시각까지 도착한 가장 최신 5분 관측만 사용해야 한다."""
    weather = spark.createDataFrame(
        [
            (pd.Timestamp(f"2025-06-01 08:{minute:02d}:00").to_pydatetime(), float(minute), 0.0)
            for minute in (5, 10, 15, 25, 30)
        ],
        ["hour_ts", "temp", "precip"],
    )

    expanded = (
        _forward_fill_weather_to_ticks(
            weather,
            tick_minutes=20,
            max_staleness_hours=3,
        )
        .filter("hour_ts < '2025-06-01 09:00:00'")
        .toPandas()
        .set_index("hour_ts")
    )

    assert expanded.index.tolist() == [
        pd.Timestamp("2025-06-01 08:20:00"),
        pd.Timestamp("2025-06-01 08:40:00"),
    ]
    assert expanded.loc[pd.Timestamp("2025-06-01 08:20:00"), "temp"] == pytest.approx(15.0)
    assert expanded.loc[pd.Timestamp("2025-06-01 08:40:00"), "temp"] == pytest.approx(30.0)


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


def test_missing_daily_archive_partition_fails_closed(spark, _data_roots):
    """요청 구간의 하루라도 없으면 존재하는 날짜만 조용히 읽지 않는다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "baseDate": "20250601", "baseTime": "0000", "nx": 60, "ny": 127,
            "T1H": 20.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0,
        }]),
    )

    with pytest.raises(FileNotFoundError, match="2025-06-02"):
        read_weather(
            spark,
            since="2025-06-01 12:00:00",
            until="2025-06-03 00:00:00",
        )


def test_target_reader_keeps_cross_boundary_return_and_late_archive_arrival(
    spark,
    _data_roots,
    monkeypatch,
):
    """앞 경계 전 출발 반납과 뒤 partition에 늦게 나타난 window 대여를 모두 센다."""
    silver_root = _data_roots["silver"]
    archive_root = _data_roots["archive"]
    monkeypatch.setattr(fe_config, "INCREMENTAL_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(fe_config, "TRAINING_SAFETY_MARGIN_DAYS", 2)
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-06-05" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "current",
            "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1",
        }]),
    )
    rows_by_partition = {
        "2025-06-01": [{
            "BIKE_ID": "PRE-START", "RENT_DT": "2025-06-01 23:50:00",
            "RTN_DT": "2025-06-02 00:10:00", "RENT_STATION_ID": "ST-1",
            "RETURN_STATION_ID": "ST-1",
        }],
        "2025-06-02": [{
            "BIKE_ID": "MID", "RENT_DT": "2025-06-02 12:00:00",
            "RTN_DT": "2025-06-02 12:05:00", "RENT_STATION_ID": "ST-1",
            "RETURN_STATION_ID": "ST-1",
        }],
        # compaction 수집일은 target event 날짜보다 늦을 수 있다.
        "2025-06-03": [{
            "BIKE_ID": "LATE-ARCHIVE", "RENT_DT": "2025-06-02 23:50:00",
            "RTN_DT": "2025-06-02 23:55:00", "RENT_STATION_ID": "ST-1",
            "RETURN_STATION_ID": "ST-1",
        }],
        # safety margin의 마지막 exact 날짜가 존재함을 검증하는 범위 밖 padding.
        "2025-06-04": [{
            "BIKE_ID": "OUT", "RENT_DT": "2025-06-05 00:00:00",
            "RTN_DT": "2025-06-05 00:05:00", "RENT_STATION_ID": "ST-1",
            "RETURN_STATION_ID": "ST-1",
        }],
    }
    for day, rows in rows_by_partition.items():
        _write_parquet(
            archive_root / "bike_rental_history" / f"dt={day}.parquet",
            pd.DataFrame(rows),
        )

    rental_targets, return_targets = build_targets(
        spark,
        since="2025-06-02 00:00:00",
        until="2025-06-03 00:00:00",
    )
    rental_query = spark.createDataFrame(
        [("ST-1", pd.Timestamp("2025-06-02 23:00:00").to_pydatetime())],
        ["station_id", "tick"],
    )
    return_query = spark.createDataFrame(
        [("ST-1", pd.Timestamp("2025-06-02 00:00:00").to_pydatetime())],
        ["station_id", "tick"],
    )
    rental_at_23 = lookup_count_at_ticks(rental_targets, rental_query).toPandas()
    return_at_00 = lookup_count_at_ticks(return_targets, return_query).toPandas()

    assert rental_at_23.iloc[0]["count"] == 1
    assert return_at_00.iloc[0]["count"] == 1


def test_target_reader_missing_lookback_partition_fails_closed(
    spark,
    _data_roots,
    monkeypatch,
):
    """target 날짜가 있어도 앞쪽 return context archive가 없으면 실패한다."""
    silver_root = _data_roots["silver"]
    archive_root = _data_roots["archive"]
    monkeypatch.setattr(fe_config, "INCREMENTAL_LOOKBACK_HOURS", 24)
    monkeypatch.setattr(fe_config, "TRAINING_SAFETY_MARGIN_DAYS", 0)
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2025-06-02" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "current",
            "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1",
        }]),
    )
    _write_parquet(
        archive_root / "bike_rental_history" / "dt=2025-06-02.parquet",
        pd.DataFrame([{
            "BIKE_ID": "MID", "RENT_DT": "2025-06-02 12:00:00",
            "RTN_DT": "2025-06-02 12:05:00", "RENT_STATION_ID": "ST-1",
            "RETURN_STATION_ID": "ST-1",
        }]),
    )

    with pytest.raises(FileNotFoundError, match="2025-06-01"):
        build_targets(
            spark,
            since="2025-06-02 00:00:00",
            until="2025-06-03 00:00:00",
        )


def test_incompatible_daily_archive_schema_fails_closed(spark, _data_roots):
    """재고 시각 물리 컬럼과 메타가 모두 없으면 schema 단계에서 실패한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "bike_station_realtime" / "dt=2025-06-01.parquet",
        pd.DataFrame([{"stationId": "ST-1", "parkingBikeTotCnt": 3}]),
    )

    with pytest.raises(ValueError, match=r"_window_start\|stationDt"):
        read_station_status(
            spark,
            since="2025-06-01 00:00:00",
            until="2025-06-02 00:00:00",
        )


def test_population_archive_missing_required_h_dng_cd_fails_closed(spark, _data_roots):
    """생활인구 Archive에 required 행정동 코드 컬럼이 없으면 schema 단계에서 실패한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "YMD": "20250601", "TT": "08", "CELL_ID": "G1", "SPOP": 123.0,
        }]),
    )

    with pytest.raises(ValueError, match="H_DNG_CD"):
        read_population(
            spark,
            since="2025-06-01 00:00:00",
            until="2025-06-02 00:00:00",
        )


def test_metadata_free_population_and_status_physical_time_are_supported(spark, _data_roots):
    """메타 없는 실측 인구와 물리 stationDt가 있는 과거 재고는 그대로 처리한다."""
    archive_root = _data_roots["archive"]
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "YMD": "20250601", "TT": "08", "H_DNG_CD": "11110515", "CELL_ID": "G1", "SPOP": 123.0,
        }]),
    )
    _write_parquet(
        archive_root / "bike_station_realtime" / "dt=2025-06-01.parquet",
        pd.DataFrame([{
            "stationId": "ST-1", "parkingBikeTotCnt": 4, "stationDt": "2025060108",
        }]),
    )

    population = read_population(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()
    status = read_station_status(
        spark,
        since="2025-06-01 00:00:00",
        until="2025-06-02 00:00:00",
    ).toPandas()

    assert population.iloc[0]["pop_total"] == pytest.approx(123.0)
    assert status.iloc[0]["hour_ts"] == pd.Timestamp("2025-06-01 08:00:00")


def test_refresh_keeps_current_silver_master_and_refilters_archive_upper_bound(
    spark,
    _data_roots,
    monkeypatch,
):
    """최신 Silver master는 쓰되 Archive fact의 `[since, until)` 경계를 모두 지킨다."""
    silver_root = _data_roots["silver"]
    archive_root = _data_roots["archive"]
    monkeypatch.setattr(fe_config, "INCREMENTAL_LOOKBACK_HOURS", 1)
    monkeypatch.setattr(fe_config, "TRAINING_SAFETY_MARGIN_DAYS", 0)
    _write_parquet(
        silver_root / "station_master_enriched" / "dt=2026-08-01" / "hh=00" / "0000.parquet",
        pd.DataFrame([{
            "station_id": "ST-1", "station_no": 1, "station_name": "current",
            "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "G1",
        }]),
    )
    _write_parquet(
        archive_root / "bike_rental_history" / "dt=2025-12-31.parquet",
        pd.DataFrame([
            {
                "BIKE_ID": "IN", "RENT_DT": "2025-12-31 23:50:00", "RTN_DT": "2025-12-31 23:55:00",
                "RENT_STATION_ID": "ST-1", "RETURN_STATION_ID": "ST-1",
            },
            {
                "BIKE_ID": "OUT", "RENT_DT": "2026-01-01 00:00:00", "RTN_DT": "2026-01-01 00:05:00",
                "RENT_STATION_ID": "ST-1", "RETURN_STATION_ID": "ST-1",
            },
        ]),
    )
    _write_parquet(
        archive_root / "bike_station_realtime" / "dt=2025-12-31.parquet",
        pd.DataFrame([
            {"stationId": "ST-1", "parkingBikeTotCnt": 3, "_window_start": "2025-12-31T23:55:00+09:00"},
            {"stationId": "ST-1", "parkingBikeTotCnt": 4, "_window_start": "2026-01-01T00:00:00+09:00"},
        ]),
    )
    _write_parquet(
        archive_root / "weather_ultra_short_live" / "dt=2025-12-31.parquet",
        pd.DataFrame([
            {"baseDate": "20251231", "baseTime": "2000", "nx": 60, "ny": 127,
             "T1H": 7.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0},
            {"baseDate": "20251231", "baseTime": "2355", "nx": 60, "ny": 127,
             "T1H": 20.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0},
            {"baseDate": "20260101", "baseTime": "0000", "nx": 60, "ny": 127,
             "T1H": 30.0, "REH": 50.0, "WSD": 2.0, "RN1": 0.0},
        ]),
    )
    _write_parquet(
        archive_root / "living_population_grid" / "dt=2025-12-31.parquet",
        pd.DataFrame([
            {"YMD": "20251231", "TT": "23", "H_DNG_CD": "11110515", "CELL_ID": "G1", "SPOP": 100.0},
            {"YMD": "20260101", "TT": "00", "H_DNG_CD": "11110515", "CELL_ID": "G1", "SPOP": 200.0},
        ]),
    )

    output_paths = {
        "STATION_MASTER_PARQUET": archive_root.parent / "master.parquet",
        "TARGETS_PARQUET": archive_root.parent / "targets.parquet",
        "RETURN_TARGETS_PARQUET": archive_root.parent / "return_targets.parquet",
        "STATION_STATUS_PARQUET": archive_root.parent / "status.parquet",
        "WEATHER_PARQUET": archive_root.parent / "weather.parquet",
        "POPULATION_PARQUET": archive_root.parent / "population.parquet",
    }
    for name, path in output_paths.items():
        monkeypatch.setattr(fe_config, name, str(path))

    since = "2025-12-31 23:00:00"
    until = "2026-01-01 00:00:00"
    _refresh_primary_tables(spark, since=since, until=until)

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

    # 첫 target tick의 3시간 weather context가 같은 이전 날짜 flat archive에서 유지된다.
    weather = spark.read.parquet(str(output_paths["WEATHER_PARQUET"])).toPandas()
    assert weather["hour_ts"].min() == pd.Timestamp("2025-12-31 20:00:00")

    targets = spark.read.parquet(str(output_paths["TARGETS_PARQUET"])).toPandas()
    assert targets["tick"].max() <= pd.Timestamp("2025-12-31 23:00:00")
    assert targets.sort_values("tick").iloc[-1]["count"] == 1
