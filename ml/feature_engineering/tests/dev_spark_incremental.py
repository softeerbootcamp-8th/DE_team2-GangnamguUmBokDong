"""증분(watermark 기반 append) 계산이 전체 재빌드와 정확히 같은 값을 내는지 검증한다.

`run_pipeline._run_incremental()`은 "워터마크 - INCREMENTAL_LOOKBACK_HOURS"부터만
다시 계산해서 워터마크 이후 새 행만 append한다 — lookback 마진이 충분하면(lag_168h
등 7일 참조보다 넉넉하면) 이 방식이 "처음부터 전체를 다시 계산"한 것과 동일한
값을 내야 한다. 이 테스트가 그 핵심 주장을 실측으로 확인한다: 없으면 증분 로직이
경계 근처에서 조용히 다른 값(또는 결측)을 내는 회귀를 잡을 방법이 없다.
"""

import json
import os
import sys

import pandas as pd
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import functions as F

from feature_engineering.spark import config as fe_config
from feature_engineering.spark.build_features import build_features
from feature_engineering.spark.build_merged_table import build_merged_table
from feature_engineering.spark.build_rolling_rental_features import (
    build_rolling_rental_features,
)
from feature_engineering.spark.build_targets import build_targets
from feature_engineering.spark.run_pipeline import _run_incremental
from feature_engineering.spark.watermark import write_watermark

N_HOURS = 600  # 25일
WATERMARK_OFFSET_HOURS = 432  # 18일차 -> 신규 구간 168시간(7일)
LOOKBACK_HOURS = 240  # 10일 (lag_168h의 7일보다 넉넉한 마진)
TICKS_PER_HOUR = 60 // fe_config.GRID_TICK_MINUTES  # 그리드가 이제 시간이 아니라 5분 tick 단위


@pytest.fixture(scope="module")
def spark():
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # timestamp_ntz/timestamp(tz-aware) 왕복 어긋남 방지 — feature_engineering/spark_session.py 참고.
    os.environ.setdefault("TZ", "Asia/Seoul")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-feature-engineering-incremental")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    yield session
    session.stop()


@pytest.fixture
def synthetic_environment(spark, tmp_path, monkeypatch):
    """station 1개, 25일(600시간)치 "1차 정제" 산출물을 임시 경로에 만들고 config를 그쪽으로 돌린다.

    타겟(rental/return)은 이제 sparse step function이라(build_merged_table.py가
    lookup_count_at_ticks()로 조회), 손으로 dense 테이블을 만드는 대신 실제
    build_targets.build_targets()를 트립 데이터로 돌려서 만든다 — 이 fixture 자체가
    build_targets.py의 Spark 포팅도 같이 검증하는 셈이다.
    """
    start = pd.Timestamp("2025-01-01 00:00")
    hours = pd.date_range(start, periods=N_HOURS, freq="h")

    master_pdf = pd.DataFrame(
        [{"station_id": "A", "station_no": "00001", "station_name": "test", "capacity": 10, "lat": 37.5, "lon": 127.0, "grid_id": "다사00000000"}]
    )
    master_path = str(tmp_path / "station_master.parquet")
    master_pdf.to_parquet(master_path, index=False)

    status_pdf = pd.DataFrame({"station_id": "A", "hour_ts": hours, "bike_count": 5, "stockout_flag": 0})
    status_path = str(tmp_path / "status.parquet")
    status_pdf.to_parquet(status_path, index=False)

    weather_pdf = pd.DataFrame({"hour_ts": hours, "temp": 20.0, "precip": 0.0, "wind": 2.0, "humidity": 50.0})
    weather_path = str(tmp_path / "weather.parquet")
    weather_pdf.to_parquet(weather_path, index=False)

    pop_pdf = pd.DataFrame(
        {"grid_id": "다사00000000", "hour_ts": hours, "pop_resd": 100.0, "pop_long_foreign": 1.0, "pop_short_foreign": 1.0, "pop_total": 102.0}
    )
    pop_path = str(tmp_path / "population.parquet")
    pop_pdf.to_parquet(pop_path, index=False)

    summary_path = tmp_path / "analysis_summary.json"
    summary_path.write_text(json.dumps({"holidays_2025": []}), encoding="utf-8")

    # 트립: 시간마다 1건, 대여 소요시간을 5~40분 사이에서 순환시켜 censoring 신호를 다양하게 만든다.
    # start/end 둘 다 station "00001"에서 일어난다고 두고 rental/return 타겟을 동시에 만든다.
    trip_rows = []
    for i, h in enumerate(hours):
        duration_min = 5 + (i % 8) * 5
        trip_start = h + pd.Timedelta(minutes=(i % 3) * 10)
        trip_rows.append(
            {
                "start_dt": trip_start,
                "start_st": "00001",
                "end_dt": trip_start + pd.Timedelta(minutes=duration_min),
                "end_st": "00001",
            }
        )
    trips_pdf = pd.DataFrame(trip_rows)
    rental_dir = tmp_path / "rental_parquet"
    rental_dir.mkdir()
    trips_pdf.to_parquet(rental_dir / "서울특별시 공공자전거 대여이력 정보_9901.parquet", index=False)

    output_root = str(tmp_path / "output")
    targets_path = str(tmp_path / "targets.parquet")
    return_targets_path = str(tmp_path / "return_targets.parquet")

    monkeypatch.setattr(fe_config, "STATION_MASTER_PARQUET", master_path)
    monkeypatch.setattr(fe_config, "TARGETS_PARQUET", targets_path)
    monkeypatch.setattr(fe_config, "RETURN_TARGETS_PARQUET", return_targets_path)
    monkeypatch.setattr(fe_config, "STATION_STATUS_PARQUET", status_path)
    monkeypatch.setattr(fe_config, "WEATHER_PARQUET", weather_path)
    monkeypatch.setattr(fe_config, "POPULATION_PARQUET", pop_path)
    monkeypatch.setattr(fe_config, "ANALYSIS_SUMMARY_JSON", str(summary_path))
    monkeypatch.setattr(fe_config, "RENTAL_PARQUET_DIR", str(rental_dir))
    monkeypatch.setattr(fe_config, "TRAIN_MONTHS", ["9901"])
    monkeypatch.setattr(fe_config, "OUTPUT_ROOT", output_root)
    monkeypatch.setattr(fe_config, "ROLLING_RENTAL_FEATURES_PARQUET", output_root + "/rolling.parquet")
    monkeypatch.setattr(fe_config, "MERGED_TABLE_PARQUET", output_root + "/merged.parquet")
    monkeypatch.setattr(fe_config, "FEATURES_TABLE_PARQUET", output_root + "/features.parquet")
    monkeypatch.setattr(fe_config, "WATERMARK_PATH", output_root + "/_watermark.json")
    monkeypatch.setattr(fe_config, "INCREMENTAL_LOOKBACK_HOURS", LOOKBACK_HOURS)

    # 타겟은 여기서 한 번 계산해서 저장 — since 필터와 무관하게 항상 전체 히스토리로
    # 만들어야 하므로(모듈 docstring 참고), _run_incremental이 내부에서 다시 만들지
    # 않고 이 고정 parquet을 그대로 읽는다.
    rental_targets_df, return_targets_df = build_targets(spark)
    rental_targets_df.write.mode("overwrite").parquet(targets_path)
    return_targets_df.write.mode("overwrite").parquet(return_targets_path)

    watermark_cutoff = start + pd.Timedelta(hours=WATERMARK_OFFSET_HOURS)
    return {"watermark_cutoff": watermark_cutoff}


COMPARE_COLS = [
    "hour_ts",
    "rental_lag_1h", "rental_lag_24h", "rental_lag_168h",
    "rental_roll_mean_3h", "rental_roll_std_3h", "rental_roll_mean_24h", "rental_roll_std_24h",
    "return_lag_1h", "return_lag_24h", "return_lag_168h",
    "return_roll_mean_3h", "return_roll_std_3h", "return_roll_mean_24h", "return_roll_std_24h",
]


def test_incremental_append_matches_full_rebuild(spark, synthetic_environment, tmp_path):
    watermark_cutoff = synthetic_environment["watermark_cutoff"]

    # (A) 기준값: 전체 600시간으로 한 번에 계산한 뒤, 워터마크 이후 구간만 잘라낸다.
    full_rolling_path = str(tmp_path / "full_rolling.parquet")
    build_rolling_rental_features(spark, output_path=full_rolling_path)
    full_merged = build_merged_table(spark)
    full_features = build_features(spark, full_merged, rolling_parquet_path=full_rolling_path)

    expected_new = (
        full_features.filter(F.col("hour_ts") > F.lit(watermark_cutoff))
        .select(*COMPARE_COLS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )
    # watermark_cutoff 자체는 "이후"가 아니므로 제외. 그리드가 이제 시간이 아니라
    # 5분 tick 단위라 시간당 TICKS_PER_HOUR개 행이 있다.
    assert len(expected_new) == (N_HOURS - WATERMARK_OFFSET_HOURS) * TICKS_PER_HOUR - 1

    # 기존 피처마트(챔피언 산출물) 역할 — 워터마크까지의 부분만 미리 저장해둔다.
    existing = full_features.filter(F.col("hour_ts") <= F.lit(watermark_cutoff))
    existing.write.mode("overwrite").parquet(fe_config.FEATURES_TABLE_PARQUET)
    write_watermark(fe_config.WATERMARK_PATH, watermark_cutoff.isoformat(), {})

    # (B) 증분 실행 — lookback 구간부터만 다시 계산해서 워터마크 이후만 append.
    _run_incremental(spark, {"max_hour_ts": watermark_cutoff.isoformat()})

    appended = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET)
    got_new = (
        appended.filter(F.col("hour_ts") > F.lit(watermark_cutoff))
        .select(*COMPARE_COLS)
        .toPandas()
        .sort_values("hour_ts")
        .reset_index(drop=True)
    )

    pd.testing.assert_frame_equal(got_new, expected_new, check_dtype=False, check_exact=False, rtol=1e-9)


def test_incremental_is_noop_when_no_new_data(spark, synthetic_environment, tmp_path):
    """워터마크가 데이터의 최신 시각과 같으면(새 데이터 없음) append 없이 조용히 끝나야 한다."""
    full_rolling_path = str(tmp_path / "full_rolling2.parquet")
    build_rolling_rental_features(spark, output_path=full_rolling_path)
    full_merged = build_merged_table(spark)
    full_features = build_features(spark, full_merged, rolling_parquet_path=full_rolling_path)
    full_features.write.mode("overwrite").parquet(fe_config.FEATURES_TABLE_PARQUET)

    # 그리드가 5분 tick 단위라 마지막 시각은 "N_HOURS-1시간째" 정각이 아니라 그 시간의
    # 마지막 tick이다 — 손으로 계산하지 않고 실제 데이터의 max(hour_ts)를 그대로 쓴다.
    last_tick = full_features.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(fe_config.WATERMARK_PATH, last_tick.isoformat(), {})

    before_count = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET).count()
    _run_incremental(spark, {"max_hour_ts": last_tick.isoformat()})
    after_count = spark.read.parquet(fe_config.FEATURES_TABLE_PARQUET).count()

    assert before_count == after_count == N_HOURS * TICKS_PER_HOUR
