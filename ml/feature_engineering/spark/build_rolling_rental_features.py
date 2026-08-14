"""2025년 대여이력 -> station별 point-in-time 대여 카운트 (PySpark 포팅).

`src/build_rolling_rental_features.py`(pandas, 로컬 검증용)와 동일한 규칙으로
station_master 크로스워크 매칭 후 `rolling_window_features.censored_rolling_counts()`를
호출한다 — 자세한 설계 배경은 REALTIME_FEATURES.md 참고.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .build_targets import _normalize_station_no
from .rolling_window_features import censored_rolling_counts


def load_rental_trip_events(spark: SparkSession, since: str | None = None) -> DataFrame:
    """1차 정제 산출물(원본 대여이력 parquet, config.TRAIN_MONTHS 기준)에서
    station_id 매칭된 트립만 추린다.

    args:
        spark: SparkSession
        since: 지정하면 start_dt >= since인 트립만 남긴다 (증분 재계산용 — 호출부가
            "워터마크 - INCREMENTAL_LOOKBACK_HOURS"를 넘겨줘야 lag/rolling이 안전하게
            맞물린다). None이면(기본값) 전체 히스토리.
    returns:
        DataFrame: station_id, start_dt, end_dt (station_master와 매칭 안 되는
            트립은 제외 — 폐쇄/결번 정류소 등, 약 5~7%)
    """
    master = spark.read.parquet(config.STATION_MASTER_PARQUET).select("station_no", "station_id")

    paths = [
        f"{config.RENTAL_PARQUET_DIR}/서울특별시 공공자전거 대여이력 정보_{ym}.parquet"
        for ym in config.TRAIN_MONTHS
    ]
    trips = spark.read.parquet(*paths).select("start_dt", "start_st", "end_dt")
    if since is not None:
        trips = trips.filter(F.col("start_dt") >= F.lit(since))
    trips = trips.withColumn("station_no", _normalize_station_no(F.col("start_st")))

    matched = trips.join(master, on="station_no", how="inner").select("station_id", "start_dt", "end_dt")
    return matched


def build_rolling_rental_features(
    spark: SparkSession, output_path: str | None = None, since: str | None = None
) -> DataFrame:
    """station별 point-in-time 대여 카운트를 5분 틱 grid에서 계산한다 (호출부가 저장 여부/방식을 결정).

    args:
        spark: SparkSession
        output_path: 지정하면 이 경로에 overwrite로 저장(전체 빌드용). None이면 저장하지
            않고 DataFrame만 반환(증분 빌드는 run_pipeline.py가 append 여부를 직접 결정).
        since: load_rental_trip_events()에 그대로 전달 (증분 재계산용)
    returns:
        DataFrame: station_id, tick, count (sparse step function)
    """
    trips = load_rental_trip_events(spark, since=since)
    result = censored_rolling_counts(
        trips,
        window_minutes=config.ROLLING_WINDOW_MINUTES,
        embargo_minutes=config.ROLLING_EMBARGO_MINUTES,
        tick_minutes=config.ROLLING_TICK_MINUTES,
    )
    if output_path is not None:
        result.write.mode("overwrite").parquet(output_path)
    return result
