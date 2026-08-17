"""Silver `bike_rental_history` -> station별 point-in-time 대여 카운트 (PySpark).

트립 로딩·station_id 매칭은 `silver_source.read_rental_trips()`가 담당한다 — 자세한
설계 배경은 REALTIME_FEATURES.md 참고.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession

from . import config
from .rolling_window_features import censored_rolling_counts
from .silver_source import read_rental_trips


def load_rental_trip_events(spark: SparkSession, since: str | None = None) -> DataFrame:
    """Silver 대여이력에서 station_id 매칭된 트립만 추린다(대여 쪽 station_id 기준).

    args:
        spark: SparkSession
        since: 지정하면 start_dt >= since인 트립만 남긴다 (증분 재계산용 — 호출부가
            "워터마크 - INCREMENTAL_LOOKBACK_HOURS"를 넘겨줘야 lag/rolling이 안전하게
            맞물린다). None이면(기본값) 전체 히스토리.
    returns:
        DataFrame: station_id, start_dt, end_dt (station_master와 매칭 안 되는
            트립은 제외)
    """
    return read_rental_trips(spark, since=since).select("station_id", "start_dt", "end_dt")


def build_rolling_rental_features(
    spark: SparkSession, output_path: str | None = None, since: str | None = None
) -> DataFrame:
    """station별 point-in-time 대여 카운트를 5분 틱 grid에서 계산한다 (호출부가 저장 여부/방식을 결정).

    args:
        spark: SparkSession
        output_path: 지정하면 이 경로에 overwrite로 저장(전체 빌드용). None이면 저장하지
            않고 DataFrame만 반환(증분 빌드는 run_pipeline.py가 저장 방식을 직접 결정).
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
