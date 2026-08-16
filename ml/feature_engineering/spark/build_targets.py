"""Silver `bike_rental_history` -> "T로부터 앞으로 1시간 동안 일어날 대여/반납 건수" 타겟 생성 (PySpark).

트립 원본 로딩·station_id 매칭은 `silver_source.read_rental_trips()`가 담당한다
(예전엔 원본 historical CSV parquet을 station_no로 크로스워크했지만, 실제 Silver
`bike_rental_history`는 `RENT_STATION_ID`/`RETURN_STATION_ID`가 이미 station_id
형식이라 그 크로스워크가 필요 없다 — `docs/collector/ml-integration-requests.md` 1번).
이 파일은 그 결과에 `rolling_window_features.future_rolling_counts()`로 sparse
step function 타겟만 계산한다.
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .rolling_window_features import future_rolling_counts
from .silver_source import read_rental_trips


def build_targets(spark: SparkSession, since: str | None = None) -> tuple[DataFrame, DataFrame]:
    """Silver 대여이력을 읽어 "[T,T+1시간)에 일어날 대여/반납 건수" 타겟을 계산한다.

    args:
        spark: SparkSession
        since: 지정하면 start_dt >= since인 트립만 남긴다(증분 재계산용)
    returns:
        tuple[DataFrame, DataFrame]: (rental_targets, return_targets) — 각각
            station_id, tick, count (sparse step function)
    """
    trips = read_rental_trips(spark, since=since)

    rental_trips = trips.select("station_id", "start_dt")
    return_trips = (
        trips.filter(F.col("end_dt").isNotNull() & F.col("end_station_id").isNotNull())
        .select(F.col("end_station_id").alias("station_id"), "end_dt")
    )

    rental_targets = future_rolling_counts(
        rental_trips,
        width_minutes=config.TARGET_HORIZON_MINUTES,
        tick_minutes=config.GRID_TICK_MINUTES,
        event_col="start_dt",
    )
    return_targets = future_rolling_counts(
        return_trips,
        width_minutes=config.TARGET_HORIZON_MINUTES,
        tick_minutes=config.GRID_TICK_MINUTES,
        event_col="end_dt",
    )
    return rental_targets, return_targets


if __name__ == "__main__":
    from .spark_session import get_spark

    spark = get_spark()
    rental_targets, return_targets = build_targets(spark)
    rental_targets.write.mode("overwrite").parquet(config.TARGETS_PARQUET)
    return_targets.write.mode("overwrite").parquet(config.RETURN_TARGETS_PARQUET)
    print(f"rental targets -> {config.TARGETS_PARQUET}")
    print(f"return targets -> {config.RETURN_TARGETS_PARQUET}")
