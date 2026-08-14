"""2025년 대여이력 parquet -> "T로부터 앞으로 1시간 동안 일어날 대여/반납 건수" 타겟 생성 (PySpark 포팅).

`src/build_targets.py`(pandas, 로컬 검증용)와 동일한 규칙 — station_master
크로스워크로 매칭한 뒤 `rolling_window_features.future_rolling_counts()`로 sparse
step function 타겟을 만든다. "1차 정제는 이 저장소 밖에서 처리하고 여기는 테스트
편의상만 같이 한다"는 원칙(history.md 5번)에 따라, `build_rolling_rental_features.py`가
이미 pandas/Spark 양쪽에 있는 것처럼 이 파일도 대칭을 맞춰 로컬 Spark 테스트를
가능하게 한다.

trip parquet의 start_st/end_st는 5자리 대여소번호(문자열, 0-padding 불일치)이고
'\\N'은 반납 전 미완료(분실 등) 트립을 의미한다. station_master의 station_no와
매칭되지 않는 트립(폐쇄/결번 정류소 등)은 해당 방향 집계에서 제외한다.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .rolling_window_features import future_rolling_counts


def _normalize_station_no(col: Column) -> Column:
    """대여소번호 컬럼을 5자리 zero-padding 문자열로 정규화한다 ('\\N'은 결측 처리).

    `src/build_targets.py`의 `_normalize_station_no()`와 동일한 규칙.

    args:
        col: start_st 또는 end_st 컬럼 (문자열, 0-padding 불일치 + '\\N' 포함 가능)
    returns:
        Column: 5자리로 zfill된 문자열, '\\N'이었던 자리는 null
    """
    numeric = F.when(col == "\\N", F.lit(None)).otherwise(col.cast("long"))
    return F.when(numeric.isNull(), F.lit(None)).otherwise(F.lpad(numeric.cast("string"), 5, "0"))


def build_targets(spark: SparkSession) -> tuple[DataFrame, DataFrame]:
    """월별 대여이력 parquet를 읽어 "[T,T+1시간)에 일어날 대여/반납 건수" 타겟을 계산한다.

    args:
        spark: SparkSession
    returns:
        tuple[DataFrame, DataFrame]: (rental_targets, return_targets) — 각각
            station_id, tick, count (sparse step function)
    """
    master = spark.read.parquet(config.STATION_MASTER_PARQUET).select("station_no", "station_id")

    paths = [
        f"{config.RENTAL_PARQUET_DIR}/서울특별시 공공자전거 대여이력 정보_{ym}.parquet" for ym in config.TRAIN_MONTHS
    ]
    trips = spark.read.parquet(*paths).select("start_dt", "start_st", "end_dt", "end_st")

    rental_trips = (
        trips.withColumn("station_no", _normalize_station_no(F.col("start_st")))
        .join(master, on="station_no", how="inner")
        .select("station_id", "start_dt")
    )
    return_trips = (
        trips.withColumn("station_no", _normalize_station_no(F.col("end_st")))
        .join(master, on="station_no", how="inner")
        .filter(F.col("end_dt").isNotNull())
        .select("station_id", "end_dt")
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
