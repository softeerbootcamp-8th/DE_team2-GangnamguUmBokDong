"""station_hour_merged 테이블에 lag/exposure feature를 추가한다 (PySpark 포팅).

`src/features.py`(pandas)와 동일한 정신을 잇지만, 피처 중요도 분석 이후 lag는
한쪽당 `lag_1h` 1개만 남기고(lag_24h/168h, roll_mean/std 전부 제거) 캘린더
순환 인코딩(hour/dow의 sin·cos)도 뺐다(`libs/ml_core/common_config.py`
`BASE_FEATURE_COLUMNS` 참고) — 대여/반납은 여전히 이 tick 단위 테이블 하나를
같이 쓴다(둘 다 같은 기저 grid/날씨/인구를 보므로 여기서 나눌 이유가 없음).
실제로 완전히 분리되는 지점은 `build_multi_horizon_features.py`의 horizon
self-join 이후다 — 거기서부터 대여/반납의 데이터셋 크기가 다르게 불어난다.

**pandas 버전과 다르게 반드시 시간 기준(range) 윈도우를 써야 한다.** pandas
버전의 그리드는 station마다 dense해서 "N번째 이전 행"과 "N시간 전"이 항상
같았다 — 그래서 `shift`(행 개수 기준)가 그대로 맞았다. 이 패키지의
`build_merged_table.py`는 station 활성 구간 필터링 때문에 **그리드에 구멍
(결측 시간대)이 생길 수 있다** — 그래서 lag는 위치 기반 `F.lag()`가 아니라
`hour_ts + N시간 = 현재 행의 hour_ts`인 행을 찾는 **self-join**으로 구현한다
(정확히 그 시각의 행이 그리드에 없으면(구멍) null이 되는 게 맞는 동작).

대여의 `lag_1h`만 point-in-time censored 값(rolling_rental_features)으로
계산하고, 반납은 raw 값으로 계산한다 — 이유는 REALTIME_FEATURES.md/`src/features.py`
docstring 참고 (여기서 반복하지 않음).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, ShortType

from . import config
from .rolling_window_features import lookup_count_at_ticks

# rental_exposure는 Spark 집계 함수가 아니라 F.when()으로 만들어 캐스트가 없으면
# double로 남는다 — 값 범위가 [0.05, 1.0]이라 float32로 정밀도 손실 없이 절반으로
# 줄인다. rental_lag_1h/return_lag_1h도 float32로 둔다 — ShortType(정수)로 캐스트하면
# 안 된다: 둘 다 LEFT JOIN 결과라 그리드 구멍에서 null일 수 있고,
# ml_core.model_contract의 pandas dtype 계약도 같은 이유로 float32라 학습/서빙
# 쪽에서 이 컬럼에 정수 dtype을 기대하면 안 된다(모듈 docstring 참고).
_COMPUTED_FLOAT_COLUMNS = ["rental_exposure", "rental_lag_1h", "return_lag_1h"]


def _downcast_computed_columns(df: DataFrame) -> DataFrame:
    """이 파일이 계산해서 붙인 컬럼들을 값 범위에 맞는 최소 타입으로 다운캐스트한다."""
    for col in _COMPUTED_FLOAT_COLUMNS:
        df = df.withColumn(col, F.col(col).cast(FloatType()))
    return df


def _exact_hour_lag(df: DataFrame, value_col: str, lag_hours: int, out_col: str) -> DataFrame:
    """정확히 lag_hours시간 전의 value_col 값을 self-join으로 조회한다 (그리드에 그 시각이 없으면 null).

    args:
        df: station_no, hour_ts를 포함한 DataFrame
        value_col: 조회할 값 컬럼명
        lag_hours: 몇 시간 전인지
        out_col: 결과를 담을 새 컬럼명
    returns:
        DataFrame: out_col이 추가된 df
    """
    shifted = df.select(
        F.col("station_no").alias("_lag_station"),
        (F.col("hour_ts") + F.expr(f"INTERVAL {lag_hours} HOURS")).alias("_lag_target_hour_ts"),
        F.col(value_col).alias(out_col),
    )
    joined = df.join(
        shifted,
        (df["station_no"] == shifted["_lag_station"]) & (df["hour_ts"] == shifted["_lag_target_hour_ts"]),
        "left",
    )
    return joined.drop("_lag_station", "_lag_target_hour_ts")


def _add_return_lag_1h(df: DataFrame) -> DataFrame:
    """반납(return_count)의 1시간 전 값을 추가한다 — raw 값 그대로 (censoring 문제 없음)."""
    return _exact_hour_lag(df, "return_count", 1, "return_lag_1h")


def _add_rental_lag_1h(spark: SparkSession, df: DataFrame, rolling_parquet_path: str) -> DataFrame:
    """대여(rental_count)의 point-in-time censored "1시간 전" 값을 추가한다.

    `rolling_rental_features.py`(censored_rolling_counts, config.ROLLING_WINDOW_MINUTES/
    EMBARGO_MINUTES 기준 — 지금은 [T-100분, T-40분) 60분짜리 창)가 만든 sparse
    step function을 그리드의 각 (station_no, hour_ts)에 대해 조회한다.

    `cumulative`는 Silver 트립(station_id 텍스트 원본)에서 곧바로 계산돼 station_id로
    키가 잡혀 있다 — station_master를 작게 join해서 station_no로 바꾼 뒤 조회한다
    (station_id는 df 쪽엔 이미 없으므로, df를 건드리지 않고 이 작은 테이블만 변환하는
    쪽이 싸다 — cumulative는 트립이 실제로 있었던 시점만 담은 sparse 테이블이라
    tick x station 전체 그리드인 df보다 훨씬 작다).

    args:
        spark: SparkSession
        df: station_no, hour_ts를 포함한 DataFrame
        rolling_parquet_path: build_rolling_rental_features.py가 만든 sparse step function 경로
    returns:
        DataFrame: rental_lag_1h 컬럼이 추가된 df
    """
    cumulative = spark.read.parquet(rolling_parquet_path)  # station_id, tick, count
    master = spark.read.parquet(config.STATION_MASTER_PARQUET).select(
        "station_id", F.col("station_no").cast(ShortType()).alias("station_no")
    )
    cumulative = cumulative.join(master, on="station_id", how="inner").drop("station_id")
    query = df.select("station_no", F.col("hour_ts").alias("tick"))
    visible = lookup_count_at_ticks(
        cumulative, query, station_col="station_no", tick_col="tick", query_tick_col="tick"
    )
    visible = visible.withColumnRenamed("tick", "hour_ts").withColumnRenamed("count", "rental_lag_1h")
    return df.join(visible, on=["station_no", "hour_ts"], how="left")


def _add_exposure(df: DataFrame) -> DataFrame:
    """대여 모델 전용 exposure: 품절(stockout) 시간대는 대여 가능성이 낮음을 반영."""
    return df.withColumn(
        "rental_exposure",
        F.when(F.col("stockout_flag") == 1, F.lit(config.EXPOSURE_STOCKOUT_VALUE)).otherwise(F.lit(1.0)),
    )


def build_features(spark: SparkSession, df: DataFrame, rolling_parquet_path: str | None = None) -> DataFrame:
    """병합 테이블에 lag/exposure feature를 전부 추가한다.

    args:
        spark: SparkSession
        df: build_merged_table()이 만든 병합 테이블
        rolling_parquet_path: 대여 point-in-time censored 카운트 parquet 경로
            (기본값 config.ROLLING_RENTAL_FEATURES_PARQUET)
    returns:
        DataFrame: rental_lag_1h, return_lag_1h, rental_exposure가 추가된 df
    """
    rolling_path = rolling_parquet_path or config.ROLLING_RENTAL_FEATURES_PARQUET
    df = _add_return_lag_1h(df)
    df = _add_rental_lag_1h(spark, df, rolling_path)
    df = _add_exposure(df)
    df = _downcast_computed_columns(df)
    return df


if __name__ == "__main__":
    from .spark_session import get_spark

    spark = get_spark()
    merged = spark.read.parquet(config.MERGED_TABLE_PARQUET)
    features_df = build_features(spark, merged)
    # run_pipeline.py의 증분 실행이 date 파티션 단위로 overwrite하므로, 수동 실행도
    # 같은 파티션 레이아웃을 유지해야 한다.
    features_df.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)
    print(f"features -> {config.FEATURES_TABLE_PARQUET}")
