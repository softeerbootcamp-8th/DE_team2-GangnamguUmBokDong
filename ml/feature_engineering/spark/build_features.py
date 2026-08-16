"""station_hour_merged 테이블에 시계열/캘린더/exposure feature를 추가한다 (PySpark 포팅).

`src/features.py`(pandas)와 동일한 컬럼 스키마(LAG_ROLLING_FEATURE_COLUMNS 14개 +
캘린더 순환 인코딩 4개 + rental_exposure)를 만든다.

**pandas 버전과 다르게 반드시 시간 기준(range) 윈도우를 써야 한다.** pandas
버전의 그리드는 station마다 8,760시간이 완전히 dense해서 "N번째 이전 행"과
"N시간 전"이 항상 같았다 — 그래서 `shift`/`rolling(window=N)`(행 개수 기준)이
그대로 맞았다. 이 패키지의 `build_merged_table.py`는 station 활성 구간 필터링
때문에 **그리드에 구멍(결측 시간대)이 생길 수 있다** — 그 상태에서 행 개수
기준 윈도우를 쓰면 "24시간 전"이라면서 실제로는 (구멍 때문에) 27시간 전 값을
가져오는 조용한 버그가 생긴다. 그래서 여기서는:

- **lag**: 위치 기반 `F.lag()`가 아니라, `hour_ts + N시간 = 현재 행의 hour_ts`인
  행을 찾는 **self-join**으로 구현한다 — 정확히 그 시각의 행이 그리드에 없으면
  (구멍) null이 되는 게 맞는 동작이다.
- **rolling**: 행 개수 기준 `rowsBetween`이 아니라 실제 초 단위 경과시간 기준
  `rangeBetween`으로 구현한다 — 구멍이 있으면 그 구간만큼 표본이 적은 평균이
  나올 뿐, 엉뚱한 먼 시점의 값이 섞여 들어오지 않는다.

대여의 "직전 1시간" 4개(lag_1h, roll_mean/std_3h/24h)만 point-in-time censored
값(rolling_rental_features)으로 계산하고, 나머지 9개(반납 전체 + 대여
lag_24h/168h)는 raw 값으로 계산한다 — 이유는 REALTIME_FEATURES.md/`src/features.py`
docstring 참고 (여기서 반복하지 않음).
"""

from __future__ import annotations

import math

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import FloatType, ShortType

from . import config
from .rolling_window_features import _unix_seconds_ntz, lookup_count_at_ticks

HOUR_SECONDS = 3600

# build_merged_table.py의 NATIVE_COLUMN_DTYPES는 그 파일이 직접 만드는 컬럼만 다운캐스트한다 —
# 이 파일이 나중에 계산해서 붙이는 컬럼(rolling mean/std, 순환 인코딩, exposure)은 캐스트가
# 없어서 Spark 집계 함수(F.avg/F.stddev)의 기본 반환 타입인 double로 그대로 남아 있었다
# (263M행 규모에서 이 13개 컬럼만으로 행당 58바이트, 전체로는 수 GB~십수 GB 차이 — 실측
# 기반 값 범위: rolling mean/std는 최대 수십 단위, sin/cos는 [-1,1], exposure는 [0.05,1.0]
# 이라 float32로 정밀도 손실 없음). rental_lag_1h만 `rolling_rental_features`의 카운트
# 조회 결과라 다른 lag_24h/168h(둘 다 ShortType 유지)와 다르게 long으로 남는다 — 실측
# max=96이라 int16(ShortType)에 안전하게 들어간다.
_COMPUTED_FLOAT_COLUMNS = [
    "rental_roll_mean_3h", "rental_roll_std_3h", "rental_roll_mean_24h", "rental_roll_std_24h",
    "return_roll_mean_3h", "return_roll_std_3h", "return_roll_mean_24h", "return_roll_std_24h",
    "hour_sin", "hour_cos", "dow_sin", "dow_cos",
    "rental_exposure",
]
_COMPUTED_SHORT_COLUMNS = ["rental_lag_1h"]


def _downcast_computed_columns(df: DataFrame) -> DataFrame:
    """이 파일이 계산해서 붙인 컬럼들을 값 범위에 맞는 최소 타입으로 다운캐스트한다."""
    for col in _COMPUTED_FLOAT_COLUMNS:
        df = df.withColumn(col, F.col(col).cast(FloatType()))
    for col in _COMPUTED_SHORT_COLUMNS:
        df = df.withColumn(col, F.col(col).cast(ShortType()))
    return df


def _add_cyclical(df: DataFrame) -> DataFrame:
    """hour/dow를 sin/cos로 순환 인코딩한 컬럼 4개를 추가한다."""
    two_pi = 2 * math.pi
    df = df.withColumn("hour_sin", F.sin(F.col("hour") * F.lit(two_pi / 24)))
    df = df.withColumn("hour_cos", F.cos(F.col("hour") * F.lit(two_pi / 24)))
    df = df.withColumn("dow_sin", F.sin(F.col("dow") * F.lit(two_pi / 7)))
    df = df.withColumn("dow_cos", F.cos(F.col("dow") * F.lit(two_pi / 7)))
    return df


def _exact_hour_lag(df: DataFrame, value_col: str, lag_hours: int, out_col: str) -> DataFrame:
    """정확히 lag_hours시간 전의 value_col 값을 self-join으로 조회한다 (그리드에 그 시각이 없으면 null).

    args:
        df: station_id, hour_ts를 포함한 DataFrame
        value_col: 조회할 값 컬럼명
        lag_hours: 몇 시간 전인지
        out_col: 결과를 담을 새 컬럼명
    returns:
        DataFrame: out_col이 추가된 df
    """
    shifted = df.select(
        F.col("station_id").alias("_lag_station"),
        (F.col("hour_ts") + F.expr(f"INTERVAL {lag_hours} HOURS")).alias("_lag_target_hour_ts"),
        F.col(value_col).alias(out_col),
    )
    joined = df.join(
        shifted,
        (df["station_id"] == shifted["_lag_station"]) & (df["hour_ts"] == shifted["_lag_target_hour_ts"]),
        "left",
    )
    return joined.drop("_lag_station", "_lag_target_hour_ts")


def _add_return_lag_rolling(df: DataFrame) -> DataFrame:
    """반납(return_count) lag/rolling feature를 추가한다 — raw 값 그대로 (censoring 문제 없음)."""
    for lag in config.LAG_HOURS:
        df = _exact_hour_lag(df, "return_count", lag, f"return_lag_{lag}h")

    df = df.withColumn("_hour_ts_unix", _unix_seconds_ntz(F.col("hour_ts")))
    w_order = Window.partitionBy("station_id").orderBy("_hour_ts_unix")
    for window in config.ROLLING_WINDOWS:
        w_roll = w_order.rangeBetween(-window * HOUR_SECONDS, -1)  # 직전 window시간, 현재 시점 제외
        df = df.withColumn(f"return_roll_mean_{window}h", F.avg("return_count").over(w_roll))
        df = df.withColumn(f"return_roll_std_{window}h", F.stddev("return_count").over(w_roll))
    return df.drop("_hour_ts_unix")


def _rental_visible(spark: SparkSession, df: DataFrame, rolling_parquet_path: str) -> DataFrame:
    """station_id, hour_ts별 point-in-time censored 대여 카운트를 조회해 df에 붙인다.

    args:
        spark: SparkSession
        df: station_id, hour_ts를 포함한 DataFrame
        rolling_parquet_path: build_rolling_rental_features.py가 만든 sparse step function 경로
    returns:
        DataFrame: rental_visible 컬럼이 추가된 df
    """
    cumulative = spark.read.parquet(rolling_parquet_path)
    query = df.select("station_id", F.col("hour_ts").alias("tick"))
    visible = lookup_count_at_ticks(
        cumulative, query, station_col="station_id", tick_col="tick", query_tick_col="tick"
    )
    visible = visible.withColumnRenamed("tick", "hour_ts").withColumnRenamed("count", "rental_visible")
    return df.join(visible, on=["station_id", "hour_ts"], how="left")


def _add_rental_lag_rolling(spark: SparkSession, df: DataFrame, rolling_parquet_path: str) -> DataFrame:
    """대여(rental_count) lag/rolling feature를 추가한다 — "직전 1시간" 4개만 censored 값으로 대체."""
    df = _rental_visible(spark, df, rolling_parquet_path)

    for lag in config.LAG_HOURS:
        if lag == 1:
            df = df.withColumn("rental_lag_1h", F.col("rental_visible"))
            continue
        df = _exact_hour_lag(df, "rental_count", lag, f"rental_lag_{lag}h")

    df = df.withColumn("_hour_ts_unix", _unix_seconds_ntz(F.col("hour_ts")))
    w_order = Window.partitionBy("station_id").orderBy("_hour_ts_unix")
    tick_seconds = config.GRID_TICK_MINUTES * 60
    for window in config.ROLLING_WINDOWS:
        # rental_visible[T]는 이미 [T-30분,...) 이전 정보만 쓰므로 shift 불필요 — 현재 행(0) 포함.
        # 그리드가 5분 tick이므로 "window시간 폭"을 맞추려면 window*HOUR_SECONDS에서 tick 하나(5분)를
        # 빼야 한다 — (window-1)*HOUR_SECONDS만 쓰면(시간 단위 그리드 시절 공식) tick 하나만큼
        # 창이 좁아진다(예: window=3이면 실제로는 2시간짜리 평균이 되는 버그, predict_single.py의
        # (target_ts-window, target_ts] 정의와 어긋남 — inference/tests/dev_rental_censoring_cross_parity.py 참고).
        w_roll = w_order.rangeBetween(-(window * HOUR_SECONDS - tick_seconds), 0)
        df = df.withColumn(f"rental_roll_mean_{window}h", F.avg("rental_visible").over(w_roll))
        df = df.withColumn(f"rental_roll_std_{window}h", F.stddev("rental_visible").over(w_roll))

    return df.drop("_hour_ts_unix", "rental_visible")


def _add_exposure(df: DataFrame) -> DataFrame:
    """대여 모델 전용 exposure: 품절(stockout) 시간대는 대여 가능성이 낮음을 반영."""
    return df.withColumn(
        "rental_exposure",
        F.when(F.col("stockout_flag") == 1, F.lit(config.EXPOSURE_STOCKOUT_VALUE)).otherwise(F.lit(1.0)),
    )


def build_features(spark: SparkSession, df: DataFrame, rolling_parquet_path: str | None = None) -> DataFrame:
    """병합 테이블에 캘린더/lag/rolling/exposure feature를 전부 추가한다.

    args:
        spark: SparkSession
        df: build_merged_table()이 만든 병합 테이블
        rolling_parquet_path: 대여 point-in-time censored 카운트 parquet 경로
            (기본값 config.ROLLING_RENTAL_FEATURES_PARQUET)
    returns:
        DataFrame: LAG_ROLLING_FEATURE_COLUMNS + hour/dow_sin/cos + rental_exposure가 추가된 df
    """
    rolling_path = rolling_parquet_path or config.ROLLING_RENTAL_FEATURES_PARQUET
    df = _add_cyclical(df)
    df = _add_return_lag_rolling(df)
    df = _add_rental_lag_rolling(spark, df, rolling_path)
    df = _add_exposure(df)
    df = _downcast_computed_columns(df)
    return df


LAG_ROLLING_FEATURE_COLUMNS = [
    f"{prefix}_{suffix}"
    for prefix in ("rental", "return")
    for suffix in (
        [f"lag_{lag}h" for lag in config.LAG_HOURS]
        + [f"roll_mean_{w}h" for w in config.ROLLING_WINDOWS]
        + [f"roll_std_{w}h" for w in config.ROLLING_WINDOWS]
    )
]


if __name__ == "__main__":
    from .spark_session import get_spark

    spark = get_spark()
    merged = spark.read.parquet(config.MERGED_TABLE_PARQUET)
    features_df = build_features(spark, merged)
    features_df.write.mode("overwrite").parquet(config.FEATURES_TABLE_PARQUET)
    print(f"features -> {config.FEATURES_TABLE_PARQUET}")
