"""feature_engineering이 만든 5분 tick feature 테이블(FEATURES_TABLE_PARQUET, horizon=1
전용)을 horizon=1..HORIZON_COUNT 학습 테이블로 확장한다 (PySpark).

**핵심 아이디어(history.md 18번 항목에서 실험·검증됨) — "horizon을 feature로"**: 별도 모델을
horizon마다 두거나 예측값을 재귀적으로 다음 입력에 먹이는 대신(오차 누적), lag/rolling(직전
실적)은 항상 "지금(anchor_ts=T0)" 기준으로 고정하고, "몇 시간 뒤를 묻는지"만 horizon
feature로 모델에 알려준다. 그래서 이 테이블의 한 행은 **원본 테이블의 서로 다른 두 시점을
조합**한 것뿐이다:

- anchor_ts(T0) 쪽에서: `LAG_ROLLING_FEATURE_COLUMNS`(직전 실적, "지금 아는 것") 14개.
- target_ts(T0+(horizon-1)시간) 쪽에서: 날씨/인구/캘린더/`rental_exposure`/타겟 카운트
  (`rental_count`/`return_count`, "그 미래 시점에 실제로 어땠는지") + `date`.

horizon=1이면 anchor_ts==target_ts라 원본 테이블의 해당 행과 완전히 같은 값이 나온다 —
`tests/dev_spark_multi_horizon_parity.py`가 이 불변조건을 회귀 테스트로 고정한다.

**`date`를 target_ts 쪽에서 가져오는 이유**: `training/train_common._split()`이 `date`로
train/valid/test 경계를 가른다. 라벨(타겟 이벤트)이 실제로 언제 일어났는지를 기준으로 잘라야
walk-forward 검증이 안전하다 — anchor_ts 기준으로 자르면 horizon이 큰 행의 라벨이 다음
split으로 새는 누출이 생긴다.

**station 활성 구간 밖 처리**: target_ts에 해당하는 행이 그리드에 없으면(station 비활성
구간이거나, 아직 관측되지 않은 미래라 증분 파이프라인이 그 시점까지 못 만들었을 때) inner
join으로 그 (anchor, horizon) 조합 자체가 자연히 빠진다 — build_merged_table.py의 "그리드
구멍" 철학과 동일. 워터마크 기반 증분 생성을 나중에 이 파이프라인에 붙일 때도 이 성질
덕분에 별도 방어 로직 없이 "target이 아직 없으면 그냥 빠지고, 다음 증분 때 채워짐"이 자동으로
성립한다.

**규모**: T0을 원본과 동일하게 5분 tick 전체로 유지하므로 이 테이블은 원본의 최대
HORIZON_COUNT배 행 수가 된다 — 로컬 `local[*]`로 전체 연도를 처리하는 건 비현실적이고
EMR 대상이다(로컬 검증은 짧은 기간의 합성 데이터로만).
"""

from __future__ import annotations

from ml_common.model_contract import LAG_ROLLING_FEATURE_COLUMNS
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config

ANCHOR_COLUMNS = ["station_id", "hour_ts", *LAG_ROLLING_FEATURE_COLUMNS]
TARGET_COLUMNS = [
    "station_id",
    "hour_ts",
    "capacity",
    "lat",
    "lon",
    "temp",
    "precip",
    "wind",
    "humidity",
    "pop_resd",
    "pop_long_foreign",
    "pop_short_foreign",
    "pop_total",
    "hour",
    "dow",
    "month",
    "is_holiday",
    "is_weekend",
    "is_next_day_off",
    "is_prev_day_off",
    "hour_sin",
    "hour_cos",
    "dow_sin",
    "dow_cos",
    "rental_exposure",
    "rental_count",
    "return_count",
    "date",
]


def _shift_for_horizon(anchor: DataFrame, target: DataFrame, horizon: int) -> DataFrame:
    """anchor(각 행이 T0)에 target_ts=T0+(horizon-1)시간의 컬럼들을 붙이고 horizon을 단다.

    `build_features._exact_hour_lag()`와 정확히 같은 self-join 기법(정확히 그 시각의 행이
    없으면 자동으로 빠짐)을 반대 방향(과거가 아니라 미래 조회)으로 쓴다.

    args:
        anchor: ANCHOR_COLUMNS만 담은 DataFrame (station_id, hour_ts=T0, lag/rolling 14개)
        target: TARGET_COLUMNS만 담은 DataFrame (station_id, hour_ts, 날씨/캘린더/타겟/date)
        horizon: 1~HORIZON_COUNT
    returns:
        DataFrame: station_id, anchor_ts(T0), horizon, lag/rolling 14개,
            TARGET_COLUMNS 중 station_id/hour_ts를 뺀 나머지(target_ts 기준)
    """
    offset_hours = horizon - 1
    shifted_target = (
        target.withColumnRenamed("station_id", "_tgt_station")
        .withColumn("_anchor_hour_ts", F.col("hour_ts") - F.expr(f"INTERVAL {offset_hours} HOURS"))
        .drop("hour_ts")
    )
    joined = anchor.join(
        shifted_target,
        (anchor["station_id"] == shifted_target["_tgt_station"])
        & (anchor["hour_ts"] == shifted_target["_anchor_hour_ts"]),
        "inner",
    )
    joined = joined.withColumnRenamed("hour_ts", "anchor_ts").withColumn("horizon", F.lit(horizon).cast("tinyint"))
    return joined.drop("_tgt_station", "_anchor_hour_ts")


def build_multi_horizon_features(
    spark: SparkSession, features_df: DataFrame, anchor_df: DataFrame | None = None
) -> DataFrame:
    """FEATURES_TABLE_PARQUET(horizon=1 전용, 5분 tick)를 horizon=1..HORIZON_COUNT 학습
    테이블로 확장한다.

    args:
        spark: SparkSession (현재는 미사용 — 다른 build_*.py와 시그니처 대칭 유지)
        features_df: FEATURES_TABLE_PARQUET을 읽은 DataFrame — target_ts 쪽(날씨/캘린더/
            타겟)은 항상 이 전체 범위에서 조회한다(예: anchor를 11월로 좁혀도 horizon이
            큰 행의 target_ts가 12월로 넘어갈 수 있으므로 target 쪽은 좁히면 안 됨).
        anchor_df: anchor_ts(T0) 쪽 후보를 좁히고 싶을 때만 지정 — 예: 학습 기간만큼만
            anchor를 뽑아 self-join 결과 행 수를 줄이는 용도(EMR 전체 연도 빌드에선
            불필요, 로컬에서 좁은 기간만 학습할 때 규모를 줄이려고 추가된 옵션).
            None이면 features_df 전체를 anchor로도 쓴다(기존 동작과 동일).
    returns:
        DataFrame: station_id, anchor_ts, horizon, LAG_ROLLING_FEATURE_COLUMNS(anchor_ts
            기준), 날씨/인구/캘린더/rental_exposure/rental_count/return_count/date(target_ts
            기준)를 horizon 1..HORIZON_COUNT만큼 union한 것
    """
    del spark  # 시그니처 대칭용 — 이 함수 자체는 SparkSession을 직접 쓰지 않음
    anchor_source = anchor_df if anchor_df is not None else features_df
    anchor = anchor_source.select(*ANCHOR_COLUMNS)
    target = features_df.select(*TARGET_COLUMNS)

    horizon_frames = [_shift_for_horizon(anchor, target, h) for h in range(1, config.HORIZON_COUNT + 1)]
    combined = horizon_frames[0]
    for frame in horizon_frames[1:]:
        combined = combined.unionByName(frame)
    return combined


if __name__ == "__main__":
    import os

    from .spark_session import get_spark

    spark = get_spark()
    features = spark.read.parquet(config.FEATURES_TABLE_PARQUET)

    # 로컬에서 좁은 기간만 학습할 때(디스크/시간 제약) anchor 쪽만 좁히기 위한 옵션 —
    # 미지정 시(EMR 전체 빌드 등) 기존과 동일하게 전체 범위를 anchor로 쓴다. target 쪽은
    # 항상 features 전체를 그대로 써서 anchor 끝 근처 horizon이 범위를 벗어나지 않게 한다.
    anchor_since = os.environ.get("MULTI_HORIZON_ANCHOR_SINCE")
    anchor_until = os.environ.get("MULTI_HORIZON_ANCHOR_UNTIL")
    # anchor를 5분 tick 전체로 유지하면 이 self-join 결과는 원본의 최대 HORIZON_COUNT배
    # 행 수가 된다 — 실측(2025-11 한 달, station ~2,977개)으로 2.6억 행이 나와 로컬
    # 학습 머신(RAM 18GB)에서 판다스로 못 읽는다(pd.read_parquet가 출력도 없이 OOM kill됨).
    # EMR처럼 그 규모를 받아낼 수 있는 환경이 아니면(로컬 1회성 검증 등) 이 옵션으로
    # anchor를 정시(매시 0분)로만 좁혀 12배 줄인다 — target_ts/타겟 라벨은 그대로 원본
    # 5분 tick 값이라 서빙 정밀도와는 무관(서빙은 predict_single.py가 라이브로 계산).
    anchor_hourly_only = os.environ.get("MULTI_HORIZON_ANCHOR_HOURLY_ONLY") == "1"
    anchor_input = None
    if anchor_since or anchor_until or anchor_hourly_only:
        anchor_input = features
        if anchor_since:
            anchor_input = anchor_input.filter(F.col("hour_ts") >= F.lit(anchor_since))
        if anchor_until:
            anchor_input = anchor_input.filter(F.col("hour_ts") < F.lit(anchor_until))
        if anchor_hourly_only:
            anchor_input = anchor_input.filter(F.minute(F.col("hour_ts")) == 0)

    result = build_multi_horizon_features(spark, features, anchor_df=anchor_input)
    result.write.mode("overwrite").parquet(config.MULTI_HORIZON_FEATURES_TABLE_PARQUET)
    print(f"multi-horizon features -> {config.MULTI_HORIZON_FEATURES_TABLE_PARQUET}")
