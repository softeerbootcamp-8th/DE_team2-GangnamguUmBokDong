"""feature_engine이 만든 tick 단위 feature 테이블(FEATURES_TABLE_PARQUET, horizon=1
전용)을 horizon=1..HORIZON_COUNT 학습 테이블로 확장한다 (PySpark).

**핵심 아이디어(history.md 18번 항목에서 실험·검증됨) — "horizon을 feature로"**: 별도 모델을
horizon마다 두거나 예측값을 재귀적으로 다음 입력에 먹이는 대신(오차 누적), lag(직전
실적)는 항상 "지금(anchor_ts=T0)" 기준으로 고정하고, "몇 시간 뒤를 묻는지"만 horizon
feature로 모델에 알려준다. 그래서 이 테이블의 한 행은 **원본 테이블의 서로 다른 두 시점을
조합**한 것뿐이다:

- anchor_ts(T0) 쪽에서: 그 모델의 lag 컬럼(`rental_lag_1h` 또는 `return_lag_1h`, "지금 아는 것") 1개.
- target_ts(T0+(horizon-1)시간) 쪽에서: 날씨/인구/캘린더/(대여면 `rental_exposure`)/타겟 카운트
  ("그 미래 시점에 실제로 어땠는지") + `date`.

horizon=1이면 anchor_ts==target_ts라 원본 테이블의 해당 행과 완전히 같은 값이 나온다 —
`tests/dev_spark_multi_horizon_parity.py`가 이 불변조건을 회귀 테스트로 고정한다.

**대여/반납이 완전히 분리된 데이터셋이다** — 서로 상대방의 lag를 보지 않으므로
(`RENTAL_ANCHOR_COLUMNS`/`RETURN_ANCHOR_COLUMNS`가 서로 다름) self-join도, 결과
테이블도 따로 만든다. `run_pipeline.py`가 대여 쪽을 끝까지 만들어 S3에 쓰고 나서
반납 쪽을 시작한다 — 이 self-join 자체가 원본의 최대 HORIZON_COUNT배 행 수로
불어나므로(아래 "규모" 참고), 두 개를 동시에 메모리에 띄우지 않기 위함이다.

**`date`를 target_ts 쪽에서 가져오는 이유**: `training/train_common._split()`이 `date`로
train/valid/test 경계를 가른다. 라벨(타겟 이벤트)이 실제로 언제 일어났는지를 기준으로 잘라야
walk-forward 검증이 안전하다 — anchor_ts 기준으로 자르면 horizon이 큰 행의 라벨이 다음
split으로 새는 누출이 생긴다.

**station 활성 구간 밖 처리**: target_ts에 해당하는 행이 그리드에 없으면(station 비활성
구간이거나, 아직 관측되지 않은 미래라 증분 파이프라인이 그 시점까지 못 만들었을 때) inner
join으로 그 (anchor, horizon) 조합 자체가 자연히 빠진다 — build_merged_table.py의 "그리드
구멍" 철학과 동일. 12/31 오후처럼 target_ts가 다음 해로 넘어가는 앵커도 같은 이유로
자동으로 걸러진다(별도 예외 처리 불필요) — 워터마크 기반 증분 생성을 나중에 이
파이프라인에 붙일 때도 이 성질 덕분에 "target이 아직 없으면 그냥 빠지고, 다음 증분 때
채워짐"이 자동으로 성립한다.

**규모**: T0을 원본과 동일하게 tick 전체로 유지하므로 이 테이블은 원본의 최대
HORIZON_COUNT배 행 수가 된다 — 로컬 `local[*]`로 전체 연도를 처리하는 건 비현실적이고
EMR 대상이다(로컬 검증은 짧은 기간의 합성 데이터로만).
"""

from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config

_COMMON_TARGET_COLUMNS = [
    "station_id",
    "hour_ts",
    "capacity",
    "lat",
    "lon",
    "temp",
    "precip",
    "pop_total",
    "hour",
    "dow",
    "is_holiday",
    "day",
    "date",
]

RENTAL_ANCHOR_COLUMNS = ["station_id", "hour_ts", "rental_lag_1h"]
RENTAL_TARGET_COLUMNS = [*_COMMON_TARGET_COLUMNS, "rental_exposure", "rental_count"]
RETURN_ANCHOR_COLUMNS = ["station_id", "hour_ts", "return_lag_1h"]
RETURN_TARGET_COLUMNS = [*_COMMON_TARGET_COLUMNS, "return_count"]


def _shift_for_horizon(anchor: DataFrame, target: DataFrame, horizon: int) -> DataFrame:
    """anchor(각 행이 T0)에 target_ts=T0+(horizon-1)시간의 컬럼들을 붙이고 horizon을 단다.

    `build_features._exact_hour_lag()`와 정확히 같은 self-join 기법(정확히 그 시각의 행이
    없으면 자동으로 빠짐)을 반대 방향(과거가 아니라 미래 조회)으로 쓴다.

    args:
        anchor: station_id, hour_ts=T0, lag 컬럼 1개만 담은 DataFrame
        target: station_id, hour_ts, 날씨/캘린더/타겟/date를 담은 DataFrame
        horizon: 1~HORIZON_COUNT
    returns:
        DataFrame: station_id, anchor_ts(T0), horizon, lag 1개, target 컬럼 중
            station_id/hour_ts를 뺀 나머지(target_ts 기준)
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
    spark: SparkSession,
    features_df: DataFrame,
    anchor_columns: list[str],
    target_columns: list[str],
    anchor_df: DataFrame | None = None,
) -> DataFrame:
    """FEATURES_TABLE_PARQUET(horizon=1 전용, tick 단위)를 horizon=1..HORIZON_COUNT 학습
    테이블로 확장한다 — 대여/반납 어느 쪽이든 이 함수 하나를 컬럼셋만 바꿔 재사용한다.

    args:
        spark: SparkSession (현재는 미사용 — 다른 build_*.py와 시그니처 대칭 유지)
        features_df: FEATURES_TABLE_PARQUET을 읽은 DataFrame — target_ts 쪽(날씨/캘린더/
            타겟)은 항상 이 전체 범위에서 조회한다(예: anchor를 11월로 좁혀도 horizon이
            큰 행의 target_ts가 12월로 넘어갈 수 있으므로 target 쪽은 좁히면 안 됨).
        anchor_columns: `RENTAL_ANCHOR_COLUMNS` 또는 `RETURN_ANCHOR_COLUMNS`
        target_columns: `RENTAL_TARGET_COLUMNS` 또는 `RETURN_TARGET_COLUMNS`
        anchor_df: anchor_ts(T0) 쪽 후보를 좁히고 싶을 때만 지정 — 예: 학습 기간만큼만
            anchor를 뽑아 self-join 결과 행 수를 줄이는 용도(EMR 전체 연도 빌드에선
            불필요, 로컬에서 좁은 기간만 학습할 때 규모를 줄이려고 추가된 옵션).
            None이면 features_df 전체를 anchor로도 쓴다(기존 동작과 동일).
    returns:
        DataFrame: station_id, anchor_ts, horizon, lag 컬럼 1개(anchor_ts 기준),
            날씨/인구/캘린더/(대여면 rental_exposure)/타겟 카운트/date(target_ts 기준)를
            horizon 1..HORIZON_COUNT만큼 union한 것
    """
    del spark  # 시그니처 대칭용 — 이 함수 자체는 SparkSession을 직접 쓰지 않음
    anchor_source = anchor_df if anchor_df is not None else features_df
    anchor = anchor_source.select(*anchor_columns)
    target = features_df.select(*target_columns)

    horizon_frames = [_shift_for_horizon(anchor, target, h) for h in range(1, config.HORIZON_COUNT + 1)]
    combined = horizon_frames[0]
    for frame in horizon_frames[1:]:
        combined = combined.unionByName(frame)
    return combined


def _anchor_input(features: DataFrame) -> DataFrame | None:
    """환경변수로 지정된 anchor 밀도 축소 옵션을 적용한 anchor 후보 DataFrame을 만든다.

    로컬에서 좁은 기간만 학습할 때(디스크/시간 제약) anchor 쪽만 좁히기 위한
    옵션 — 미지정 시(EMR 전체 빌드 등) None을 반환해 build_multi_horizon_features()가
    features_df 전체를 anchor로 쓰게 한다. target 쪽은 항상 features 전체를 그대로
    써서 anchor 끝 근처 horizon이 범위를 벗어나지 않게 한다.
    """
    import os

    anchor_since = os.environ.get("MULTI_HORIZON_ANCHOR_SINCE")
    anchor_until = os.environ.get("MULTI_HORIZON_ANCHOR_UNTIL")
    # anchor를 tick 전체로 유지하면 이 self-join 결과는 원본의 최대 HORIZON_COUNT배
    # 행 수가 된다 — 실측(2025-11 한 달, station ~2,977개, 5분 tick 시절)으로 2.6억
    # 행이 나와 로컬 학습 머신(RAM 18GB)에서 판다스로 못 읽었다(pd.read_parquet가
    # 출력도 없이 OOM kill됨). EMR처럼 그 규모를 받아낼 수 있는 환경이 아니면(로컬
    # 1회성 검증 등) 이 옵션으로 anchor를 성기게 줄인다 — target_ts/타겟 라벨은
    # 그대로 원본 tick 값이라 서빙 정밀도와는 무관(서빙은 predict_single.py가
    # 라이브로 계산).
    anchor_hourly_only = os.environ.get("MULTI_HORIZON_ANCHOR_HOURLY_ONLY") == "1"
    # 정각(60분)보다 촘촘하되 tick 전체보다는 성긴 임의 간격으로 anchor를 뽑고
    # 싶을 때 — MULTI_HORIZON_ANCHOR_HOURLY_ONLY=1은 사실 이 값의 60분짜리 특수
    # 케이스와 같다(둘 다 켜져 있으면 이 값이 우선).
    anchor_tick_minutes = os.environ.get("MULTI_HORIZON_ANCHOR_TICK_MINUTES")
    if not (anchor_since or anchor_until or anchor_hourly_only or anchor_tick_minutes):
        return None

    anchor_input = features
    if anchor_since:
        anchor_input = anchor_input.filter(F.col("hour_ts") >= F.lit(anchor_since))
    if anchor_until:
        anchor_input = anchor_input.filter(F.col("hour_ts") < F.lit(anchor_until))
    if anchor_tick_minutes:
        tick = int(anchor_tick_minutes)
        anchor_input = anchor_input.filter((F.hour(F.col("hour_ts")) * 60 + F.minute(F.col("hour_ts"))) % tick == 0)
    elif anchor_hourly_only:
        anchor_input = anchor_input.filter(F.minute(F.col("hour_ts")) == 0)
    return anchor_input


def _run_cli() -> None:
    from .spark_session import get_spark

    spark = get_spark()
    features = spark.read.parquet(config.FEATURES_TABLE_PARQUET)
    anchor_input = _anchor_input(features)

    # 대여를 끝까지 만들어 S3에 쓰고 나서 반납을 시작한다 — 두 self-join 결과
    # (원본의 최대 HORIZON_COUNT배 행 수) 를 동시에 메모리에 띄우지 않기 위함.
    rental_result = build_multi_horizon_features(
        spark, features, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS, anchor_df=anchor_input
    )
    # date 파티션으로 써야 training/monitor_performance가 core.s3.read_parquet()의
    # date_range로 필요한 기간만 읽을 수 있다(전체 히스토리를 매번 훑지 않음) — s3.py
    # 모듈 docstring 참고.
    rental_result.write.mode("overwrite").partitionBy("date").parquet(
        config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    )
    print(f"rental multi-horizon features -> {config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET}")

    return_result = build_multi_horizon_features(
        spark, features, RETURN_ANCHOR_COLUMNS, RETURN_TARGET_COLUMNS, anchor_df=anchor_input
    )
    return_result.write.mode("overwrite").partitionBy("date").parquet(
        config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    )
    print(f"return multi-horizon features -> {config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET}")


if __name__ == "__main__":
    try:
        _run_cli()
    except Exception as exc:
        # training/scripts/monthly_retrain_check.py가 이 스크립트를 subprocess로
        # 띄운다 — 표준출력이 그대로 스트리밍되므로, 실패 사유를 알아보기 쉬운
        # 한 줄로 여기 남겨야 오케스트레이터 로그만 보고도 원인을 알 수 있다.
        print(f"[build_multi_horizon_features] 실패: {exc}", flush=True)
        raise
