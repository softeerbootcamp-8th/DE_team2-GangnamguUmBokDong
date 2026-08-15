"""피처마트(2차 정제) 생성 파이프라인 엔트리포인트 — EMR `spark-submit` 대상.

파라미터 조합(`config.PARAM_COMBO_ID` — window/embargo/tick 값으로 정해짐)별로
워터마크(`watermark.py`)를 확인한다:

- **워터마크가 없으면**(이 조합으로 처음 만드는 것) 1차 정제 산출물 전체로 처음부터
  만든다.
- **워터마크가 있으면** "워터마크 - `config.INCREMENTAL_LOOKBACK_HOURS`"부터만 다시
  계산해서(lag_168h 등이 과거를 참조할 수 있도록 안전 마진을 둠), 워터마크보다
  최신인 행만 걸러 기존 피처마트에 **append**한다 — 전체 재계산을 피한다.

다른 파라미터 조합(다른 모델)은 `config.OUTPUT_ROOT`가 조합 ID로 이미 분리돼 있어서
서로 겹치지 않는다 — 조합별로 이 스크립트를 각자 실행하면 된다(예:
`ROLLING_EMBARGO_MINUTES=45 python -m feature_engineering.spark.run_pipeline`).

실행 예:
    로컬: ./.venv-spark/bin/python -m feature_engineering.spark.run_pipeline
    EMR:  spark-submit --deploy-mode cluster feature_engineering/run_pipeline.py
"""

from datetime import datetime, timedelta

from pyspark.sql import functions as F

from . import config
from .build_features import build_features
from .build_merged_table import build_merged_table
from .build_rolling_rental_features import build_rolling_rental_features
from .spark_session import get_spark
from .watermark import read_watermark, write_watermark


def _current_params() -> dict:
    return {
        "window_minutes": config.ROLLING_WINDOW_MINUTES,
        "embargo_minutes": config.ROLLING_EMBARGO_MINUTES,
        "tick_minutes": config.ROLLING_TICK_MINUTES,
    }


def _run_full_build(spark) -> None:
    """워터마크가 없을 때 — 1차 정제 산출물 전체로 처음부터 만든다."""
    print(f"[{config.PARAM_COMBO_ID}] 워터마크 없음 -> 전체 히스토리로 처음부터 생성")

    build_rolling_rental_features(spark, output_path=config.ROLLING_RENTAL_FEATURES_PARQUET)

    merged = build_merged_table(spark)
    merged.write.mode("overwrite").parquet(config.MERGED_TABLE_PARQUET)

    merged_reloaded = spark.read.parquet(config.MERGED_TABLE_PARQUET)
    features_df = build_features(spark, merged_reloaded)
    features_df.write.mode("overwrite").parquet(config.FEATURES_TABLE_PARQUET)

    max_hour_ts = features_df.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(config.WATERMARK_PATH, max_hour_ts.isoformat(), _current_params())
    print(f"[{config.PARAM_COMBO_ID}] 전체 빌드 완료 -> {config.FEATURES_TABLE_PARQUET} (워터마크={max_hour_ts})")


def _run_incremental(spark, watermark: dict) -> None:
    """워터마크가 있을 때 — lookback 구간부터 다시 계산해서 새 행만 append한다.

    args:
        spark: SparkSession
        watermark: watermark.read_watermark()의 결과 (max_hour_ts 포함)
    """
    watermark_dt = datetime.fromisoformat(watermark["max_hour_ts"])
    since_dt = watermark_dt - timedelta(hours=config.INCREMENTAL_LOOKBACK_HOURS)
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{config.PARAM_COMBO_ID}] 워터마크={watermark_dt} -> {since_str}부터 재계산(증분, "
          f"lookback={config.INCREMENTAL_LOOKBACK_HOURS}시간)")

    # rolling_rental_features는 매번 챔피언 경로에 영구 저장하지 않는다 — lookback
    # 구간(기본 35일)만 있으면 항상 다시 계산할 수 있을 만큼 가벼워서(창 폭이 최대
    # 90분), 매 증분마다 저장소를 늘리기보다 build_features가 읽을 임시 parquet으로만
    # 써둔다.
    rolling_tmp_path = f"{config.OUTPUT_ROOT}/_rolling_incremental_tmp.parquet"
    build_rolling_rental_features(spark, output_path=rolling_tmp_path, since=since_str)

    merged_increment = build_merged_table(spark, since=since_str)
    features_increment = build_features(spark, merged_increment, rolling_parquet_path=rolling_tmp_path)

    new_rows = features_increment.filter(F.col("hour_ts") > F.lit(watermark["max_hour_ts"]))
    new_count = new_rows.count()
    if new_count == 0:
        print(f"[{config.PARAM_COMBO_ID}] 새 데이터 없음(워터마크 이후 행이 0개) — append 생략")
        return

    new_rows.write.mode("append").parquet(config.FEATURES_TABLE_PARQUET)

    max_hour_ts = new_rows.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(config.WATERMARK_PATH, max_hour_ts.isoformat(), _current_params())
    print(f"[{config.PARAM_COMBO_ID}] 증분 {new_count:,}행 append -> {config.FEATURES_TABLE_PARQUET} "
          f"(워터마크 갱신={max_hour_ts})")


def main() -> None:
    spark = get_spark()
    watermark = read_watermark(config.WATERMARK_PATH)
    if watermark is None:
        _run_full_build(spark)
    else:
        _run_incremental(spark, watermark)


if __name__ == "__main__":
    main()
