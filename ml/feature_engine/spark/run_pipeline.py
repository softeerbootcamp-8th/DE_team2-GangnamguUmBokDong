"""피처마트(2차 정제) 생성 파이프라인 엔트리포인트 (EMR spark-submit 대상).

파라미터 조합(`config.PARAM_COMBO_ID`)별로 워터마크를 확인하여 전체 빌드 또는 증분 빌드를 수행한다.
- 최초 실행: Archive fact와 Silver 마스터 데이터를 읽어 1차 정제 산출물을 생성하고 피처마트를 전체 빌드한다.
- 증분 실행: INCREMENTAL_LOOKBACK_HOURS 구간을 자정 경계로 내림하여 해당 날짜 파티션들을 동적 덮어쓰기(Dynamic Partition Overwrite)한다.
"""


import argparse
import os
from datetime import datetime, timedelta

from core import s3 as s3_io
from pyspark.sql import functions as F


from . import config
from .build_features import build_features
from .build_merged_table import _weather_context_start, build_merged_table
from .build_rolling_rental_features import build_rolling_rental_features
from .build_targets import build_targets
from .silver_source import (
    read_population,
    read_station_master,
    read_station_status,
    read_weather,
)
from .spark_session import get_spark
from .watermark import is_fresh, read_watermark, write_watermark



def _current_params() -> dict:
    """현재 feature parameter 조합을 watermark 기록 형식으로 반환한다."""
    return {
        "window_minutes": config.ROLLING_WINDOW_MINUTES,
        "embargo_minutes": config.ROLLING_EMBARGO_MINUTES,
        "tick_minutes": config.ROLLING_TICK_MINUTES,
    }


def _window_timestamp_bounds() -> tuple[str, str]:
    """inclusive 날짜 window를 Spark용 `[since, until)` timestamp 경계로 바꾼다."""
    since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
    until = (config.WINDOW_END + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    return since, until


def _target_complete_through(until: str) -> str:
    """source upper bound 안에서 전체 target horizon이 완결되는 마지막 tick을 반환한다."""
    return (
        datetime.fromisoformat(until)
        - timedelta(minutes=config.TARGET_HORIZON_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")


def _explicit_window_requested() -> bool:
    """최초학습 등 명시적 고정 window 환경변수가 설정됐는지 반환한다."""
    return "TRAIN_WINDOW_START" in os.environ and "TRAIN_WINDOW_END" in os.environ


def _refresh_primary_tables(
    spark,
    since: str | None = None,
    until: str | None = None,
) -> None:
    """Archive fact와 최신 Silver master에서 1차 정제 테이블을 통째로
    다시 만들어 `build_merged_table.py`가 읽는 경로(`config.STATION_MASTER_PARQUET` 등)에
    저장한다.

    이 5개는 전체 빌드든 증분 빌드든 항상 **target `since`부터 `until` 직전까지
    필요한 범위를 전체 재계산**한다. 단, weather는 첫 target tick도 inference와
    같은 as-of fallback을 할 수 있도록 `since` 이전 최대 3시간 source context를
    중간 산출물에 함께 둔다 —
    station_status(연 22M행 규모)/weather(연 8,760행)/population/targets는 EMR Spark
    풀 리빌드로 감당 못 할 크기가 아니고, 예전처럼 "이미 어딘가에 존재하는 1차 정제
    산출물"이 아니라 이제 이 패키지가 Archive/current Silver에서 만들어내므로 부분 갱신 로직을
    따로 둘 이유가 없다. 증분 실행에서 실제로 아끼는 부분은 그 뒤 단계(대여이력
    lag/rolling 재계산, `build_rolling_rental_features`/`build_merged_table`의 `since`)다.

    `since`/`until`은 archive 일별 파일의 정확한 목록과 행 timestamp 재필터에 함께
    쓰인다. 호출부(`_run_full_build()`/`_run_incremental()`)는 항상
    `config.WINDOW_START`와 `WINDOW_END + 1일 00:00`을 넘기며, 둘을 생략해 직접
    호출하면 같은 config 경계를 기본으로 쓴다. station_master는 과거 enriched
    snapshot이 있다는 보장이 없는 serving용 current dimension이라 시간 필터 대상이
    아니며 항상 최신 Silver를 쓴다.
    """
    window_since, window_until = _window_timestamp_bounds()
    since = since or window_since
    until = until or window_until

    read_station_master(spark).write.mode("overwrite").parquet(config.STATION_MASTER_PARQUET)
    read_station_status(spark, since=since, until=until).write.mode("overwrite").parquet(
        config.STATION_STATUS_PARQUET
    )
    # 최종 target window의 첫 tick도 inference와 동일하게 직전 최대 3시간 관측을
    # fallback할 수 있도록 weather 중간 산출물에만 앞쪽 context를 보존한다.
    read_weather(
        spark,
        since=_weather_context_start(since),
        until=until,
    ).write.mode("overwrite").parquet(config.WEATHER_PARQUET)
    read_population(spark, since=since, until=until).write.mode("overwrite").parquet(
        config.POPULATION_PARQUET
    )

    rental_targets, return_targets = build_targets(spark, since=since, until=until)
    rental_targets.write.mode("overwrite").parquet(config.TARGETS_PARQUET)
    return_targets.write.mode("overwrite").parquet(config.RETURN_TARGETS_PARQUET)


def _run_full_build(spark) -> None:
    """워터마크가 없거나 명시적 window일 때 해당 구간을 처음부터 만든다."""
    window_since, window_until = _window_timestamp_bounds()
    print(
        f"[{config.PARAM_COMBO_ID}] 전체 overwrite -> Archive에서 "
        f"{window_since}~{config.WINDOW_END} 구간으로 처음부터 생성"
    )

    _refresh_primary_tables(spark, since=window_since, until=window_until)

    rolling_context_since = (
        datetime.fromisoformat(window_since)
        - timedelta(minutes=config.ROLLING_WINDOW_MINUTES + config.ROLLING_EMBARGO_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    build_rolling_rental_features(
        spark,
        output_path=config.ROLLING_RENTAL_FEATURES_PARQUET,
        since=rolling_context_since,
        until=window_until,
    )

    merged = build_merged_table(spark, since=window_since, until=window_until)
    merged.write.mode("overwrite").parquet(config.MERGED_TABLE_PARQUET)

    merged_reloaded = spark.read.parquet(config.MERGED_TABLE_PARQUET)
    features_df = build_features(spark, merged_reloaded)
    target_complete_through = _target_complete_through(window_until)
    features_df = features_df.filter(
        (F.col("hour_ts") >= F.lit(window_since))
        & (F.col("hour_ts") <= F.lit(target_complete_through))
    )
    # 증분 실행이 date 파티션 단위 overwrite로 과거 구간을 사후 보정하므로(아래
    # _run_incremental 참고), 전체 빌드도 처음부터 같은 파티션 레이아웃으로 써야 한다.
    features_df.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)

    # 방금 저장한 Parquet에서 max(hour_ts)를 직접 읽어와 무거운 upstream 재계산을 방지한다.
    max_hour_row = spark.read.parquet(config.FEATURES_TABLE_PARQUET).agg(F.max("hour_ts")).collect()[0][0]
    if max_hour_row is not None:
        max_hour_str = max_hour_row.isoformat() if hasattr(max_hour_row, "isoformat") else str(max_hour_row)
        write_watermark(config.WATERMARK_PATH, max_hour_str, _current_params())
        print(f"[{config.PARAM_COMBO_ID}] 전체 빌드 완료 -> {config.FEATURES_TABLE_PARQUET} (워터마크={max_hour_str})")
    else:
        print(f"[{config.PARAM_COMBO_ID}] 전체 빌드 완료 (데이터 없음) -> {config.FEATURES_TABLE_PARQUET}")



def _incremental_since(watermark_dt: datetime) -> datetime:
    """증분 재계산을 시작할 시각(자정 경계로 내림) — 모듈 docstring의 overwrite 근거 참고.

    args:
        watermark_dt: 이전 실행이 기록한 max_hour_ts
    returns:
        datetime: lookback을 적용한 뒤 그 날짜 00:00:00으로 내린 시각
    """
    lookback_dt = watermark_dt - timedelta(hours=config.INCREMENTAL_LOOKBACK_HOURS)
    return lookback_dt.replace(hour=0, minute=0, second=0, microsecond=0)


def _reject_if_legacy_flat_layout() -> None:
    """`FEATURES_TABLE_PARQUET` 밑에 `date=` 파티션이 아닌 구버전 flat parquet
    파일(이 프로젝트가 append 모드로 쓰던 시절의 잔재)이 섞여 있으면 즉시 실패한다.

    `_run_incremental()`의 dynamic partition overwrite는 이번 실행에 실제로 등장한
    `date=` 파티션만 통째로 교체하고 그 밖의 파일은 손대지 않는다 — 만약 이 prefix에
    (과거 `partitionBy("date")` 도입 이전에 append로 쌓아둔) 파티션 없는 flat 파일이
    남아있다면, 그 뒤로 `spark.read.parquet(FEATURES_TABLE_PARQUET)`가 그 flat 파일과
    새 `date=` 파티션 파일을 모두 읽어 겹치는 기간이 조용히 중복 집계된다.

    이 상태를 자동으로 지우거나 그냥 넘어가지 않고 바로 실패시킨다 — 운영자가
    직접 워터마크를 지우고 `_run_full_build()`(정적 overwrite라 디렉터리를 통째로
    새로 씀)를 한 번 실행하거나, 구버전 파일을 수동으로 정리한 뒤 다시 실행하게
    한다(그래야 어느 쪽이 맞는 판단인지 — 데이터를 버려도 되는지 — 운영자가 직접
    확인할 수 있다).

    raises:
        RuntimeError: `date=`가 아닌 최상위 flat parquet 파일이 하나라도 있을 때
    """
    prefix = f"{config.FEATURES_TABLE_KEY}/"
    flat_files = [key for key in s3_io.list_keys(prefix) if key.endswith(".parquet") and "date=" not in key]
    if flat_files:
        raise RuntimeError(
            f"{prefix} 밑에 date= 파티션이 아닌 구버전 flat parquet 파일이 {len(flat_files)}개 "
            f"남아있습니다(예: {flat_files[0]}). 이 상태로 증분 실행하면 그 파일까지 같이 읽혀서 "
            "겹치는 기간이 중복 집계됩니다 — 워터마크를 지우고 _run_full_build()를 한 번 "
            "실행하거나(디렉터리를 통째로 새로 씀), 구버전 파일을 수동으로 정리한 뒤 다시 "
            "실행하세요."
        )


def _run_incremental(spark, watermark: dict) -> None:
    """워터마크가 있을 때 — lookback 구간(자정 경계로 내림)부터 다시 계산해서, 그
    구간에 걸리는 날짜 파티션을 통째로 덮어쓴다.

    append가 아니라 overwrite인 이유, 파티션을 자정 경계로 내려야 하는 이유는 모듈
    docstring 참고 — 요약하면 대여이력은 반납 완료 시에야 Archive에 나타날 수 있으므로, 이미
    발행된 과거 날짜의 `rental_count`가 뒤늦게 늘어날 수 있고 이 재계산이 그걸
    보정한다.

    args:
        spark: SparkSession
        watermark: watermark.read_watermark()의 결과 (max_hour_ts 포함)
    raises:
        RuntimeError: `_reject_if_legacy_flat_layout()` 참고 — 구버전 flat parquet이
            섞여 있으면 실제 재계산을 시작하기 전에 여기서 먼저 걸린다
    """
    _reject_if_legacy_flat_layout()

    watermark_dt = datetime.fromisoformat(watermark["max_hour_ts"])
    since_dt = _incremental_since(watermark_dt)
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{config.PARAM_COMBO_ID}] 워터마크={watermark_dt} -> {since_str}부터 재계산(증분, "
          f"lookback={config.INCREMENTAL_LOOKBACK_HOURS}시간, 날짜 경계로 내림)")

    read_since_dt = since_dt - timedelta(days=1)
    read_since_str = read_since_dt.strftime("%Y-%m-%d %H:%M:%S")

    window_since, window_until = _window_timestamp_bounds()
    _refresh_primary_tables(spark, since=window_since, until=window_until)

    rolling_tmp_path = f"{config.OUTPUT_ROOT}/_rolling_incremental_tmp.parquet"
    build_rolling_rental_features(
        spark,
        output_path=rolling_tmp_path,
        since=read_since_str,
        until=window_until,
    )

    merged_increment = build_merged_table(
        spark,
        since=read_since_str,
        until=window_until,
    )
    features_increment = build_features(spark, merged_increment, rolling_parquet_path=rolling_tmp_path)
    target_complete_through = _target_complete_through(window_until)
    features_increment = features_increment.filter(
        (F.col("hour_ts") >= F.lit(since_str))
        & (F.col("hour_ts") <= F.lit(target_complete_through))
    )
    # 액션 반복 실행 시 상류 연산 중복을 방지하기 위해 캐싱 적용
    features_increment = features_increment.cache()


    if features_increment.limit(1).count() == 0:
        print(f"[{config.PARAM_COMBO_ID}] 재계산 구간({since_str}~)에 데이터 없음 — 건너뜀")
        features_increment.unpersist()
        return

    new_count = features_increment.filter(F.col("hour_ts") > F.lit(watermark["max_hour_ts"])).count()

    # append가 아니라 재계산 구간에 걸리는 날짜 파티션을 통째로 교체한다 — dynamic
    # partition overwrite는 이 DataFrame에 실제로 등장하는 날짜 파티션만 건드리고
    # 나머지(lookback 밖의 과거 데이터)는 그대로 둔다.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    features_increment.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)

    max_hour_row = features_increment.agg(F.max("hour_ts")).collect()[0][0]
    features_increment.unpersist()
    if max_hour_row is not None:
        max_hour_str = max_hour_row.isoformat() if hasattr(max_hour_row, "isoformat") else str(max_hour_row)
        write_watermark(config.WATERMARK_PATH, max_hour_str, _current_params())
        print(f"[{config.PARAM_COMBO_ID}] {since_str}~{max_hour_str} 재계산(신규 {new_count:,}행 포함) -> "
              f"{config.FEATURES_TABLE_PARQUET} 날짜 파티션 덮어씀 (워터마크 갱신={max_hour_str})")
    else:
        print(f"[{config.PARAM_COMBO_ID}] 증분 계산 결과 행 없음")


def main() -> None:
    """피처마트 파이프라인을 실행한다 (워터마크 유무에 따라 전체 빌드 또는 증분 실행)."""
    parser = argparse.ArgumentParser(description="Base feature mart pipeline")
    parser.add_argument("--force", action="store_true", help="워터마크 신선도를 무시하고 강제 실행")
    args, _ = parser.parse_known_args()

    spark = get_spark()
    watermark = read_watermark(config.WATERMARK_PATH)
    # 명시적 window나 --force는 같은 profile output에 과거 rolling 실행의 바깥 파티션이 남아
    # 있을 수 있으므로, watermark 존재 여부/신선도와 무관하게 전체 overwrite한다.
    if watermark is None or _explicit_window_requested() or args.force:
        _run_full_build(spark)
    else:
        window_until = None if config.WINDOW_END is None else str(config.WINDOW_END)
        target_complete_through = _target_complete_through(window_until)
        if (
            is_fresh(watermark, max_age_hours=24.0)
            and watermark.get("max_hour_ts")
            and watermark["max_hour_ts"] >= target_complete_through
        ):
            print(
                f"[{config.PARAM_COMBO_ID}] Base feature 마트가 이미 최신 상태"
                f"(워터마크={watermark['max_hour_ts']}, 갱신={watermark.get('updated_at')})입니다 "
                "— 실행 건너뜀"
            )
            spark.stop()
            return
        _run_incremental(spark, watermark)





if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        # training/scripts/monthly_retrain_check.py가 이 스크립트를 subprocess로
        # 띄운다 — 표준출력이 그대로 스트리밍되므로, 실패 사유를 알아보기 쉬운
        # 한 줄로 여기 남겨야 오케스트레이터 로그만 보고도 원인을 알 수 있다.
        print(f"[run_pipeline] 실패: {exc}", flush=True)
        raise
