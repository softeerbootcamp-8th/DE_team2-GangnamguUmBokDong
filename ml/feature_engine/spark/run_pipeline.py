"""피처마트(2차 정제) 생성 파이프라인 엔트리포인트 — EMR `spark-submit` 대상.

파라미터 조합(`config.PARAM_COMBO_ID` — window/embargo/tick 값으로 정해짐)별로
워터마크(`watermark.py`)를 확인한다:

- **워터마크가 없으면**(이 조합으로 처음 만드는 것) Silver 전체로 1차 정제 산출물
  (station_master/targets/station_status/weather/population, `silver_source.py`)을
  새로 만들고, 그걸로 피처마트를 처음부터 만든다.
- **워터마크가 있으면** "워터마크 - `config.INCREMENTAL_LOOKBACK_HOURS`"를 **그 날짜의
  자정으로 내림**한 시각부터 다시 계산해서, 그 날짜 이후 파티션을 통째로
  **덮어쓴다**(append가 아님 — 아래 "왜 append가 아니라 overwrite인가" 참고).

**왜 append가 아니라 (날짜 파티션) overwrite인가 — 대여 건수의 지연 반영 문제**:
Silver `bike_rental_history`는 트립이 **반납 완료된 시점에야** 한 행으로 나타난다
(`silver_source.read_rental_trips()` 참고) — 대여 시작 시각(`start_dt`)이 아니라
반납 시각을 기준으로 데이터가 들어온다. 즉 어떤 트립이 유난히 오래 걸리면(장시간
대여, 반납 실패 후 재시도 등), 그 트립의 `start_dt`가 가리키는 시간대는 **이미
피처마트에 발행되고 한참 지난 뒤에야** 실제 카운트에 반영될 수 있다 —
`INCREMENTAL_LOOKBACK_HOURS`(기본 840시간=35일)가 lag_168h(7일)보다 훨씬 넉넉한
이유도 이 여유를 감안한 것이다. 매 증분 실행이 lookback 구간을 "새 행 후보"가
아니라 **아직 보정될 수 있는 구간**으로 보고 항상 통째로 다시 계산·덮어써야만,
뒤늦게 나타난 트립이 이미 발행됐던 과거 시간대의 `rental_count`를 사후 보정한다.
append 방식(이전 구현)은 워터마크 이하 행을 전부 버렸기 때문에 이 보정이 절대
반영되지 않고 조용히 영구 누락됐다 — `return_count`는 반납 시각 자체가 곧
"데이터가 들어오는 시각"이라 이 문제가 없다(수집 지연은 있어도 사후 보정 대상인
"트립 진행 중"이 없음).

**overwrite가 파티션 경계로 내림해야 하는 이유**: `FEATURES_TABLE_PARQUET`는
`date`(`YYYY-MM-DD`) 컬럼으로 파티셔닝돼 있고, dynamic partition overwrite
(`spark.sql.sources.partitionOverwriteMode=dynamic`)는 "이번에 쓰는 DataFrame에
등장하는 날짜 파티션만" 통째로 교체한다. 재계산 구간이 하루 중간부터 시작하면
그 날의 앞부분(이번에 다시 안 만든 시간대)까지 통째로 지워지므로, 반드시 자정
경계로 내림한 뒤 그 날짜 전체를 다시 계산해야 한다.

**학습 시 주의 — "아직 확정 안 된 최근 구간"**: 워터마크 파일(`watermark.py`)의
`updated_at`(이 파이프라인이 실제로 실행된 시각) 기준으로 `updated_at -
INCREMENTAL_LOOKBACK_HOURS`보다 최신인 날짜는 아직 위 사후 보정 대상이다 — 다음
증분 실행 때 `rental_count`가 더 늘어날 수 있으므로, 그 구간을 학습/평가에 "확정된
라벨"로 쓰면 안 된다(과소집계된 값을 정답으로 학습하는 셈). 다만 `training/
config.py`의 `safety_cutoff_date()`, `monitor_performance.py`의 "완결된 달만
본다" 로직은 이 35일 전체가 아니라 더 짧은 `TRAINING_SAFETY_MARGIN_DAYS`(기본
7일 — "이 정도면 거의 다 반납됐다"는 실용적 판단)만 뺀다 — 35일을 그대로 쓰면
학습/모니터링이 항상 한두 달 뒤처진 데이터만 보게 돼서다. 즉 완전한 안전(35일)과
신선함(7일) 사이의 의도적인 트레이드오프이니, 새로 이런 판단을 넣을 땐 어느 쪽
마진이 맞는지(데이터 정확성이 더 중요한지, 최신성이 더 중요한지) 먼저 정할 것.

다른 파라미터 조합(다른 모델)은 `config.OUTPUT_ROOT`가 조합 ID로 이미 분리돼 있어서
서로 겹치지 않는다 — 조합별로 이 스크립트를 각자 실행하면 된다(예:
`ROLLING_EMBARGO_MINUTES=45 python -m feature_engine.spark.run_pipeline`).

실행 예:
    로컬: ./.venv-spark/bin/python -m feature_engine.spark.run_pipeline
    EMR:  spark-submit --deploy-mode cluster feature_engine/run_pipeline.py
"""

from datetime import datetime, timedelta

from core import s3 as s3_io
from pyspark.sql import functions as F

from . import config
from .build_features import build_features
from .build_merged_table import build_merged_table
from .build_rolling_rental_features import build_rolling_rental_features
from .build_targets import build_targets
from .silver_source import (
    read_population,
    read_station_master,
    read_station_status,
    read_weather,
)
from .spark_session import get_spark
from .watermark import read_watermark, write_watermark


def _current_params() -> dict:
    return {
        "window_minutes": config.ROLLING_WINDOW_MINUTES,
        "embargo_minutes": config.ROLLING_EMBARGO_MINUTES,
        "tick_minutes": config.ROLLING_TICK_MINUTES,
    }


def _refresh_primary_tables(spark, since: str | None = None) -> None:
    """Silver로부터 station_master/targets/station_status/weather/population을 통째로
    다시 만들어 `build_merged_table.py`가 읽는 경로(`config.STATION_MASTER_PARQUET` 등)에
    저장한다.

    이 5개는 전체 빌드든 증분 빌드든 항상 **`since`부터 지금까지 전체 재계산**한다 —
    station_status(연 22M행 규모)/weather(연 8,760행)/population/targets는 EMR Spark
    풀 리빌드로 감당 못 할 크기가 아니고, 예전처럼 "이미 어딘가에 존재하는 1차 정제
    산출물"이 아니라 이제 이 패키지가 직접 Silver에서 만들어내므로 부분 갱신 로직을
    따로 둘 이유가 없다. 증분 실행에서 실제로 아끼는 부분은 그 뒤 단계(대여이력
    lag/rolling 재계산, `build_rolling_rental_features`/`build_merged_table`의 `since`)다.

    **2026-08부터 `since`가 필수에 가까워짐** — `silver_source._silver_glob()`이
    더 이상 연도로 안 좁히므로(고정 TRAIN_YEAR 폐지, 롤링 윈도우로 전환), `since`
    없이 부르면 Silver에 쌓인 전체 히스토리를 매번 다시 계산하게 된다. 호출부
    (`_run_full_build()`/`_run_incremental()`)는 항상 `config.WINDOW_START`를
    넘긴다 — station_master는 시간 축이 없는 참조 테이블이라 `since` 필터 대상이
    아니다.
    """
    read_station_master(spark).write.mode("overwrite").parquet(config.STATION_MASTER_PARQUET)
    read_station_status(spark, since=since).write.mode("overwrite").parquet(config.STATION_STATUS_PARQUET)
    read_weather(spark, since=since).write.mode("overwrite").parquet(config.WEATHER_PARQUET)
    read_population(spark, since=since).write.mode("overwrite").parquet(config.POPULATION_PARQUET)

    rental_targets, return_targets = build_targets(spark, since=since)
    rental_targets.write.mode("overwrite").parquet(config.TARGETS_PARQUET)
    return_targets.write.mode("overwrite").parquet(config.RETURN_TARGETS_PARQUET)


def _run_full_build(spark) -> None:
    """워터마크가 없을 때 — Silver 전체로 1차 정제부터 처음부터 만든다."""
    window_since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
    print(
        f"[{config.PARAM_COMBO_ID}] 워터마크 없음 -> Silver에서 {window_since}~{config.WINDOW_END} "
        "구간으로 처음부터 생성"
    )

    _refresh_primary_tables(spark, since=window_since)

    build_rolling_rental_features(spark, output_path=config.ROLLING_RENTAL_FEATURES_PARQUET)

    merged = build_merged_table(spark)
    merged.write.mode("overwrite").parquet(config.MERGED_TABLE_PARQUET)

    merged_reloaded = spark.read.parquet(config.MERGED_TABLE_PARQUET)
    features_df = build_features(spark, merged_reloaded)
    # 증분 실행이 date 파티션 단위 overwrite로 과거 구간을 사후 보정하므로(아래
    # _run_incremental 참고), 전체 빌드도 처음부터 같은 파티션 레이아웃으로 써야 한다.
    features_df.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)

    max_hour_ts = features_df.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(config.WATERMARK_PATH, max_hour_ts.isoformat(), _current_params())
    print(f"[{config.PARAM_COMBO_ID}] 전체 빌드 완료 -> {config.FEATURES_TABLE_PARQUET} (워터마크={max_hour_ts})")


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
    docstring 참고 — 요약하면 대여이력은 반납 완료 시에만 Silver에 나타나므로, 이미
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

    # _refresh_primary_tables()는 위 since_str(증분 워터마크 기준, 보통 최근 며칠~몇 주)이
    # 아니라 학습기간 롤링 윈도우 시작점(config.WINDOW_START)을 쓴다 — station_status/
    # weather/population/targets는 그 자체가 학습에 쓰이는 전체 구간을 커버해야 하고,
    # 증분 워터마크보다 훨씬 이전 데이터도 필요하기 때문이다.
    window_since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
    _refresh_primary_tables(spark, since=window_since)

    # rolling_rental_features는 매번 챔피언 경로에 영구 저장하지 않는다 — lookback
    # 구간(기본 35일)만 있으면 항상 다시 계산할 수 있을 만큼 가벼워서(창 폭이 최대
    # 90분), 매 증분마다 저장소를 늘리기보다 build_features가 읽을 임시 parquet으로만
    # 써둔다.
    rolling_tmp_path = f"{config.OUTPUT_ROOT}/_rolling_incremental_tmp.parquet"
    build_rolling_rental_features(spark, output_path=rolling_tmp_path, since=since_str)

    merged_increment = build_merged_table(spark, since=since_str)
    features_increment = build_features(spark, merged_increment, rolling_parquet_path=rolling_tmp_path)

    if features_increment.limit(1).count() == 0:
        print(f"[{config.PARAM_COMBO_ID}] 재계산 구간({since_str}~)에 데이터 없음 — 건너뜀")
        return

    new_count = features_increment.filter(F.col("hour_ts") > F.lit(watermark["max_hour_ts"])).count()

    # append가 아니라 재계산 구간에 걸리는 날짜 파티션을 통째로 교체한다 — dynamic
    # partition overwrite는 이 DataFrame에 실제로 등장하는 날짜 파티션만 건드리고
    # 나머지(lookback 밖의 과거 데이터)는 그대로 둔다.
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    features_increment.write.mode("overwrite").partitionBy("date").parquet(config.FEATURES_TABLE_PARQUET)

    max_hour_ts = features_increment.agg(F.max("hour_ts")).collect()[0][0]
    write_watermark(config.WATERMARK_PATH, max_hour_ts.isoformat(), _current_params())
    print(f"[{config.PARAM_COMBO_ID}] {since_str}~{max_hour_ts} 재계산(신규 {new_count:,}행 포함) -> "
          f"{config.FEATURES_TABLE_PARQUET} 날짜 파티션 덮어씀 (워터마크 갱신={max_hour_ts})")


def main() -> None:
    """피처마트 파이프라인을 실행한다 (워터마크 유무에 따라 전체 빌드 또는 증분 실행)."""
    spark = get_spark()
    watermark = read_watermark(config.WATERMARK_PATH)
    if watermark is None:
        _run_full_build(spark)
    else:
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
