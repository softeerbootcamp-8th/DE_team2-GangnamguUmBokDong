"""tick 단위 feature 테이블을 horizon=1..HORIZON_COUNT 학습 테이블로 확장하는 모듈 (PySpark).

단일 모델 다중 시계열 확장을 위해 앵커 시점(T0)의 lag 정보와 미래 목표 시점(T0 + horizon - 1)의 컨텍스트 피처(날씨, 인구 등) 및 타겟을 결합한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from ml_core import common_config
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config

# BASE_FEATURE_COLUMNS에서 horizon을 제외한 공통 피처 컬럼 및 메타데이터 컬럼 정의
_COMMON_TARGET_COLUMNS = [
    *(c for c in common_config.BASE_FEATURE_COLUMNS if c != "horizon"),
    "hour_ts",
    "hour",
    "date",
]

RENTAL_ANCHOR_COLUMNS = ["station_no", "hour_ts", "rental_lag_1h"]
RENTAL_TARGET_COLUMNS = [*_COMMON_TARGET_COLUMNS, "rental_exposure", "rental_count"]
RETURN_ANCHOR_COLUMNS = ["station_no", "hour_ts", "return_lag_1h"]
RETURN_TARGET_COLUMNS = [*_COMMON_TARGET_COLUMNS, "return_count"]



def _shift_for_horizon(anchor: DataFrame, target: DataFrame, horizon: int) -> DataFrame:
    """anchor(각 행이 T0)에 target_ts=T0+(horizon-1)시간의 컬럼들을 붙이고 horizon을 단다.

    `build_features._exact_hour_lag()`와 정확히 같은 self-join 기법(정확히 그 시각의 행이
    없으면 자동으로 빠짐)을 반대 방향(과거가 아니라 미래 조회)으로 쓴다.

    args:
        anchor: station_no, hour_ts=T0, lag 컬럼 1개만 담은 DataFrame
        target: station_no, hour_ts, 날씨/캘린더/타겟/date를 담은 DataFrame
        horizon: 1~HORIZON_COUNT
    returns:
        DataFrame: station_no, anchor_ts(T0), horizon, lag 1개, target 컬럼 중
            station_no/hour_ts를 뺀 나머지(target_ts 기준)
    """
    offset_hours = horizon - 1
    shifted_target = (
        target.withColumnRenamed("station_no", "_tgt_station_no")
        .withColumn("_anchor_hour_ts", F.col("hour_ts") - F.expr(f"INTERVAL {offset_hours} HOURS"))
        .drop("hour_ts")
    )
    joined = anchor.join(
        shifted_target,
        (anchor["station_no"] == shifted_target["_tgt_station_no"])
        & (anchor["hour_ts"] == shifted_target["_anchor_hour_ts"]),
        "inner",
    )
    joined = joined.withColumnRenamed("hour_ts", "anchor_ts").withColumn("horizon", F.lit(horizon).cast("tinyint"))
    return joined.drop("_tgt_station_no", "_anchor_hour_ts")


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
        DataFrame: station_no, anchor_ts, horizon, lag 컬럼 1개(anchor_ts 기준),
            날씨/인구/캘린더/(대여면 rental_exposure)/타겟 카운트/date(target_ts 기준)를
            horizon 1..HORIZON_COUNT만큼 union한 것 — station_id(텍스트)는 담지 않는다
            (모듈 docstring의 station_no 전환 이유 참고)
    """
    del spark  # 시그니처 대칭용 — 이 함수 자체는 SparkSession을 직접 쓰지 않음
    anchor_source = anchor_df if anchor_df is not None else features_df
    # HORIZON_COUNT(기본 12)번의 self-join이 전부 같은 anchor/target lineage를
    # 공유한다 — 캐싱이 없으면 Spark가 join마다 독립적으로 재분석·재실행할 수 있어
    # 매 horizon마다 같은 원본을 다시 훑는 중복 비용이 붙는다(2026-08-27 Spark UI
    # 실측: horizon 루프 구간에서 같은 크기의 parquet read job이 반복되며 소요
    # 시간이 계속 늘어남). 두 DataFrame 다 작으므로(tick 단위 feature 테이블) 캐싱
    # 비용은 무시할 만하다.
    anchor = anchor_source.select(*anchor_columns).cache()
    target = features_df.select(*target_columns).cache()

    horizon_frames = [_shift_for_horizon(anchor, target, h) for h in range(1, config.HORIZON_COUNT + 1)]
    return _balanced_union_by_name(horizon_frames)


def _balanced_union_by_name(frames: list[DataFrame]) -> DataFrame:
    """`frames`를 순차(left-deep) fold 대신 균형 이진트리로 union한다.

    `combined = combined.unionByName(frame)`을 반복하면 union 깊이가 프레임
    개수만큼(n=HORIZON_COUNT) 길어져, 이후 액션마다 Catalyst가 매번 그 전체 누적
    계획을 다시 분석해야 한다(2026-08-27 Spark UI 실측: 같은 크기의 작업인데도
    horizon이 늘어날수록 job 소요 시간이 계속 증가). 균형 트리로 묶으면 깊이가
    `log2(n)`으로 줄어 재분석 비용이 크게 준다 — 최종 union 결과(row 집합)는
    fold 방식과 동일하다.
    """
    if not frames:
        raise ValueError("union할 frame이 최소 1개 이상 필요합니다.")
    if len(frames) == 1:
        return frames[0]
    mid = len(frames) // 2
    left = _balanced_union_by_name(frames[:mid])
    right = _balanced_union_by_name(frames[mid:])
    return left.unionByName(right)


def _anchor_input(features: DataFrame) -> DataFrame | None:
    """학습 계약과 선택적 날짜 범위에 맞는 anchor 후보 DataFrame을 만든다.

    실제 밀도는 effective profile의 `TRAIN_ANCHOR_TICK_MINUTES`가 단일 소스다.
    이전에 사용하던 `MULTI_HORIZON_ANCHOR_TICK_MINUTES`와
    `MULTI_HORIZON_ANCHOR_HOURLY_ONLY`는 common_config가 검증된 환경변수 별칭으로
    해석한다. 날짜 since/until은 로컬 디버깅용으로 유지한다. target 쪽은 항상
    features 전체를 써서 anchor 끝 근처 horizon이 범위를 벗어나지 않게 한다.
    """
    import os

    anchor_since = os.environ.get("MULTI_HORIZON_ANCHOR_SINCE")
    anchor_until = os.environ.get("MULTI_HORIZON_ANCHOR_UNTIL")
    # anchor를 tick 전체로 유지하면 이 self-join 결과는 원본의 최대 HORIZON_COUNT배
    # 행 수가 된다 — 실측(2025-11 한 달, station ~2,977개, 5분 tick 시절)으로 2.6억
    # 행이 나와 로컬 학습 머신(RAM 18GB)에서 판다스로 못 읽었다(pd.read_parquet가
    # 출력도 없이 OOM kill됨). EMR처럼 그 규모를 받아낼 수 있는 환경이 아니면(로컬
    # 1회성 검증 등) 이 옵션으로 anchor를 성기게 줄인다. target_ts/타겟 라벨은
    # 원본 grid 값을 유지하지만, 학습에서 보게 되는 minute 분포 자체는 달라진다.
    # 따라서 g5/a20 같은 thinning의 5분 추론 성능은 공통 g5 test mart에서 a5와
    # 별도로 비교해야 하며 코드 동작만으로 동등하다고 간주하지 않는다.
    anchor_tick_minutes = config.TRAIN_ANCHOR_TICK_MINUTES
    if not (anchor_since or anchor_until) and anchor_tick_minutes == config.GRID_TICK_MINUTES:
        return None

    anchor_input = features
    if anchor_since:
        anchor_input = anchor_input.filter(F.col("hour_ts") >= F.lit(anchor_since))
    if anchor_until:
        anchor_input = anchor_input.filter(F.col("hour_ts") < F.lit(anchor_until))
    if anchor_tick_minutes != config.GRID_TICK_MINUTES:
        minute_of_day = F.hour(F.col("hour_ts")) * 60 + F.minute(F.col("hour_ts"))
        anchor_input = anchor_input.filter(minute_of_day % anchor_tick_minutes == 0)
    return anchor_input


def _features_in_training_window(features: DataFrame) -> DataFrame:
    """tick feature를 config의 inclusive 날짜 window 안으로 제한한다.

    `run_pipeline`이 같은 경계를 적용하지만, 과거 rolling 실행의 파티션이나 수동
    생성 파일이 입력 경로에 남아 있어도 최종 multi-horizon 학습 테이블에 섞이지
    않게 소비 지점에서 한 번 더 방어한다.
    """
    since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
    complete_through = (
        datetime.combine(config.WINDOW_END + timedelta(days=1), datetime.min.time())
        - timedelta(minutes=config.TARGET_HORIZON_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    return features.filter(
        (F.col("hour_ts") >= F.lit(since))
        & (F.col("hour_ts") <= F.lit(complete_through))
    )


def _write_date_partitioned(features: DataFrame, output_path: str) -> None:
    """날짜 하나가 writer task 하나에만 가도록 모은 뒤 Parquet을 저장한다.

    ``partitionBy("date")``만 호출하면 입력의 모든 Spark partition이 각 날짜
    디렉터리에 파일을 하나씩 쓴다. 2025년 전체 multi-horizon 실측에서는 대여
    mart 하나에 45,341개 파일이 생겨 S3 listing/GET 비용과 학습 로더 지연이
    커졌다. 먼저 date로 hash repartition하면 같은 날짜는 정확히 한 task에만
    들어가므로 날짜당 data file 하나를 유지하면서 논리 partition 계약은 같다.

    args:
        features: date 컬럼이 있는 multi-horizon DataFrame.
        output_path: overwrite할 Parquet 경로.
    raises:
        ValueError: date 컬럼이 없을 때.
    """
    if "date" not in features.columns:
        raise ValueError("multi-horizon 출력에는 date 컬럼이 필요합니다")
    # date로 repartition 후 파티션 내부를 정렬하여 Parquet RLE/Snappy 압축률과 읽기 I/O를 최적화한다.
    preferred_sort_cols = ["date", "anchor_ts", "station_no", "horizon"]
    sort_cols = [c for c in preferred_sort_cols if c in features.columns]
    writer = features.repartition("date")
    if sort_cols:
        writer = writer.sortWithinPartitions(*sort_cols)
    writer.write.mode("overwrite").partitionBy("date").parquet(output_path)


def _multi_horizon_marts_are_fresh(
    source_watermark: dict, existing_marker: dict | None, window_since: str, window_until: str
) -> bool:
    """이 profile의 마지막 multi-horizon 생성이 지금 소스 watermark/학습 윈도우와
    동일한 상태에서 이뤄졌으면 True(=재생성 불필요).

    args:
        source_watermark: `run_pipeline.py`가 남긴 현재 `WATERMARK_PATH` 내용
            (`watermark.read_watermark()` 결과, `max_hour_ts` 포함)
        existing_marker: 이전 multi-horizon 생성이 남긴 워터마크
            내용(없으면 None — 이 profile로 처음 만드는 것이라 항상 False)
        window_since, window_until: 지금 이 실행이 쓰는 학습 윈도우 경계
            (`config.WINDOW_START`/`WINDOW_END`를 문자열로 바꾼 것)
    """
    if existing_marker is None:
        return False
    marker_params = existing_marker.get("params") or {}
    return (
        existing_marker.get("max_hour_ts") == source_watermark.get("max_hour_ts")
        and marker_params.get("window_since") == window_since
        and marker_params.get("window_until") == window_until
    )


def _run_cli() -> None:
    import argparse
    from .watermark import is_fresh, multi_horizon_watermark_path, read_watermark, write_watermark

    parser = argparse.ArgumentParser(description="Multi-horizon feature table builder")
    parser.add_argument(
        "--models",
        default="rental,return",
        help="생성할 모델 대상 (rental, return, 또는 rental,return. 기본값 rental,return)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="워터마크 신선도 검사를 무시하고 강제로 재생성",
    )
    args, _ = parser.parse_known_args()

    from .spark_session import get_spark

    spark = get_spark()
    requested_models = [m.strip() for m in args.models.split(",") if m.strip()]
    if not requested_models or "all" in requested_models:
        requested_models = ["rental", "return"]

    window_since_str = str(config.WINDOW_START)
    window_until_str = str(config.WINDOW_END)
    source_watermark = read_watermark(config.WATERMARK_PATH)
    if source_watermark is None:
        raise RuntimeError(
            f"{config.WATERMARK_PATH}에 watermark가 없습니다 — run_pipeline.py가 이 "
            "profile의 base feature 테이블을 먼저 만들어야 합니다."
        )

    # 요청된 모델 중 실제로 생성이 필요한 모델만 선별
    models_to_build: list[str] = []
    for model in requested_models:
        if args.force:
            models_to_build.append(model)
            continue
        model_wm_path = multi_horizon_watermark_path(model)
        existing_marker = read_watermark(model_wm_path)
        # 1차: 동일 max_hour_ts + 동일 윈도우 검사
        # 2차: 2단계 워터마크가 없는 경우 레거시 공통 워터마크 검사
        if existing_marker is None:
            existing_marker = read_watermark(config.MULTI_HORIZON_WATERMARK_PATH)

        if _multi_horizon_marts_are_fresh(source_watermark, existing_marker, window_since_str, window_until_str):
            print(
                f"[build_multi_horizon_features] [{model}] {config.PARAM_COMBO_ID}/"
                f"a{config.TRAIN_ANCHOR_TICK_MINUTES}: 소스 watermark"
                f"({source_watermark['max_hour_ts']})와 학습 윈도우가 최신 — 재생성 건너뜀"
            )
        else:
            models_to_build.append(model)

    if not models_to_build:
        print("[build_multi_horizon_features] 모든 요청 모델의 Multi-horizon 피처가 최신 상태입니다 — 종료")
        spark.stop()
        return

    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    features = _features_in_training_window(
        spark.read.parquet(config.FEATURES_TABLE_PARQUET)
    )

    anchor_input = _anchor_input(features)

    for i, model in enumerate(models_to_build):
        if i > 0:
            spark.catalog.clearCache()

        if model == "rental":
            rental_result = build_multi_horizon_features(
                spark, features, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS, anchor_df=anchor_input
            )
            _write_date_partitioned(
                rental_result,
                config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
            )
            print(f"rental multi-horizon features -> {config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET}")
            write_watermark(
                multi_horizon_watermark_path("rental"),
                source_watermark["max_hour_ts"],
                {"window_since": window_since_str, "window_until": window_until_str},
            )
        elif model == "return":
            return_result = build_multi_horizon_features(
                spark, features, RETURN_ANCHOR_COLUMNS, RETURN_TARGET_COLUMNS, anchor_df=anchor_input
            )
            _write_date_partitioned(
                return_result,
                config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET,
            )
            print(f"return multi-horizon features -> {config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET}")
            write_watermark(
                multi_horizon_watermark_path("return"),
                source_watermark["max_hour_ts"],
                {"window_since": window_since_str, "window_until": window_until_str},
            )

    # 레거시 호환용 공통 워터마크 갱신
    write_watermark(
        config.MULTI_HORIZON_WATERMARK_PATH,
        source_watermark["max_hour_ts"],
        {"window_since": window_since_str, "window_until": window_until_str},
    )


if __name__ == "__main__":
    try:
        _run_cli()
    except Exception as exc:
        print(f"[build_multi_horizon_features] 실패: {exc}", flush=True)
        raise

