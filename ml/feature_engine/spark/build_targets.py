"""Archive `bike_rental_history`에서 대여·반납 미래 1시간 타겟을 만든다.

트립 원본 로딩·station_id 매칭은 `silver_source.read_rental_trips()`가 담당한다.
Archive의 `RENT_STATION_ID`/`RETURN_STATION_ID`가 이미 station_id 형식이라 별도
station_no 크로스워크는 필요 없다.
이 파일은 그 결과에 `rolling_window_features.future_rolling_counts()`로 sparse
step function 타겟만 계산한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from . import config
from .rolling_window_features import future_rolling_counts
from .silver_source import read_rental_trips


def build_targets(
    spark: SparkSession,
    since: str | None = None,
    until: str | None = None,
) -> tuple[DataFrame, DataFrame]:
    """Archive 대여이력을 읽어 "[T,T+1시간)에 일어날 대여/반납 건수" 타겟을 계산한다.

    args:
        spark: SparkSession
        since: 지정하면 각 대여/반납 event가 이 값 이상인 타겟만 만든다.
        until: 지정하면 각 event와 결과 tick이 이 값보다 이른 타겟만 만든다.
            학습 종료일 다음날 00:00을 넘기는 행을 막는 exclusive upper bound다.
    returns:
        tuple[DataFrame, DataFrame]: (rental_targets, return_targets) — 각각
            station_id, tick, count (sparse step function)
    """
    if since is None and until is None:
        target_since = config.WINDOW_START.strftime("%Y-%m-%d 00:00:00")
        target_until = (config.WINDOW_END + timedelta(days=1)).strftime("%Y-%m-%d 00:00:00")
    elif since is None or until is None:
        raise ValueError("target Archive 범위는 since와 until을 함께 받아야 합니다")
    else:
        target_since = since
        target_until = until

    # Bootstrap archive는 RENT_DT 날짜, compaction archive는 수집 날짜다. 따라서
    # target window와 같은 partition만 읽으면 (a) 시작은 경계 전이지만 반납은 window
    # 안인 트립, (b) 시작은 window 안이지만 반납 완료 뒤 future partition에 나타난
    # 트립을 잃는다. 앞쪽은 증분 재계산 lookback, 뒤쪽은 학습 safety margin만큼
    # 명시적으로 확장하되, 아래 rental/return event는 원래 target 경계로 다시 자른다.
    partition_since = (
        datetime.fromisoformat(target_since)
        - timedelta(hours=config.INCREMENTAL_LOOKBACK_HOURS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    partition_until = (
        datetime.fromisoformat(target_until)
        + timedelta(days=config.TRAINING_SAFETY_MARGIN_DAYS)
    ).strftime("%Y-%m-%d %H:%M:%S")
    trips = read_rental_trips(
        spark,
        since=partition_since,
        until=target_until,
        partition_since=partition_since,
        partition_until=partition_until,
    )

    rental_trips = trips.select("station_id", "start_dt")
    return_trips = (
        trips.filter(F.col("end_dt").isNotNull() & F.col("end_station_id").isNotNull())
        .select(F.col("end_station_id").alias("station_id"), "end_dt")
    )
    rental_trips = rental_trips.filter(
        (F.col("start_dt") >= F.lit(target_since))
        & (F.col("start_dt") < F.lit(target_until))
    )
    return_trips = return_trips.filter(
        (F.col("end_dt") >= F.lit(target_since))
        & (F.col("end_dt") < F.lit(target_until))
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
    # `[T, T+TARGET_HORIZON)` 라벨 전체가 source window 안에 들어오는 마지막
    # 기준시각까지만 남긴다. 단순히 tick < until만 적용하면 12/31 23:55 라벨은
    # 1/1 00:00 이후 event가 잘린 값을 정상 60분 라벨처럼 학습시키게 된다.
    # 마지막 완결 tick(기본 23:00)은 포함해야 하므로 <= 경계다. lower bound
    # 이전 delta는 구간 첫 tick의 누적값 계산에 필요해 의도적으로 보존한다.
    complete_through = (
        datetime.fromisoformat(target_until)
        - timedelta(minutes=config.TARGET_HORIZON_MINUTES)
    ).strftime("%Y-%m-%d %H:%M:%S")
    rental_targets = rental_targets.filter(F.col("tick") <= F.lit(complete_through))
    return_targets = return_targets.filter(F.col("tick") <= F.lit(complete_through))
    return rental_targets, return_targets


if __name__ == "__main__":
    from .spark_session import get_spark

    spark = get_spark()
    rental_targets, return_targets = build_targets(spark)
    rental_targets.write.mode("overwrite").parquet(config.TARGETS_PARQUET)
    return_targets.write.mode("overwrite").parquet(config.RETURN_TARGETS_PARQUET)
    print(f"rental targets -> {config.TARGETS_PARQUET}")
    print(f"return targets -> {config.RETURN_TARGETS_PARQUET}")
