"""Silver `bike_rental_history` -> "T로부터 앞으로 1시간 동안 일어날 대여/반납 건수" 타겟 생성 (PySpark).

트립 원본 로딩·station_id 매칭은 `silver_source.read_rental_trips()`가 담당한다
(예전엔 원본 historical CSV parquet을 station_no로 크로스워크했지만, 실제 Silver
`bike_rental_history`는 `RENT_STATION_ID`/`RETURN_STATION_ID`가 이미 station_id
형식이라 그 크로스워크가 필요 없다 — `docs/collector/ml-integration-requests.md` 1번).
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
    """Silver 대여이력을 읽어 "[T,T+1시간)에 일어날 대여/반납 건수" 타겟을 계산한다.

    args:
        spark: SparkSession
        since: 지정하면 각 대여/반납 event가 이 값 이상인 타겟만 만든다.
        until: 지정하면 각 event와 결과 tick이 이 값보다 이른 타겟만 만든다.
            학습 종료일 다음날 00:00을 넘기는 행을 막는 exclusive upper bound다.
    returns:
        tuple[DataFrame, DataFrame]: (rental_targets, return_targets) — 각각
            station_id, tick, count (sparse step function)
    """
    # 반납은 학습 구간 전에 출발한 트립도 구간 안에서 끝날 수 있다. 그래서 source를
    # start_dt lower bound로 먼저 자르지 않고, 대여/반납 event를 아래에서 각자의 시각
    # 컬럼으로 필터링한다. upper bound는 start_dt 기준으로 source 양을 줄여도 안전하다
    # (start_dt는 end_dt보다 늦을 수 없으므로, 구간 안 반납이 빠지지 않는다).
    trips = read_rental_trips(spark, until=until)

    rental_trips = trips.select("station_id", "start_dt")
    return_trips = (
        trips.filter(F.col("end_dt").isNotNull() & F.col("end_station_id").isNotNull())
        .select(F.col("end_station_id").alias("station_id"), "end_dt")
    )
    if since is not None:
        rental_trips = rental_trips.filter(F.col("start_dt") >= F.lit(since))
        return_trips = return_trips.filter(F.col("end_dt") >= F.lit(since))
    if until is not None:
        rental_trips = rental_trips.filter(F.col("start_dt") < F.lit(until))
        return_trips = return_trips.filter(F.col("end_dt") < F.lit(until))

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
    if until is not None:
        # `[T, T+TARGET_HORIZON)` 라벨 전체가 source window 안에 들어오는 마지막
        # 기준시각까지만 남긴다. 단순히 tick < until만 적용하면 12/31 23:55 라벨은
        # 1/1 00:00 이후 event가 잘린 값을 정상 60분 라벨처럼 학습시키게 된다.
        # 마지막 완결 tick(기본 23:00)은 포함해야 하므로 <= 경계다. lower bound
        # 이전 delta는 구간 첫 tick의 누적값 계산에 필요해 의도적으로 보존한다.
        complete_through = (
            datetime.fromisoformat(until)
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
