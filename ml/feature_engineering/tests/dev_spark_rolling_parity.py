"""feature_engineering(PySpark)의 censored_rolling_counts/lookup_count_at_ticks가
src/rolling_window_features.py(pandas, 이미 검증된 기준 구현)와 정확히 같은 결과를
내는지 대조한다.

두 구현은 알고리즘 레벨(차분배열+cumsum vs Window 함수)이 다르기 때문에, 이 대조가
없으면 조용히 갈라져도(=학습에 쓰는 실제 값이 서로 달라져도) 알아챌 방법이 없다.
`tests/dev_rolling_window_features.py`의 합성 트립 fixture를 그대로 재사용해서
같은 입력에 같은 출력이 나오는지 확인한다.
"""

import os

import pandas as pd
import pytest
from ml_common import common_config as pandas_config
from ml_common.rolling_window_features import (
    censored_rolling_counts as pandas_censored_rolling_counts,
)
from ml_common.rolling_window_features import (
    future_rolling_counts as pandas_future_rolling_counts,
)
from ml_common.rolling_window_features import (
    lookup_count_at_ticks as pandas_lookup_count_at_ticks,
)

pyspark = pytest.importorskip("pyspark")

from feature_engineering.spark.rolling_window_features import (
    censored_rolling_counts as spark_censored_rolling_counts,
)
from feature_engineering.spark.rolling_window_features import (
    future_rolling_counts as spark_future_rolling_counts,
)
from feature_engineering.spark.rolling_window_features import (
    lookup_count_at_ticks as spark_lookup_count_at_ticks,
)


@pytest.fixture(scope="module")
def spark():
    import sys

    from pyspark.sql import SparkSession

    # 드라이버/워커(로컬 서브프로세스) Python을 이 프로세스와 고정 — 방치하면 PATH의
    # 다른 python(pyspark가 지원 안 하는 버전일 수 있음)을 워커가 집어서 죽는다.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # JVM 기본 타임존을 세션 타임존(아래 UTC)과 맞춘다 — 안 맞추면 timestamp_ntz
    # (parquet에서 읽은 값)와 timestamp(tz-aware, spark.createDataFrame(pandas_df)로
    # 만든 값)가 unix_timestamp()/timestamp_seconds() 왕복에서 로컬 타임존만큼(이
    # 개발 머신은 KST라 9시간) 조용히 어긋난다 — feature_engineering/spark_session.py
    # 참고, 실제로 이 버그에 걸려서 발견함.
    os.environ.setdefault("TZ", "Asia/Seoul")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-feature-engineering-rolling-parity")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    yield session
    session.stop()


def _trip(station, start, end=None):
    return {
        "station_id": station,
        "start_dt": pd.Timestamp(start),
        "end_dt": pd.Timestamp(end) if end is not None else pd.NaT,
    }


SAMPLE_TRIPS = [
    _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
    _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
    _trip("A", "2025-06-01 09:50:00", "2025-06-01 11:20:00"),
    _trip("A", "2025-06-01 10:40:00", "2025-06-01 10:55:00"),
    _trip("B", "2025-06-01 09:00:00", "2025-06-01 09:02:00"),
    _trip("B", "2025-06-01 09:58:00", None),  # 반납 안 됨 -> 영원히 안 보임
    _trip("B", "2025-06-01 09:30:00", "2025-06-01 12:00:00"),  # embargo+width(90분)보다 늦게 반납 -> 제외
]


def _spark_trips_df(spark, trips: list[dict]):
    pdf = pd.DataFrame(trips)
    return spark.createDataFrame(pdf)


def _sorted_records(df: pd.DataFrame) -> list[tuple]:
    return sorted(df.itertuples(index=False, name=None))


@pytest.mark.parametrize(
    "window_minutes,embargo_minutes,tick_minutes",
    [
        (60, 30, 5),  # 채택된 챔피언 설계
        (60, 0, 5),  # 임바고 스윕 후보
        (5, 0, 5),  # "버킷이 닫히는 순간" 특수 케이스
    ],
)
def test_censored_rolling_counts_matches_pandas(spark, window_minutes, embargo_minutes, tick_minutes):
    trips_pdf = pd.DataFrame(SAMPLE_TRIPS)
    pandas_result = pandas_censored_rolling_counts(
        trips_pdf, window_minutes=window_minutes, embargo_minutes=embargo_minutes, tick_minutes=tick_minutes
    )
    pandas_result["tick"] = pandas_result["tick"].astype("datetime64[us]")

    spark_trips = _spark_trips_df(spark, SAMPLE_TRIPS)
    spark_result = spark_censored_rolling_counts(
        spark_trips, window_minutes=window_minutes, embargo_minutes=embargo_minutes, tick_minutes=tick_minutes
    ).toPandas()
    spark_result["tick"] = spark_result["tick"].astype("datetime64[us]")
    spark_result["count"] = spark_result["count"].astype("int32")

    assert _sorted_records(pandas_result) == _sorted_records(spark_result)


def test_lookup_count_at_ticks_matches_pandas(spark):
    trips_pdf = pd.DataFrame(SAMPLE_TRIPS)
    window_minutes, embargo_minutes, tick_minutes = (
        pandas_config.ROLLING_WINDOW_MINUTES,
        pandas_config.ROLLING_EMBARGO_MINUTES,
        pandas_config.ROLLING_TICK_MINUTES,
    )

    pandas_cumulative = pandas_censored_rolling_counts(
        trips_pdf, window_minutes=window_minutes, embargo_minutes=embargo_minutes, tick_minutes=tick_minutes
    )
    spark_trips = _spark_trips_df(spark, SAMPLE_TRIPS)
    spark_cumulative = spark_censored_rolling_counts(
        spark_trips, window_minutes=window_minutes, embargo_minutes=embargo_minutes, tick_minutes=tick_minutes
    )

    query_hours = pd.date_range("2025-06-01 08:00", periods=8, freq="h")
    query_pdf = pd.DataFrame(
        {"station_id": ["A"] * len(query_hours) + ["B"] * len(query_hours), "tick": list(query_hours) * 2}
    )

    pandas_looked_up = pandas_lookup_count_at_ticks(pandas_cumulative, query_pdf)
    pandas_out = query_pdf.copy()
    pandas_out["count"] = pandas_looked_up.to_numpy()

    spark_query = spark.createDataFrame(query_pdf)
    spark_looked_up = spark_lookup_count_at_ticks(spark_cumulative, spark_query).toPandas()
    spark_looked_up["tick"] = spark_looked_up["tick"].astype("datetime64[us]")

    pandas_out["tick"] = pandas_out["tick"].astype("datetime64[us]")
    pandas_sorted = pandas_out.sort_values(["station_id", "tick"]).reset_index(drop=True)
    spark_sorted = spark_looked_up.sort_values(["station_id", "tick"]).reset_index(drop=True)

    pd.testing.assert_frame_equal(
        pandas_sorted[["station_id", "tick", "count"]],
        spark_sorted[["station_id", "tick", "count"]].astype({"count": pandas_sorted["count"].dtype}),
    )


@pytest.mark.parametrize(
    "width_minutes,tick_minutes,event_col",
    [
        (60, 5, "start_dt"),  # 대여 타겟(챔피언 설계)
        (60, 5, "end_dt"),  # 반납 타겟 — end_dt가 NaT인 트립은 pandas/spark 양쪽에서 걸러져야 함
        (5, 5, "start_dt"),  # "버킷이 닫히는 순간" 특수 케이스
    ],
)
def test_future_rolling_counts_matches_pandas(spark, width_minutes, tick_minutes, event_col):
    trips_for_targets = [t for t in SAMPLE_TRIPS if not (event_col == "end_dt" and pd.isna(t["end_dt"]))]
    trips_pdf = pd.DataFrame(trips_for_targets)

    pandas_result = pandas_future_rolling_counts(
        trips_pdf, width_minutes=width_minutes, tick_minutes=tick_minutes, event_col=event_col
    )
    pandas_result["tick"] = pandas_result["tick"].astype("datetime64[us]")

    spark_trips = _spark_trips_df(spark, trips_for_targets)
    spark_result = spark_future_rolling_counts(
        spark_trips, width_minutes=width_minutes, tick_minutes=tick_minutes, event_col=event_col
    ).toPandas()
    spark_result["tick"] = spark_result["tick"].astype("datetime64[us]")
    spark_result["count"] = spark_result["count"].astype("int32")

    assert _sorted_records(pandas_result) == _sorted_records(spark_result)


def test_lookup_count_at_ticks_unknown_station_defaults_to_zero(spark):
    """cumulative에 전혀 없는 station을 조회하면 0을 반환해야 한다 (pandas와 동일)."""
    cumulative_pdf = pd.DataFrame({"station_id": ["A"], "tick": [pd.Timestamp("2025-06-01 09:00")], "count": [3]})
    query_pdf = pd.DataFrame({"station_id": ["Z"], "tick": [pd.Timestamp("2025-06-01 09:00")]})

    spark_cumulative = spark.createDataFrame(cumulative_pdf)
    spark_query = spark.createDataFrame(query_pdf)
    result = spark_lookup_count_at_ticks(spark_cumulative, spark_query).toPandas()

    assert result["count"].tolist() == [0]
