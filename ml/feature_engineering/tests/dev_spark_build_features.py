"""feature_engineering.build_features()의 lag/rolling 정확성 검증.

가장 중요하게 확인하는 것: `build_merged_table.py`의 station 활성 구간 필터링
때문에 그리드에 구멍(결측 시간대)이 생길 수 있는데, 그 상태에서도 lag/rolling이
"행 개수"가 아니라 "실제 경과 시간" 기준으로 정확히 계산되는지 — 이게 깨지면
구멍 근처에서 "24시간 전"이 실제로는 27시간 전 값을 가져오는 조용한 버그가 된다.
"""

import os

import pandas as pd
import pytest

from ml_common.rolling_window_features import censored_rolling_counts

pyspark = pytest.importorskip("pyspark")

from make_dataset.spark import config as fe_config
from make_dataset.spark.build_features import build_features


@pytest.fixture(scope="module")
def spark():
    import sys

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # timestamp_ntz/timestamp(tz-aware) 왕복 어긋남 방지 — feature_engineering/spark_session.py 참고.
    os.environ.setdefault("TZ", "Asia/Seoul")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-feature-engineering-build-features")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "Asia/Seoul")
        .getOrCreate()
    )
    yield session
    session.stop()


def _write_rolling_parquet(tmp_path, trips: list[dict]) -> str:
    if not trips:
        # censored_rolling_counts가 빈 입력에서도 안전하게 동작하도록, 테스트 시각과 무관한
        # 더미 트립 1건을 넣어준다 (완전히 빈 DataFrame은 dtype 추론 문제가 생길 수 있어 회피).
        trips = [{"station_id": "unused", "start_dt": pd.Timestamp("2000-01-01"), "end_dt": pd.Timestamp("2000-01-01 00:05")}]
    trips_pdf = pd.DataFrame(trips)
    cumulative = censored_rolling_counts(
        trips_pdf,
        window_minutes=fe_config.ROLLING_WINDOW_MINUTES,
        embargo_minutes=fe_config.ROLLING_EMBARGO_MINUTES,
        tick_minutes=fe_config.ROLLING_TICK_MINUTES,
    )
    # pandas 2.x는 tick을 datetime64[ns]로 만드는데, ns 단위 parquet TIMESTAMP는 이 Spark
    # 버전이 못 읽는다("Illegal Parquet type: INT64 (TIMESTAMP(NANOS,...))") — 실제
    # processed_v2 산출물은 이미 us 단위라 문제 없음(펜더스 3.x 기준 기본값 차이일 뿐),
    # 테스트에서만 명시적으로 us로 캐스팅해서 pandas 버전과 무관하게 안전하게 만든다.
    cumulative["tick"] = cumulative["tick"].astype("datetime64[us]")
    path = str(tmp_path / "rolling_rental_features_test.parquet")
    cumulative.to_parquet(path, index=False)
    return path


def _grid(station: str, hours) -> pd.DataFrame:
    hours = pd.to_datetime(hours)
    return pd.DataFrame(
        {
            "station_id": station,
            "hour_ts": hours,
            "rental_count": 0,
            "return_count": [(i % 4) for i in range(len(hours))],
            "stockout_flag": 0,
            "hour": hours.hour,
            "dow": hours.dayofweek,
        }
    )


def test_return_lag_matches_hand_computation_on_dense_grid(spark, tmp_path):
    """구멍 없는 그리드에서는 lag_24h/168h, roll_mean_3h가 손으로 계산한 값과 정확히 같아야 한다."""
    hours = pd.date_range("2025-06-01 00:00", periods=200, freq="h")
    pdf = _grid("A", hours)
    rolling_path = _write_rolling_parquet(tmp_path, [])

    sdf = spark.createDataFrame(pdf)
    out = build_features(spark, sdf, rolling_parquet_path=rolling_path).toPandas().sort_values("hour_ts").reset_index(drop=True)
    # Spark 왕복 후 hour_ts가 datetime64[us]로 바뀌는 pandas 버전이 있어(ns였던 원본과
    # 값은 같지만 unit만 다름), 인덱스 dtype이 아니라 값만 비교하도록 통일한다.
    out["hour_ts"] = out["hour_ts"].astype(pdf["hour_ts"].dtype)

    series = pdf.set_index("hour_ts")["return_count"]
    expected_lag24 = series.shift(24)
    got_lag24 = out.set_index("hour_ts")["return_lag_24h"]
    pd.testing.assert_series_equal(got_lag24, expected_lag24, check_names=False, check_dtype=False)

    expected_roll_mean_3h = series.shift(1).rolling(window=3, min_periods=1).mean()
    got_roll_mean_3h = out.set_index("hour_ts")["return_roll_mean_3h"]
    pd.testing.assert_series_equal(got_roll_mean_3h, expected_roll_mean_3h, check_names=False, check_dtype=False)


def test_lag_and_rolling_are_gap_aware_not_row_count_based(spark, tmp_path):
    """그리드에 구멍(결측 시간대)이 있을 때 lag/rolling이 실제 경과시간 기준으로 동작해야 한다.

    2025-06-05 00:00~05:00(6시간) 구간을 통째로 그리드에서 뺀 station을 만들고:
    - 구멍 바로 다음 행에서 lag_24h는 "정확히 24시간 전 행이 그리드에 있는지"로 판정돼야
      한다(행 개수 기준이면 구멍 때문에 실제로는 30시간 전 값을 잘못 가져오게 됨).
    - roll_mean_3h는 구멍 근처에서 실제로 존재하는 행만 평균에 반영해야 한다(존재하는
      행 개수가 3개 미만이면 그만큼만 평균 내고, 구멍 너머 먼 시점 값이 섞이면 안 됨).
    """
    all_hours = pd.date_range("2025-06-01 00:00", periods=240, freq="h")
    gap_start = pd.Timestamp("2025-06-05 00:00")
    gap_end = pd.Timestamp("2025-06-05 05:00")  # 이 6시간(00~05시)을 그리드에서 제외
    kept_hours = [h for h in all_hours if not (gap_start <= h <= gap_end)]

    pdf = _grid("A", kept_hours)
    rolling_path = _write_rolling_parquet(tmp_path, [])

    sdf = spark.createDataFrame(pdf)
    out = build_features(spark, sdf, rolling_parquet_path=rolling_path).toPandas().set_index("hour_ts").sort_index()

    series = pdf.set_index("hour_ts")["return_count"]

    # 구멍 바로 다음 행(06:00): 24시간 전(전날 06:00)은 그리드에 존재하므로 정상적으로 값이 나와야 함
    row_after_gap = pd.Timestamp("2025-06-05 06:00")
    expected_lag24_after_gap = series.get(row_after_gap - pd.Timedelta(hours=24))
    assert out.loc[row_after_gap, "return_lag_24h"] == expected_lag24_after_gap

    # 구멍 안(예: 만약 04:00이 있었다면)의 24시간 전인 06-04 04:00은 있지만, 구멍 자체는 그리드에 없으므로
    # 그 행 자체가 결과에 없음 -> 대신 "24시간 뒤에 구멍이 있는 행"인 06-04 00:00~05:00 각각에 대해,
    # 그로부터 24시간 뒤(06-05 00:00~05:00, 즉 구멍) 행이 없으므로 그 시각을 24h-lag로 참조하는 행(06-06 00:00~05:00)의
    # lag_24h가 null이어야 한다.
    for h in pd.date_range("2025-06-06 00:00", "2025-06-06 05:00", freq="h"):
        assert pd.isna(out.loc[h, "return_lag_24h"]), f"{h}: 구멍(24시간 전 결측)인데 lag_24h가 채워짐 -> row-count 기반 버그 의심"

    # roll_mean_3h: 구멍 바로 다음 행(06:00)의 "직전 3시간"은 05:00(없음, 구멍),04:00(없음, 구멍),03:00(없음, 구멍)
    # 이므로 표본이 0개 -> NaN이어야 한다(먼 시점 값이 섞여 들어오면 안 됨).
    assert pd.isna(out.loc[row_after_gap, "return_roll_mean_3h"]), "구멍 직후 roll_mean_3h가 채워짐 -> 먼 시점 값이 섞여 들어온 것으로 의심"

    # 07:00의 "직전 3시간"은 06:00(있음, 방금 계산한 행) 하나뿐 -> 그 값만으로 평균 나야 함
    row_2h_after_gap = pd.Timestamp("2025-06-05 07:00")
    expected_single_sample_mean = float(series.get(row_after_gap))
    assert out.loc[row_2h_after_gap, "return_roll_mean_3h"] == pytest.approx(expected_single_sample_mean)
