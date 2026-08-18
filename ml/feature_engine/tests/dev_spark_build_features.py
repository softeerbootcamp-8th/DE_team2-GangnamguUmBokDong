"""feature_engine.build_features()의 return_lag_1h/rental_lag_1h 정확성 검증.

가장 중요하게 확인하는 것: `build_merged_table.py`의 station 활성 구간 필터링
때문에 그리드에 구멍(결측 시간대)이 생길 수 있는데, 그 상태에서도 return_lag_1h가
self-join(`_exact_hour_lag`) 기반이라 "정확히 1시간 전 행이 그리드에 있는지"로
정확히 판정되는지 — 행 개수(위치) 기준이었다면 구멍 근처에서 엉뚱한 시점 값을
가져오는 조용한 버그가 됐을 것이다.
"""

import pandas as pd
import pytest
from ml_core.rolling_window_features import censored_rolling_counts

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_features import build_features


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


def _write_station_master(tmp_path) -> str:
    """`_add_rental_lag_1h()`가 rolling 테이블(station_id 원본)을 station_no로 바꾸려고
    참조하는 작은 크로스워크 — 이 테스트들은 rental_lag_1h 값 자체를 확인하지 않으므로
    (`_write_rolling_parquet`의 더미 트립과 실제로 매칭될 필요가 없다) 파일이
    존재하기만 하면 된다."""
    path = tmp_path / "station_master_test.parquet"
    pd.DataFrame([{"station_id": "unused", "station_no": 999}]).to_parquet(path, index=False)
    return str(path)


def _grid(station_no: int, hours) -> pd.DataFrame:
    hours = pd.to_datetime(hours)
    return pd.DataFrame(
        {
            "station_no": station_no,
            "hour_ts": hours,
            "rental_count": 0,
            "return_count": [(i % 4) for i in range(len(hours))],
            "stockout_flag": 0,
            "hour": hours.hour,
            "dow": hours.dayofweek,
        }
    )


def test_return_lag_1h_matches_hand_computation_on_dense_grid(spark, tmp_path, monkeypatch):
    """구멍 없는 그리드에서는 return_lag_1h가 손으로 계산한 shift(1)과 정확히 같아야 한다."""
    monkeypatch.setattr(fe_config, "STATION_MASTER_PARQUET", _write_station_master(tmp_path))
    hours = pd.date_range("2025-06-01 00:00", periods=200, freq="h")
    pdf = _grid(1, hours)
    rolling_path = _write_rolling_parquet(tmp_path, [])

    sdf = spark.createDataFrame(pdf)
    out = build_features(spark, sdf, rolling_parquet_path=rolling_path).toPandas().sort_values("hour_ts").reset_index(drop=True)
    # Spark 왕복 후 hour_ts가 datetime64[us]로 바뀌는 pandas 버전이 있어(ns였던 원본과
    # 값은 같지만 unit만 다름), 인덱스 dtype이 아니라 값만 비교하도록 통일한다.
    out["hour_ts"] = out["hour_ts"].astype(pdf["hour_ts"].dtype)

    series = pdf.set_index("hour_ts")["return_count"]
    expected_lag1 = series.shift(1)
    got_lag1 = out.set_index("hour_ts")["return_lag_1h"]
    pd.testing.assert_series_equal(got_lag1, expected_lag1, check_names=False, check_dtype=False)


def test_return_lag_1h_is_gap_aware_not_row_count_based(spark, tmp_path, monkeypatch):
    """그리드에 구멍(결측 시간대)이 있을 때 return_lag_1h가 실제 경과시간 기준으로 동작해야 한다.

    2025-06-05 00:00~05:00(6시간) 구간을 통째로 그리드에서 뺀 station을 만들고:
    - 구멍 바로 다음 행(06:00)의 lag_1h는 "정확히 1시간 전(05:00) 행이 그리드에 있는지"로
      판정돼야 한다 — 05:00은 구멍 안이라 없으므로 NaN이어야 한다(행 개수 기준이면
      구멍 바로 앞의 실제 존재하는 행 값을 잘못 가져오게 됨).
    - 그 다음 행(07:00)의 1시간 전(06:00)은 그리드에 있으므로 정상적으로 값이 나와야 한다.
    """
    monkeypatch.setattr(fe_config, "STATION_MASTER_PARQUET", _write_station_master(tmp_path))
    all_hours = pd.date_range("2025-06-01 00:00", periods=240, freq="h")
    gap_start = pd.Timestamp("2025-06-05 00:00")
    gap_end = pd.Timestamp("2025-06-05 05:00")  # 이 6시간(00~05시)을 그리드에서 제외
    kept_hours = [h for h in all_hours if not (gap_start <= h <= gap_end)]

    pdf = _grid(1, kept_hours)
    rolling_path = _write_rolling_parquet(tmp_path, [])

    sdf = spark.createDataFrame(pdf)
    out = build_features(spark, sdf, rolling_parquet_path=rolling_path).toPandas().set_index("hour_ts").sort_index()

    series = pdf.set_index("hour_ts")["return_count"]

    # 구멍 바로 다음 행(06:00): 1시간 전(05:00)은 구멍 안이라 그리드에 없음 -> NaN이어야 함
    row_after_gap = pd.Timestamp("2025-06-05 06:00")
    assert pd.isna(out.loc[row_after_gap, "return_lag_1h"]), (
        "구멍 직후 return_lag_1h가 채워짐 -> row-count 기반 버그 의심"
    )

    # 07:00의 1시간 전(06:00)은 그리드에 있으므로 정상적으로 그 값이 나와야 한다.
    row_2h_after_gap = pd.Timestamp("2025-06-05 07:00")
    expected = float(series.get(row_after_gap))
    assert out.loc[row_2h_after_gap, "return_lag_1h"] == pytest.approx(expected)
