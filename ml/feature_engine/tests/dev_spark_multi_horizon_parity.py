"""build_multi_horizon_features()의 anchor/target 분리가 정확한지 검증한다.

핵심 불변조건(history.md 18번 항목에서 이미 실측 검증된 것 — 여기서는 회귀 테스트로 고정):
horizon=1이면 anchor_ts==target_ts라 원본 테이블 그 행과 완전히 같아야 한다. horizon=k(k>1)면
lag/rolling(anchor 쪽)은 그대로, 날씨/캘린더/타겟(target 쪽)만 (k-1)시간 뒤 값으로 바뀌어야
한다. target_ts가 그리드 밖(예: 마지막 몇 시간)이면 그 (anchor, horizon) 조합은 빠져야 한다.
"""

import pandas as pd
import pytest
from ml_common.model_contract import LAG_ROLLING_FEATURE_COLUMNS

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_multi_horizon_features import (
    build_multi_horizon_features,
)


def _synthetic_features_table(n_hours: int = 20) -> pd.DataFrame:
    """station "A" 하나, 시간 단위(테스트 편의상 — anchor/target 시프트 로직은 tick 밀도와
    무관하게 hour_ts 실제 값 차이로만 동작하므로 5분 tick일 필요는 없다) hour_ts n_hours개.

    각 컬럼을 행 인덱스 i의 함수로 채워서, horizon=k 조합 후 값이 "정확히 (i+k-1)행에서 왔는지"를
    바로 확인할 수 있게 한다 — anchor 쪽(lag/rolling)은 `lag_val`, target 쪽(날씨 등)은
    `tgt_val`로 서로 다른 offset을 줘서 두 그룹이 안 섞였는지도 같이 검증한다.
    """
    hours = pd.date_range("2025-06-01 00:00", periods=n_hours, freq="h")
    n = len(hours)
    df = pd.DataFrame({"station_id": "A", "hour_ts": hours})
    for col in LAG_ROLLING_FEATURE_COLUMNS:
        df[col] = [1000.0 + i for i in range(n)]  # anchor 쪽 값 (1000대)
    for col in ["temp", "precip", "wind", "humidity", "pop_resd", "pop_long_foreign", "pop_short_foreign",
                "pop_total", "rental_exposure"]:
        df[col] = [2000.0 + i for i in range(n)]  # target 쪽 값 (2000대) — anchor 값과 구분됨
    df["capacity"] = 10.0
    df["lat"] = 37.5
    df["lon"] = 127.0
    df["hour"] = hours.hour
    df["dow"] = hours.dayofweek
    df["month"] = hours.month
    df["is_holiday"] = 0
    df["is_weekend"] = (hours.dayofweek >= 5).astype(int)
    df["is_next_day_off"] = (((hours.dayofweek + 1) % 7) >= 5).astype(int)
    df["is_prev_day_off"] = (((hours.dayofweek + 6) % 7) >= 5).astype(int)
    df["hour_sin"] = 0.0
    df["hour_cos"] = 1.0
    df["dow_sin"] = 0.0
    df["dow_cos"] = 1.0
    df["rental_count"] = list(range(n))  # 라벨 — i번째 행 = i
    df["return_count"] = list(range(100, 100 + n))
    df["date"] = hours.strftime("%Y-%m-%d")
    return df


def test_horizon_1_matches_source_row_exactly(spark):
    """horizon=1이면 anchor_ts==target_ts라 원본 행과 완전히 같아야 한다."""
    pdf = _synthetic_features_table()
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf).toPandas()

    h1 = out[out["horizon"] == 1].sort_values("anchor_ts").reset_index(drop=True)
    assert len(h1) == len(pdf)
    assert (h1["anchor_ts"].values == pdf["hour_ts"].values).all()
    for col in [*LAG_ROLLING_FEATURE_COLUMNS, "temp", "rental_exposure", "rental_count", "return_count", "date", "hour"]:
        assert (h1[col].to_numpy() == pdf[col].to_numpy()).all(), col


def test_horizon_k_shifts_only_target_columns(spark):
    """horizon=k면 lag/rolling(anchor)은 그대로, 날씨/타겟(target)만 (k-1)시간 뒤 값이어야 한다."""
    pdf = _synthetic_features_table(n_hours=20)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf).toPandas()

    k = 5
    hk = out[out["horizon"] == k].sort_values("anchor_ts").reset_index(drop=True)
    # anchor i(0-indexed)는 target (i+k-1)이 그리드 안에 있어야만 살아남는다.
    expected_n = len(pdf) - (k - 1)
    assert len(hk) == expected_n

    for i in range(expected_n):
        target_row = pdf.iloc[i + k - 1]
        anchor_row = pdf.iloc[i]
        assert hk.loc[i, "anchor_ts"] == anchor_row["hour_ts"]
        for col in LAG_ROLLING_FEATURE_COLUMNS:
            assert hk.loc[i, col] == anchor_row[col], f"{col} at i={i}"
        for col in ["temp", "rental_exposure", "rental_count", "return_count", "date", "hour"]:
            assert hk.loc[i, col] == target_row[col], f"{col} at i={i}"


def test_incomplete_horizon_rows_are_dropped_not_nulled(spark):
    """그리드 끝부분(target_ts가 미래로 넘어가 데이터가 없는 anchor)은 그 horizon에서 빠져야
    한다 — null로 채워지면 안 된다(잘못된 학습 신호가 됨)."""
    pdf = _synthetic_features_table(n_hours=6)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf).toPandas()

    assert out["temp"].isna().sum() == 0
    assert out[out["horizon"] == fe_config.HORIZON_COUNT].shape[0] == max(0, 6 - (fe_config.HORIZON_COUNT - 1))


def test_total_row_count_matches_horizon_sum(spark):
    """전체 행 수 = sum over h of max(0, n_hours - (h-1)) — 빠지는 행까지 정확히 맞아야 한다."""
    n_hours = 15
    pdf = _synthetic_features_table(n_hours=n_hours)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf).toPandas()

    expected_total = sum(max(0, n_hours - (h - 1)) for h in range(1, fe_config.HORIZON_COUNT + 1))
    assert len(out) == expected_total
