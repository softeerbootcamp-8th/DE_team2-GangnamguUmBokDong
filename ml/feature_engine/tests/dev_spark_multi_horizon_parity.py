"""build_multi_horizon_features()의 anchor/target 분리가 정확한지 검증한다.

핵심 불변조건(history.md 18번 항목에서 이미 실측 검증된 것 — 여기서는 회귀 테스트로 고정):
horizon=1이면 anchor_ts==target_ts라 원본 테이블 그 행과 완전히 같아야 한다. horizon=k(k>1)면
lag(anchor 쪽)은 그대로, 날씨/캘린더/타겟(target 쪽)만 (k-1)시간 뒤 값으로 바뀌어야
한다. target_ts가 그리드 밖(예: 마지막 몇 시간)이면 그 (anchor, horizon) 조합은 빠져야 한다.

대여/반납이 완전히 분리된 데이터셋이라 이 함수는 이제 anchor_columns/target_columns를
받는 제네릭 함수다 — 대여 컬럼셋으로 핵심 시프트 로직을 검증하고, 반납 컬럼셋으로는
lag 컬럼명만 다르다는 것만 짧게 확인한다.
"""

from datetime import date

import pandas as pd
import pytest
from ml_core import paths as core_paths

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_multi_horizon_features import (
    RENTAL_ANCHOR_COLUMNS,
    RENTAL_TARGET_COLUMNS,
    RETURN_ANCHOR_COLUMNS,
    RETURN_TARGET_COLUMNS,
    _anchor_input,
    _features_in_training_window,
    _write_date_partitioned,
    build_multi_horizon_features,
)

_DAY_INDEX_EPOCH = pd.Timestamp("2000-01-01")


def test_multi_horizon_output_paths_match_shared_contract():
    """Spark writer와 training reader가 같은 anchor별 S3 키를 가리켜야 한다."""
    assert fe_config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET == (
        f"s3a://{fe_config.S3_BUCKET}/{core_paths.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET}"
    )
    assert fe_config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET == (
        f"s3a://{fe_config.S3_BUCKET}/{core_paths.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET}"
    )


def _synthetic_features_table(n_hours: int = 20) -> pd.DataFrame:
    """station "A" 하나, 시간 단위(테스트 편의상 — anchor/target 시프트 로직은 tick 밀도와
    무관하게 hour_ts 실제 값 차이로만 동작하므로 GRID_TICK_MINUTES 간격일 필요는 없다)
    hour_ts n_hours개.

    각 컬럼을 행 인덱스 i의 함수로 채워서, horizon=k 조합 후 값이 "정확히 (i+k-1)행에서 왔는지"를
    바로 확인할 수 있게 한다 — anchor 쪽(lag)은 1000/1500대, target 쪽(날씨 등)은
    2000대로 서로 다른 offset을 줘서 두 그룹이 안 섞였는지도 같이 검증한다.
    """
    hours = pd.date_range("2025-06-01 00:00", periods=n_hours, freq="h")
    n = len(hours)
    df = pd.DataFrame({"station_id": "A", "station_no": 1, "hour_ts": hours})
    df["rental_lag_1h"] = [1000.0 + i for i in range(n)]
    df["return_lag_1h"] = [1500.0 + i for i in range(n)]
    for col in ["temp", "precip", "pop_total", "rental_exposure"]:
        df[col] = [2000.0 + i for i in range(n)]
    df["capacity"] = 10.0
    df["lat"] = 37.5
    df["lon"] = 127.0
    df["hour"] = hours.hour
    df["minute"] = hours.hour * 60 + hours.minute
    df["dow"] = hours.dayofweek
    df["is_holiday"] = (hours.dayofweek >= 5).astype(int)
    df["day"] = (hours.normalize() - _DAY_INDEX_EPOCH).days
    df["rental_count"] = list(range(n))  # 라벨 — i번째 행 = i
    df["return_count"] = list(range(100, 100 + n))
    df["date"] = hours.strftime("%Y-%m-%d")
    return df


def test_horizon_1_matches_source_row_exactly(spark):
    """horizon=1이면 anchor_ts==target_ts라 원본 행과 완전히 같아야 한다."""
    pdf = _synthetic_features_table()
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS).toPandas()

    h1 = out[out["horizon"] == 1].sort_values("anchor_ts").reset_index(drop=True)
    assert len(h1) == len(pdf)
    assert (h1["anchor_ts"].values == pdf["hour_ts"].values).all()
    assert "return_count" not in h1.columns  # 대여 데이터셋엔 반납 라벨이 없어야 함
    for col in ["rental_lag_1h", "temp", "rental_exposure", "rental_count", "date", "hour", "minute", "day"]:
        assert (h1[col].to_numpy() == pdf[col].to_numpy()).all(), col


def test_horizon_k_shifts_only_target_columns(spark):
    """horizon=k면 lag(anchor)는 그대로, 날씨/타겟(target)만 (k-1)시간 뒤 값이어야 한다."""
    pdf = _synthetic_features_table(n_hours=20)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS).toPandas()

    k = 5
    hk = out[out["horizon"] == k].sort_values("anchor_ts").reset_index(drop=True)
    # anchor i(0-indexed)는 target (i+k-1)이 그리드 안에 있어야만 살아남는다.
    expected_n = len(pdf) - (k - 1)
    assert len(hk) == expected_n

    for i in range(expected_n):
        target_row = pdf.iloc[i + k - 1]
        anchor_row = pdf.iloc[i]
        assert hk.loc[i, "anchor_ts"] == anchor_row["hour_ts"]
        assert hk.loc[i, "rental_lag_1h"] == anchor_row["rental_lag_1h"]
        for col in ["temp", "rental_exposure", "rental_count", "date", "hour", "minute", "day"]:
            assert hk.loc[i, col] == target_row[col], f"{col} at i={i}"


def test_return_side_uses_return_lag_1h_as_anchor_column(spark):
    """반납 컬럼셋은 rental_lag_1h가 아니라 return_lag_1h를 anchor로 고정해야 한다."""
    pdf = _synthetic_features_table(n_hours=10)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf, RETURN_ANCHOR_COLUMNS, RETURN_TARGET_COLUMNS).toPandas()

    assert "return_lag_1h" in out.columns
    assert "rental_lag_1h" not in out.columns
    assert "rental_exposure" not in out.columns  # 반납 쪽엔 exposure 없음

    h1 = out[out["horizon"] == 1].sort_values("anchor_ts").reset_index(drop=True)
    assert (h1["return_lag_1h"].to_numpy() == pdf["return_lag_1h"].to_numpy()).all()


def test_incomplete_horizon_rows_are_dropped_not_nulled(spark):
    """그리드 끝부분(target_ts가 미래로 넘어가 데이터가 없는 anchor)은 그 horizon에서 빠져야
    한다 — null로 채워지면 안 된다(잘못된 학습 신호가 됨)."""
    pdf = _synthetic_features_table(n_hours=6)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS).toPandas()

    assert out["temp"].isna().sum() == 0
    assert out[out["horizon"] == fe_config.HORIZON_COUNT].shape[0] == max(0, 6 - (fe_config.HORIZON_COUNT - 1))


def test_total_row_count_matches_horizon_sum(spark):
    """전체 행 수 = sum over h of max(0, n_hours - (h-1)) — 빠지는 행까지 정확히 맞아야 한다."""
    n_hours = 15
    pdf = _synthetic_features_table(n_hours=n_hours)
    sdf = spark.createDataFrame(pdf)
    out = build_multi_horizon_features(spark, sdf, RENTAL_ANCHOR_COLUMNS, RENTAL_TARGET_COLUMNS).toPandas()

    expected_total = sum(max(0, n_hours - (h - 1)) for h in range(1, fe_config.HORIZON_COUNT + 1))
    assert len(out) == expected_total


def test_training_window_filter_excludes_newer_partitions(spark, monkeypatch):
    """stale input partition이 있어도 exact window 밖 tick은 최종 build 입력에서 빠진다."""
    monkeypatch.setattr(fe_config, "WINDOW_START", date(2025, 1, 1))
    monkeypatch.setattr(fe_config, "WINDOW_END", date(2025, 12, 31))
    source = spark.createDataFrame(pd.DataFrame({
        "hour_ts": pd.to_datetime([
            "2024-12-31 23:55:00",
            "2025-01-01 00:00:00",
            "2025-12-31 23:00:00",
            "2025-12-31 23:55:00",
            "2026-01-01 00:00:00",
        ]),
        "value": [0, 1, 2, 3, 4],
    }))

    got = _features_in_training_window(source).orderBy("hour_ts").toPandas()

    assert got["value"].tolist() == [1, 2]


def test_date_partitioned_writer_creates_one_data_file_per_date(spark, tmp_path):
    """여러 입력 partition이 있어도 날짜별 Parquet data file은 하나만 만든다."""
    output_path = tmp_path / "multi-horizon.parquet"
    source = spark.createDataFrame(
        [
            ("2025-06-01", station_no, horizon)
            for station_no in range(8)
            for horizon in range(1, 4)
        ]
        + [
            ("2025-06-02", station_no, horizon)
            for station_no in range(8)
            for horizon in range(1, 4)
        ],
        ["date", "station_no", "horizon"],
    ).repartition(6)

    _write_date_partitioned(source, str(output_path))

    files_by_date = {
        date_dir.name: list(date_dir.glob("part-*.parquet"))
        for date_dir in output_path.glob("date=*")
    }
    assert set(files_by_date) == {"date=2025-06-01", "date=2025-06-02"}
    assert all(len(files) == 1 for files in files_by_date.values())


def test_anchor_input_returns_none_when_training_anchor_matches_grid(spark, monkeypatch):
    """thinning 없는 기본 계약은 불필요한 Spark filter를 만들지 않아야 한다."""
    monkeypatch.setattr(fe_config, "GRID_TICK_MINUTES", 20)
    monkeypatch.setattr(fe_config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    monkeypatch.delenv("MULTI_HORIZON_ANCHOR_SINCE", raising=False)
    monkeypatch.delenv("MULTI_HORIZON_ANCHOR_UNTIL", raising=False)
    features = spark.createDataFrame(pd.DataFrame({"hour_ts": pd.to_datetime(["2025-06-01 00:00:00"])}))

    assert _anchor_input(features) is None


def test_anchor_input_thins_five_minute_grid_to_twenty_minute_anchors(spark, monkeypatch):
    """g5/a20 hybrid는 target 원본을 바꾸지 않고 anchor 후보만 00/20/40분으로 줄인다."""
    monkeypatch.setattr(fe_config, "GRID_TICK_MINUTES", 5)
    monkeypatch.setattr(fe_config, "TRAIN_ANCHOR_TICK_MINUTES", 20)
    monkeypatch.delenv("MULTI_HORIZON_ANCHOR_SINCE", raising=False)
    monkeypatch.delenv("MULTI_HORIZON_ANCHOR_UNTIL", raising=False)
    features = spark.createDataFrame(
        pd.DataFrame({"hour_ts": pd.date_range("2025-06-01 00:00:00", periods=12, freq="5min")})
    )

    got = _anchor_input(features).orderBy("hour_ts").toPandas()

    assert got["hour_ts"].dt.strftime("%H:%M").tolist() == ["00:00", "00:20", "00:40"]
