"""학습(feature_engine/spark/build_features.py, 배치)과 추론(predict_single.py,
단일 시점)이 같은 트립 데이터에 대해 rental_lag_1h를 정확히 같은 값으로 계산하는지
확인한다.

rolling_window_features.py 3절이 요구하는 "핵심 필터 조건은 동일해야 하고, 이게
갈라지면 이번에 고치려는 skew가 재발한다"는 원칙을 실제 코드로 대조하는 회귀 테스트다
— 배치 쪽은 censored_rolling_counts()의 차분배열 알고리즘, 서빙 쪽은
count_visible_in_window()을 anchor마다 반복 호출하는 방식으로 구현이 다르기 때문에,
이 대조가 없으면 두 경로가 조용히 어긋나도 알아챌 방법이 없다.

**실제 서비스가 쓰는 Spark 구현(`_add_rental_lag_1h`)을 그대로 불러다 비교한다**
— Spark 로직을 직접 검증 대상으로 삼으므로, **pyspark가 있는 venv
(`feature_engine/.venv`)로 실행해야 한다** — `inference/.venv`에는 pyspark가 없어
`pytest.importorskip`으로 자동 skip된다:
    cd ml && ./feature_engine/.venv/bin/python -m pytest inference/tests/dev_rental_censoring_cross_parity.py -q
"""

import os

import numpy as np
import pandas as pd
import pytest
from ml_core.rolling_window_features import censored_rolling_counts

pyspark = pytest.importorskip("pyspark")

from feature_engine.spark import config as fe_config
from feature_engine.spark.build_features import _add_rental_lag_1h

from inference import predict_single as ps


@pytest.fixture(scope="module")
def spark():
    import sys

    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    # timestamp_ntz/timestamp(tz-aware) 왕복 어긋남 방지 — feature_engine/spark_session.py 참고.
    os.environ.setdefault("TZ", "Asia/Seoul")

    session = (
        SparkSession.builder.master("local[2]")
        .appName("test-inference-rental-censoring-cross-parity")
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


@pytest.fixture(autouse=True)
def _reset_predict_single_caches():
    # _rental_events_sorted_by_station은 station_id 키로 in-place mutate되는
    # 캐시라(_rental_visible_at() 참고) save/restore로 참조만 되돌리면 이전
    # 테스트에서 채워진 항목이 새 테스트로 새어 들어간다 — 매번 새 dict로 비운다.
    names = [
        "_history_by_station", "_rental_events_by_station", "_rental_events_coverage",
        "_station_profile_station_index", "_station_profile_values",
    ]
    saved = {n: getattr(ps, n) for n in names}
    ps._rental_events_sorted_by_station = {}
    yield
    for n, v in saved.items():
        setattr(ps, n, v)
    ps._rental_events_sorted_by_station = {}


@pytest.fixture
def trips() -> pd.DataFrame:
    return pd.DataFrame(
        [
            _trip("A", "2025-06-01 09:00:07", "2025-06-01 09:10:33"),
            _trip("A", "2025-06-01 09:20:00", "2025-06-01 10:00:00"),
            _trip("A", "2025-06-01 09:50:00", "2025-06-01 11:20:00"),
            _trip("A", "2025-06-01 10:40:00", "2025-06-01 10:55:00"),
        ]
    )


def test_rental_lag_1h_matches_between_batch_and_single_point(spark, trips, tmp_path, monkeypatch):
    station_no = 1
    cumulative = censored_rolling_counts(
        trips,
        window_minutes=fe_config.ROLLING_WINDOW_MINUTES,
        embargo_minutes=fe_config.ROLLING_EMBARGO_MINUTES,
        tick_minutes=fe_config.ROLLING_TICK_MINUTES,
    )
    # ns 단위 parquet TIMESTAMP는 이 Spark 버전이 못 읽는다("Illegal Parquet type:
    # INT64 (TIMESTAMP(NANOS,...))") — us로 캐스팅(dev_spark_build_features.py와 동일 이유).
    cumulative["tick"] = cumulative["tick"].astype("datetime64[us]")
    rolling_path = str(tmp_path / "rolling_rental_features_test.parquet")
    cumulative.to_parquet(rolling_path, index=False)

    # _add_rental_lag_1h()가 rolling 테이블(station_id 원본)을 station_no로 바꾸려고
    # station_master를 읽는다 — 여기 station_id="A" <-> station_no=1로 매핑해둔다
    # (build_features.py 모듈 docstring 참고).
    master_path = str(tmp_path / "station_master_test.parquet")
    pd.DataFrame([{"station_id": "A", "station_no": station_no}]).to_parquet(master_path, index=False)
    monkeypatch.setattr(fe_config, "STATION_MASTER_PARQUET", master_path)

    # 그리드는 05-31 00:00부터 06-01 12:00까지 — 뒤쪽은 cumulative의 tick 커버리지
    # (최대 12:15, 트립들의 [T-100,T-40) 창이 미치는 범위) 안에 들어야 freshness
    # 가드에 안 걸린다. **GRID_TICK_MINUTES 그리드여야 한다** — 배치
    # (build_features.py)와 단일 시점(predict_single.py) 둘 다 같은 tick 간격을
    # 전제로 하므로, batch 쪽 그리드가 다른 밀도면 애초에 비교가 성립하지 않는다.
    ticks = pd.date_range("2025-05-31 00:00", "2025-06-01 12:00", freq=f"{fe_config.GRID_TICK_MINUTES}min")
    df = pd.DataFrame(
        {"station_no": station_no, "hour_ts": ticks, "rental_count": [0] * len(ticks), "return_count": [0] * len(ticks)}
    )
    sdf = spark.createDataFrame(df)
    batch_out = (
        _add_rental_lag_1h(spark, sdf, rolling_path).toPandas().sort_values("hour_ts").reset_index(drop=True)
    )

    ps._rental_events_by_station = {"A": trips.reset_index(drop=True)}
    ps._rental_events_coverage = (pd.Timestamp("2025-01-01"), pd.Timestamp("2025-12-31 23:59:59"))
    ps._history_by_station = {}  # return_lag_1h는 이 테스트 대상이 아님 — 항상 profile fallback
    ps._station_profile_station_index = {}
    ps._station_profile_values = np.empty((0, 0, 0, 0, 0), dtype="float32")

    check_targets = pd.date_range("2025-06-01 08:00", "2025-06-01 12:00", freq="h")
    for target_ts in check_targets:
        i = ticks.get_loc(target_ts)
        single_out, fallback = ps._lag_rolling_features("A", station_no, target_ts)
        assert "rental_lag_1h" not in fallback, f"{target_ts}: 예상치 못한 fallback {fallback}"
        assert single_out["rental_lag_1h"] == pytest.approx(batch_out["rental_lag_1h"].iloc[i]), target_ts
