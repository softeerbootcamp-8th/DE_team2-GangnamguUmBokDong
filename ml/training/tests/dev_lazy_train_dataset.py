"""lazy_train_dataset.py — 날짜 파티션 단위 지연 로딩이 eager(전체 로드) 학습과
결과가 사실상 동일한지, LRU 캐시가 실제로 크기를 지키는지, 날짜 사이 라벨-feature
정렬이 어긋나지 않는지 검증한다.

moto S3에 **여러 날짜 × 날짜당 여러 part 파일**로 합성 parquet을 심는다 —
Spark의 실제 출력(`date=YYYY-MM-DD/part-*.parquet`, 날짜 하나에 파일 여러 개)을
그대로 흉내낸다. 날짜별로 x1/y 값을 확연히 다르게 심어서, 날짜 순서가 뒤섞이면
(라벨-feature misalignment) 바로 드러나게 한다.
"""

import lightgbm as lgb
import numpy as np
import pandas as pd
from core import s3 as s3_io

from training.lazy_train_dataset import (
    ChunkCache,
    build_lazy_dataset,
    predict_over_dates,
    station_categories_for_dates,
)

TABLE_PATH = "processed_v2/test/lazy_table"
FEATURE_COLUMNS = ["station_no", "x1", "x2"]
DATES = ["2026-01-02", "2026-01-03", "2026-01-04"]
STATION_DTYPE = pd.CategoricalDtype(categories=[1, 2, 3, 4])
# min_data_in_bin/min_data_in_leaf류는 일부러 안 건드린다 — Dataset 구성(bin 경계
# 확정) 시점에만 유효한 값인데, build_lazy_dataset()이 params 없이 construct()를
# 먼저 끝내므로(운영 코드의 기존 패턴과 동일 — LGB_PARAMS_COMMON도 이런 구성
# 파라미터를 안 둠) 이후 lgb.train()에서 바꾸려 하면 LightGBM이 "Cannot change ...
# after constructed Dataset handle"/"feature_pre_filter" 에러를 낸다. 12행짜리
# 합성 데이터라 기본값(min_data_in_leaf=20)으로는 분기가 안 생겨 상수 모델이 되지만,
# 이 테스트가 확인하려는 건 "같은 데이터가 들어갔는가"이지 실제 분기 여부가 아니다.
LGB_PARAMS = {"objective": "poisson", "verbosity": -1}


def _write_date(date_str: str, x1: float, x2: float, y: float, n_parts: int = 2) -> None:
    """station_no 1~4, 날짜 하나 분량을 n_parts개 파일로 쪼개 심는다(Spark 다중 파트 흉내)."""
    station_nos = [1, 2, 3, 4]
    idx_splits = np.array_split(np.arange(len(station_nos)), n_parts)
    for i, idx in enumerate(idx_splits):
        if len(idx) == 0:
            continue
        chunk = pd.DataFrame({
            "station_no": np.array(station_nos, dtype=np.int16)[idx],
            "x1": np.full(len(idx), x1, dtype=np.float32),
            "x2": np.full(len(idx), x2, dtype=np.float32),
            "y": np.full(len(idx), y, dtype=np.int16),
        })
        s3_io.write_parquet(chunk, f"{TABLE_PATH}/date={date_str}/part-{i:04d}.parquet")


def _seed() -> None:
    # 날짜마다 x1/y를 확연히 다르게 심는다 — 순서가 어긋나면 바로 드러남.
    _write_date("2026-01-02", x1=1.0, x2=0.0, y=10)
    _write_date("2026-01-03", x1=2.0, x2=1.0, y=20)
    _write_date("2026-01-04", x1=3.0, x2=2.0, y=30)


def _eager_arrays() -> tuple[np.ndarray, np.ndarray]:
    """비교 기준 — 전부 eager로 읽어 같은 규칙으로 float64 배열을 만든다."""
    df = s3_io.read_parquet(TABLE_PATH, columns=[*FEATURE_COLUMNS, "y"], dates=DATES)
    arr = np.empty((len(df), len(FEATURE_COLUMNS)), dtype=np.float64)
    for i, col in enumerate(FEATURE_COLUMNS):
        if col == "station_no":
            arr[:, i] = df[col].astype(STATION_DTYPE).cat.codes.to_numpy().astype(np.float64)
        else:
            arr[:, i] = df[col].to_numpy(dtype=np.float64)
    return arr, df["y"].to_numpy(dtype=np.float64)


def test_station_categories_for_dates_returns_sorted_unique_station_no():
    _seed()
    assert station_categories_for_dates(TABLE_PATH, DATES, filters=None) == [1, 2, 3, 4]


def test_build_lazy_dataset_label_matches_date_order():
    """dates 순서대로 이어붙인 y가 실제로 그 날짜의 라벨과 일치하는지(라벨-feature 정렬)."""
    _seed()
    cache = ChunkCache()
    _dataset, y, exposure = build_lazy_dataset(
        TABLE_PATH, DATES, FEATURE_COLUMNS, STATION_DTYPE, None, "y", None, cache
    )
    assert exposure is None
    assert list(y[0:4]) == [10.0] * 4  # 2026-01-02
    assert list(y[4:8]) == [20.0] * 4  # 2026-01-03
    assert list(y[8:12]) == [30.0] * 4  # 2026-01-04


def test_chunk_cache_never_exceeds_max_size_and_evicts_lru():
    from training.lazy_train_dataset import ChunkCache as _ChunkCache

    cache = _ChunkCache(max_size=2)
    calls = []

    def _loader(key):
        def _load():
            calls.append(key)
            return np.array([key])
        return _load

    cache.get_or_fetch("a", _loader("a"))
    cache.get_or_fetch("b", _loader("b"))
    assert len(cache._data) == 2
    cache.get_or_fetch("c", _loader("c"))  # "a"가 LRU로 밀려나야 함
    assert len(cache._data) == 2
    assert "a" not in cache._data
    assert set(cache._data.keys()) == {"b", "c"}

    # "a"를 다시 요청하면 캐시에 없으니 재조회(loader 재호출)
    cache.get_or_fetch("a", _loader("a"))
    assert calls.count("a") == 2  # 최초 1회 + 재조회 1회
    assert calls.count("b") == 1
    assert calls.count("c") == 1


def test_build_lazy_dataset_training_matches_eager_training():
    """Sequence 기반으로 만든 Dataset을 학습시킨 결과가 eager(전체 배열) 학습과 사실상 동일한지."""
    _seed()
    eager_arr, y_eager = _eager_arrays()
    eager_set = lgb.Dataset(eager_arr, label=y_eager, categorical_feature=[0])
    eager_booster = lgb.train(LGB_PARAMS, eager_set, num_boost_round=5)

    cache = ChunkCache()
    lazy_set, y_lazy, _ = build_lazy_dataset(TABLE_PATH, DATES, FEATURE_COLUMNS, STATION_DTYPE, None, "y", None, cache)
    lazy_booster = lgb.train(LGB_PARAMS, lazy_set, num_boost_round=5)

    assert len(y_lazy) == len(y_eager)
    assert np.allclose(sorted(y_lazy), sorted(y_eager))
    assert np.allclose(eager_booster.predict(eager_arr), lazy_booster.predict(eager_arr))


def test_predict_over_dates_matches_per_date_eager_predict():
    """predict_over_dates()가 청크(날짜) 단위로 predict한 결과를 이어붙인 것이,
    각 날짜를 직접 eager로 읽어 predict한 것과 순서·값 모두 일치하는지."""
    _seed()
    eager_arr, y_eager = _eager_arrays()
    booster = lgb.train(LGB_PARAMS, lgb.Dataset(eager_arr, label=y_eager, categorical_feature=[0]), num_boost_round=5)

    expected_preds = []
    expected_y = []
    for d in DATES:
        df = s3_io.read_parquet(TABLE_PATH, columns=[*FEATURE_COLUMNS, "y"], dates=[d])
        arr = np.empty((len(df), len(FEATURE_COLUMNS)), dtype=np.float64)
        for i, col in enumerate(FEATURE_COLUMNS):
            if col == "station_no":
                arr[:, i] = df[col].astype(STATION_DTYPE).cat.codes.to_numpy().astype(np.float64)
            else:
                arr[:, i] = df[col].to_numpy(dtype=np.float64)
        expected_preds.append(booster.predict(arr, num_iteration=booster.best_iteration))
        expected_y.append(df["y"].to_numpy(dtype=np.float64))
    expected_preds = np.concatenate(expected_preds)
    expected_y = np.concatenate(expected_y)

    result = predict_over_dates(
        TABLE_PATH, DATES, FEATURE_COLUMNS, STATION_DTYPE, None, "y", None, {"poisson": booster}
    )

    assert np.array_equal(result["y"], expected_y)
    assert np.allclose(result["poisson"], expected_preds)
    assert result["exposure"] is None
