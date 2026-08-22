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
import pytest
from core import s3 as s3_io
from ml_core import common_config, profile_contract

from training.lazy_train_dataset import (
    ChunkCache,
    _read_date_chunk,
    build_lazy_dataset,
    predict_over_dates,
    station_categories_for_dates,
)

TABLE_PATH = "processed_v2/test/lazy_table"
FEATURE_COLUMNS = ["station_no", "x1", "x2"]
DATES = ["2026-01-02", "2026-01-03", "2026-01-04"]
STATION_DTYPE = pd.CategoricalDtype(categories=[1, 2, 3, 4])
# 작은 합성 데이터라 기본 min_data_in_leaf에서는 상수 모델이 되지만, 이 테스트가
# 확인하려는 건 "같은 데이터가 들어갔는가"이지 실제 분기 여부가 아니다. 운영
# build_lazy_dataset()은 construct 시점부터 LGB_PARAMS_COMMON을 받아 max_bin 같은
# Dataset 파라미터가 학습 시점과 어긋나지 않게 한다.
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


def test_read_date_chunk_safely_promotes_compatible_part_schemas():
    """같은 날짜에 신구 part가 공존해도 값 손실 없는 숫자 타입 변화는 결합한다."""
    path = "processed_v2/test/schema_drift"
    day = "2026-01-02"
    first = pd.DataFrame({
        "station_no": np.array([1], dtype=np.int16),
        "x1": np.array([10], dtype=np.int16),
    })
    second = pd.DataFrame({
        "station_no": np.array([2], dtype=np.int16),
        "x1": np.array([20.5], dtype=np.float32),
        "x2": np.array([3.5], dtype=np.float32),
    })
    s3_io.write_parquet(first, f"{path}/date={day}/part-0000.parquet")
    s3_io.write_parquet(second, f"{path}/date={day}/part-0001.parquet")

    result = _read_date_chunk(path, day, ["station_no", "x1", "x2"], filters=None)

    assert result["x1"].dtype.name == "float32"
    assert result["x1"].tolist() == [10.0, 20.5]
    assert result["x2"].dtype.name == "float32"
    assert pd.isna(result.loc[0, "x2"])
    assert result.loc[1, "x2"] == 3.5


def test_read_date_chunk_rejects_column_missing_from_every_part():
    """잘못된 feature 계약을 뒤쪽 KeyError가 아니라 날짜 chunk 로드 시점에 진단한다."""
    path = "processed_v2/test/missing_feature"
    day = "2026-01-02"
    s3_io.write_parquet(pd.DataFrame({"station_no": [1]}), f"{path}/date={day}/part-0000.parquet")

    with pytest.raises(s3_io.ParquetSchemaMismatchError, match="어떤 Parquet part에도 없습니다"):
        _read_date_chunk(path, day, ["unknown_feature"], filters=None)


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
    assert isinstance(y, np.memmap)
    assert not __import__("pathlib").Path(y.filename).exists()


def test_build_lazy_dataset_streams_exposure_to_unlinked_memmap():
    """대여 exposure와 init_score 경로도 전체 pandas 합본 없이 disk-backed여야 한다."""
    path = "processed_v2/test/lazy_exposure"
    for index, date_str in enumerate(DATES, start=1):
        frame = pd.DataFrame({
            "station_no": np.array([1, 2], dtype=np.int16),
            "x1": np.array([index, index + 1], dtype=np.float32),
            "x2": np.array([0, 1], dtype=np.float32),
            "y": np.array([index, index + 1], dtype=np.int16),
            "exposure": np.array([0.05, 1.0], dtype=np.float32),
        })
        s3_io.write_parquet(frame, f"{path}/date={date_str}/part-0000.parquet")

    _dataset, y, exposure = build_lazy_dataset(
        path,
        DATES,
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        "exposure",
        ChunkCache(),
    )

    assert isinstance(y, np.memmap)
    assert isinstance(exposure, np.memmap)
    assert np.allclose(exposure, [0.05, 1.0] * len(DATES))
    assert not __import__("pathlib").Path(exposure.filename).exists()


def test_build_lazy_dataset_sets_feature_names_and_construction_params():
    """Dataset 생성 시 feature 이름과 binning 파라미터가 함께 고정돼야 한다."""
    _seed()
    profile = profile_contract.merge_and_validate_profile(
        {"LGB_PARAMS_COMMON": {"max_bin": 31, "min_data_in_leaf": 5}},
        "dataset-future-key",
    )
    dataset_params = common_config._build_lgb_params(profile["LGB_PARAMS_COMMON"])

    dataset, _y, _exposure = build_lazy_dataset(
        TABLE_PATH,
        DATES,
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        None,
        ChunkCache(),
        dataset_params=dataset_params,
    )

    assert dataset.feature_name == FEATURE_COLUMNS
    assert dataset.params["max_bin"] == 31
    assert dataset.params["min_data_in_leaf"] == 5
    assert dataset.label is None
    assert dataset.init_score is None


def test_build_lazy_dataset_keeps_reference_positional_argument_compatible():
    """기존 9번째 positional reference 호출이 dataset_params 추가 후에도 유지돼야 한다."""
    _seed()
    train_set, _y, _exposure = build_lazy_dataset(
        TABLE_PATH, DATES[:2], FEATURE_COLUMNS, STATION_DTYPE, None, "y", None, ChunkCache()
    )

    valid_set, _y, _exposure = build_lazy_dataset(
        TABLE_PATH,
        DATES[2:],
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        None,
        ChunkCache(),
        train_set,
    )

    assert valid_set.reference is train_set


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


def test_lazy_dataset_resume_matches_uninterrupted_with_unequal_chunks():
    """서로 다른 길이의 날짜 chunk도 전체 결합 없이 동일한 모델로 이어 학습한다."""
    path = "processed_v2/test/lazy_resume"
    rng = np.random.default_rng(17)
    resume_dates = ["2026-02-01", "2026-02-02", "2026-02-03"]
    for day_index, (date_str, row_count) in enumerate(
        zip(resume_dates, [75, 120, 45], strict=True)
    ):
        x1 = rng.normal(loc=day_index, size=row_count).astype(np.float32)
        x2 = rng.normal(size=row_count).astype(np.float32)
        frame = pd.DataFrame({
            "station_no": rng.integers(1, 5, size=row_count, dtype=np.int16),
            "x1": x1,
            "x2": x2,
            "y": np.maximum(0, np.rint(4 + 1.5 * x1 - 0.7 * x2)).astype(np.int16),
        })
        s3_io.write_parquet(frame, f"{path}/date={date_str}/part-0000.parquet")

    params = {
        "objective": "poisson",
        "metric": "poisson",
        "learning_rate": 0.08,
        "num_leaves": 7,
        "min_data_in_leaf": 5,
        "verbosity": -1,
        "num_threads": 1,
        "seed": 19,
    }

    first_set, _, _ = build_lazy_dataset(
        path,
        resume_dates,
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        None,
        ChunkCache(),
        dataset_params=params,
        keep_raw_data=True,
    )
    first_booster = lgb.train(params, first_set, num_boost_round=4)

    resume_set, _, _ = build_lazy_dataset(
        path,
        resume_dates,
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        None,
        ChunkCache(),
        dataset_params=params,
        keep_raw_data=True,
    )
    resumed = lgb.train(params, resume_set, num_boost_round=3, init_model=first_booster)

    full_set, _, _ = build_lazy_dataset(
        path,
        resume_dates,
        FEATURE_COLUMNS,
        STATION_DTYPE,
        None,
        "y",
        None,
        ChunkCache(),
        dataset_params=params,
    )
    uninterrupted = lgb.train(params, full_set, num_boost_round=7)
    eager_arr = s3_io.read_parquet(
        path,
        columns=FEATURE_COLUMNS,
        dates=resume_dates,
    )
    feature_arr = np.column_stack([
        eager_arr["station_no"].astype(STATION_DTYPE).cat.codes.to_numpy(dtype=np.float64),
        eager_arr["x1"].to_numpy(dtype=np.float64),
        eager_arr["x2"].to_numpy(dtype=np.float64),
    ])

    assert resumed.current_iteration() == uninterrupted.current_iteration() == 7
    np.testing.assert_allclose(
        resumed.predict(feature_arr),
        uninterrupted.predict(feature_arr),
        rtol=1e-12,
        atol=1e-12,
    )


def test_lazy_dataset_resume_preserves_exposure_init_score():
    """Poisson 재개 시 이전 tree score와 원래 exposure offset을 함께 복원한다."""
    path = "processed_v2/test/lazy_resume_exposure"
    rng = np.random.default_rng(23)
    resume_dates = ["2026-03-01", "2026-03-02"]
    for day_index, (date_str, row_count) in enumerate(
        zip(resume_dates, [90, 130], strict=True)
    ):
        x1 = rng.normal(loc=day_index, size=row_count).astype(np.float32)
        x2 = rng.normal(size=row_count).astype(np.float32)
        exposure = rng.uniform(0.2, 1.0, size=row_count).astype(np.float32)
        frame = pd.DataFrame({
            "station_no": rng.integers(1, 5, size=row_count, dtype=np.int16),
            "x1": x1,
            "x2": x2,
            "y": rng.poisson(exposure * np.exp(1.0 + 0.2 * x1)).astype(np.int16),
            "exposure": exposure,
        })
        s3_io.write_parquet(frame, f"{path}/date={date_str}/part-0000.parquet")

    params = {
        "objective": "poisson",
        "metric": "poisson",
        "learning_rate": 0.08,
        "num_leaves": 7,
        "min_data_in_leaf": 5,
        "verbosity": -1,
        "num_threads": 1,
        "seed": 29,
    }

    def dataset(keep_raw_data: bool):
        """같은 exposure 계약의 새 lazy Dataset을 만든다."""
        return build_lazy_dataset(
            path,
            resume_dates,
            FEATURE_COLUMNS,
            STATION_DTYPE,
            None,
            "y",
            "exposure",
            ChunkCache(),
            dataset_params=params,
            keep_raw_data=keep_raw_data,
        )[0]

    first_booster = lgb.train(params, dataset(True), num_boost_round=4)
    resumed = lgb.train(params, dataset(True), num_boost_round=3, init_model=first_booster)
    uninterrupted = lgb.train(params, dataset(False), num_boost_round=7)

    resumed_dump = resumed.dump_model()
    uninterrupted_dump = uninterrupted.dump_model()
    assert resumed_dump["tree_info"] == uninterrupted_dump["tree_info"]
    feature_frame = s3_io.read_parquet(path, columns=FEATURE_COLUMNS, dates=resume_dates)
    feature_arr = np.column_stack([
        feature_frame["station_no"].astype(STATION_DTYPE).cat.codes.to_numpy(dtype=np.float64),
        feature_frame["x1"].to_numpy(dtype=np.float64),
        feature_frame["x2"].to_numpy(dtype=np.float64),
    ])
    np.testing.assert_allclose(
        resumed.predict(feature_arr),
        uninterrupted.predict(feature_arr),
        rtol=1e-12,
        atol=1e-12,
    )


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


def test_adaptive_anchor_mask_rules():
    """평일 및 휴일 시간대별 가변 앵커링 마스크 규칙이 정확히 동작하는지 검증한다.

    - 평일: 주간(07~21시) 20분 단위, 평시 60분 단위, 심야(00~06시) 3일 1회 60분
    - 휴일: 주간(08~21시) 20분 단위, 평시 60분 단위, 심야(00~06시) 3일 1회 60분
    """
    from training.lazy_train_dataset import _adaptive_anchor_mask

    all_minutes = pd.Series(range(0, 1440, 20))  # 0, 20, 40, ..., 1420 (총 72개)

    # 1) 평일 심야 대상일 (is_night_day=True, is_holiday=False)
    mask_w_night = _adaptive_anchor_mask(all_minutes, is_night_day=True, is_holiday=False)
    selected_w_night = all_minutes[mask_w_night].tolist()
    # 심야 6개 (0, 60, 120, 180, 240, 300)
    # 평시 6시 (360) 1개
    # 주간 7~20시 (420~1240, 14시간 * 3) 42개
    # 평시 21~23시 (1260, 1320, 1380) 3개
    # 합계: 6 + 1 + 42 + 3 = 52개
    assert len(selected_w_night) == 52
    assert 0 in selected_w_night
    assert 20 not in selected_w_night  # 심야 20분 탈락
    assert 420 in selected_w_night and 440 in selected_w_night  # 07시 20분 유지
    assert 600 in selected_w_night and 620 in selected_w_night  # 10시 20분 주간 유지
    assert 720 in selected_w_night and 740 in selected_w_night  # 12시 20분 점심 유지

    # 2) 평일 심야 비대상일 (is_night_day=False, is_holiday=False)
    mask_w_non_night = _adaptive_anchor_mask(all_minutes, is_night_day=False, is_holiday=False)
    selected_w_non_night = all_minutes[mask_w_non_night].tolist()
    # 심야 0개, 나머지 46개
    assert len(selected_w_non_night) == 46
    assert 0 not in selected_w_non_night

    # 3) 휴일 심야 대상일 (is_night_day=True, is_holiday=True)
    mask_h_night = _adaptive_anchor_mask(all_minutes, is_night_day=True, is_holiday=True)
    selected_h_night = all_minutes[mask_h_night].tolist()
    # 심야 6개 (0, 60, 120, 180, 240, 300)
    # 평시 6~7시 (360, 420) 2개
    # 주간 8~20시 (480~1240, 13시간 * 3) 39개
    # 평시 21~23시 (1260, 1320, 1380) 3개
    # 합계: 6 + 2 + 39 + 3 = 50개
    assert len(selected_h_night) == 50
    assert 420 in selected_h_night and 440 not in selected_h_night  # 휴일 07시는 60분 정시만 유지
    assert 480 in selected_h_night and 500 in selected_h_night  # 휴일 08시는 20분 주간 유지
    assert 780 in selected_h_night and 800 in selected_h_night  # 휴일 13시는 20분 주간 유지

    # 4) 커스텀 피크 시간대 지정 테스트
    mask_custom = _adaptive_anchor_mask(
        all_minutes,
        is_night_day=False,
        is_holiday=False,
        weekday_peak_hours=((8, 9),),
    )
    selected_custom = all_minutes[mask_custom].tolist()
    assert 480 in selected_custom and 500 in selected_custom  # 08:00~09:00 피크 20분 유지
    assert 420 in selected_custom and 440 not in selected_custom  # 07:00는 평시 정시만 유지


def test_read_date_chunk_applies_adaptive_anchor_filter():
    """_read_date_chunk가 minute 컬럼을 감지해 가변 앵커 필터를 적용하는지 검증한다."""
    path = "processed_v2/test/adaptive_table"
    day = "2025-01-02"  # 평일 (목요일)
    minutes = list(range(0, 1440, 20))  # 72행
    df = pd.DataFrame({
        "station_no": [1] * len(minutes),
        "minute": minutes,
        "x1": [1.0] * len(minutes),
    })
    s3_io.write_parquet(df, f"{path}/date={day}/part-0000.parquet")

    # adaptive_anchors=True: 평일 비심야일(2025-01-02 % 3 != 0) 46행 반환
    result_adaptive = _read_date_chunk(path, day, ["station_no", "minute", "x1"], filters=None, adaptive_anchors=True)
    assert len(result_adaptive) == 46

    # adaptive_anchors=False: 72행 전체 반환
    result_all = _read_date_chunk(path, day, ["station_no", "minute", "x1"], filters=None, adaptive_anchors=False)
    assert len(result_all) == 72


def test_read_date_chunk_applies_sparse_horizon_filter():
    """_read_date_chunk가 PyArrow filters로 horizon 스파스 샘플링을 적용하는지 검증한다."""
    path = "processed_v2/test/horizon_table"
    day = "2025-01-01"
    horizons = list(range(1, 13))  # 1~12 (12행)
    df = pd.DataFrame({
        "station_no": [1] * 12,
        "minute": [480] * 12,  # 피크 시간 08:00
        "horizon": horizons,
    })
    s3_io.write_parquet(df, f"{path}/date={day}/part-0000.parquet")

    sparse_horizons = [1, 2, 3, 4, 5, 6, 9, 12]
    filters = [("horizon", "in", sparse_horizons)]
    result = _read_date_chunk(path, day, ["station_no", "minute", "horizon"], filters=filters)
    assert sorted(result["horizon"].tolist()) == sparse_horizons


def test_adaptive_anchor_mask_supports_various_grid_ticks():
    """5·10·15·20·30·60분 다양한 grid 간격에서 피크 시간대 앵커가 온전히 유지되는지 검증한다."""
    from training.lazy_train_dataset import _adaptive_anchor_mask

    # 1) 30분 grid: 피크 07~21시의 :30 앵커가 20분 배수 검사로 버려지지 않고 모두 유지돼야 함
    minutes_30 = pd.Series(range(0, 1440, 30))  # 48개
    mask_30 = _adaptive_anchor_mask(
        minutes_30,
        is_night_day=True,
        is_holiday=False,
        peak_tick_minutes=30,
    )
    selected_30 = minutes_30[mask_30].tolist()
    # 심야(00~06): 6개 (0, 60, 120, 180, 240, 300) - 30분 단위는 탈락
    # 평시(06시): 1개 (360) - 390 탈락
    # 피크(07~21시): 14시간 * 2 = 28개 (420, 450, 480, 510, ..., 1230)
    # 평시(21~24시): 3개 (1260, 1320, 1380) - 1290, 1350, 1410 탈락
    # 합계: 6 + 1 + 28 + 3 = 38개
    assert len(selected_30) == 38
    assert 420 in selected_30 and 450 in selected_30  # 07:00, 07:30 유지
    assert 480 in selected_30 and 510 in selected_30  # 08:00, 08:30 유지
    assert 30 not in selected_30  # 심야 00:30 탈락
    assert 1290 not in selected_30  # 평시 21:30 탈락

    # 2) 15분 grid: 피크 07~21시의 :15, :30, :45 앵커가 모두 유지돼야 함
    minutes_15 = pd.Series(range(0, 1440, 15))  # 96개
    mask_15 = _adaptive_anchor_mask(
        minutes_15,
        is_night_day=True,
        is_holiday=False,
        peak_tick_minutes=15,
    )
    selected_15 = minutes_15[mask_15].tolist()
    # 심야: 6개 (60분 정시)
    # 평시(06시): 1개 (60분 정시)
    # 피크(07~21시): 14시간 * 4 = 56개 (15분 단위 전체)
    # 평시(21~24시): 3개 (60분 정시)
    # 합계: 6 + 1 + 56 + 3 = 66개
    assert len(selected_15) == 66
    assert 420 in selected_15 and 435 in selected_15 and 450 in selected_15 and 465 in selected_15

    # 3) 5분 grid: 피크 시간대에 5분 단위 전체(12개/시간) 유지
    minutes_5 = pd.Series(range(0, 1440, 5))  # 288개
    mask_5 = _adaptive_anchor_mask(
        minutes_5,
        is_night_day=False,
        is_holiday=False,
        peak_tick_minutes=5,
    )
    selected_5 = minutes_5[mask_5].tolist()
    # 비심야일: 심야 0개, 평시 4개(06, 21, 22, 23시), 피크 14시간 * 12 = 168개
    # 합계: 172개
    assert len(selected_5) == 172
    assert 420 in selected_5 and 425 in selected_5 and 430 in selected_5

    # 4) 60분 grid: 피크 시간대와 평시 모두 60분 정시
    minutes_60 = pd.Series(range(0, 1440, 60))  # 24개
    mask_60 = _adaptive_anchor_mask(
        minutes_60,
        is_night_day=True,
        is_holiday=False,
        peak_tick_minutes=60,
    )
    selected_60 = minutes_60[mask_60].tolist()
    assert len(selected_60) == 24


def test_is_holiday_date_identifies_weekdays_and_korean_holidays():
    """평일 대한민국 공휴일(신정, 어린이날, 광복절 등)과 주말을 정확히 판별한다."""
    from datetime import date
    from training.lazy_train_dataset import _is_holiday_date

    # 1) 평일 공휴일 (수요일 신정, 월요일 어린이날, 금요일 광복절)
    assert _is_holiday_date(date(2025, 1, 1)) is True
    assert _is_holiday_date(date(2025, 5, 5)) is True
    assert _is_holiday_date(date(2025, 8, 15)) is True

    # 2) 일반 평일 (목요일, 금요일)
    assert _is_holiday_date(date(2025, 1, 2)) is False
    assert _is_holiday_date(date(2025, 1, 3)) is False

    # 3) 주말 (토요일, 일요일)
    assert _is_holiday_date(date(2025, 1, 4)) is True
    assert _is_holiday_date(date(2025, 1, 5)) is True


def test_apply_adaptive_anchor_filter_applies_holiday_peak_hours_on_weekday_holiday():
    """평일 공휴일(2025-01-01 수요일)에 평일 피크(07시~) 대신 휴일 피크(08시~) 규칙이 적용되는지 검증한다."""
    from training.lazy_train_dataset import _apply_adaptive_anchor_filter

    day = "2025-01-01"  # 수요일 신정 (공휴일 + 3일 주기 심야 대상일)
    minutes = list(range(0, 1440, 20))  # 72행
    df = pd.DataFrame({
        "station_no": [1] * len(minutes),
        "minute": minutes,
        "x1": [1.0] * len(minutes),
    })

    result = _apply_adaptive_anchor_filter(df, day)
    selected_minutes = result["minute"].tolist()

    # 휴일 피크(08~21시)이므로 07시는 비피크 평시 -> 07:00(420)만 남고 07:20(440), 07:40(460) 탈락
    assert 420 in selected_minutes
    assert 440 not in selected_minutes
    assert 460 not in selected_minutes

    # 08시는 피크 시간 -> 08:00(480), 08:20(500), 08:40(520) 모두 유지
    assert 480 in selected_minutes
    assert 500 in selected_minutes
    assert 520 in selected_minutes

    # 휴일 심야 대상일 총 50행 (평일 심야 52행과 구별)
    assert len(selected_minutes) == 50


