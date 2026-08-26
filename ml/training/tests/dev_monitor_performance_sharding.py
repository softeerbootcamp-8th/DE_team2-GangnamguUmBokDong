"""분산 평가(`evaluate_recent_performance_shard`/`combine_evaluation_shards`)가
단일 프로세스 평가(`evaluate_recent_performance`)와 수학적으로 정확히 같은
결과를 내는지 검증한다 — poisson_deviance/RMSE/coverage가 전부 "행 단위 값의
평균"이라 부분합만 있으면 나중에 합쳐도 근사가 아니라 완전히 동일해야 한다
(`combine_evaluation_shards()` docstring 참고).
"""

import numpy as np
import pandas as pd
import pytest
from core import s3 as s3_io
from ml_core import common_config, scoring
from ml_core.day_index import day_index
from ml_core.model_contract import RETURN_FEATURE_COLUMNS
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer

from training import config
from training.monitor_performance import (
    _recent_month_range,
    _split_date_range,
    combine_evaluation_shards,
    evaluate_recent_performance,
    evaluate_recent_performance_shard,
)


def test_split_date_range_covers_every_day_exactly_once():
    shards = _split_date_range("2026-08-01", "2026-08-10", num_shards=3)
    assert len(shards) == 3
    all_days = []
    for start, end in shards:
        all_days.extend(pd.date_range(start, end, freq="D").strftime("%Y-%m-%d").tolist())
    assert all_days == pd.date_range("2026-08-01", "2026-08-10", freq="D").strftime("%Y-%m-%d").tolist()


def test_split_date_range_more_shards_than_days_gives_some_empty_ranges():
    """조각 수가 날짜 수보다 많으면 뒤쪽 워커는 담당 구간이 None(할 일 없음) —
    에러가 아니다."""
    shards = _split_date_range("2026-08-01", "2026-08-02", num_shards=5)
    assert len(shards) == 5
    non_empty = [s for s in shards if s is not None]
    assert sum(len(pd.date_range(s[0], s[1], freq="D")) for s in non_empty) == 2
    assert sum(1 for s in shards if s is None) == 3


def test_evaluate_recent_performance_shard_returns_zero_for_none_range():
    shard = evaluate_recent_performance_shard("return", "return_count", None, None, horizon=1)
    assert shard == {"n_rows": 0, "sum_deviance_term": 0.0, "sum_sq_err": 0.0, "sum_coverage_hits": 0.0}


class _FakeBooster:
    def __init__(self, value: float = 3.0):
        self._value = value

    def predict(self, X, num_iteration=None):
        return np.full(len(X), self._value)


def test_sharded_evaluation_matches_single_shot_evaluation(monkeypatch):
    """같은 데이터를 (a) 한 번에 평가한 결과와 (b) 여러 조각으로 나눠 평가한 뒤
    합친 결과가 완전히 동일해야 한다(부동소수점 오차 범위 내)."""
    monkeypatch.setattr(scoring, "load_boosters", lambda model_name: {
        "poisson": _FakeBooster(3.0), "q10": _FakeBooster(1.0), "q50": _FakeBooster(3.0), "q90": _FakeBooster(5.0),
    })
    monkeypatch.setattr(scoring, "load_conformal_correction", lambda model_name: 0.0)
    monkeypatch.setattr(scoring, "load_station_dtype", lambda model_name: pd.CategoricalDtype(categories=[1]))
    read_champion_prefix.cache_clear()

    as_of = "2026-08-17"
    window_start, _window_end = _recent_month_range(1, as_of=as_of)
    dates = pd.date_range(window_start, periods=5, freq="D").strftime("%Y-%m-%d").tolist()

    table_path = config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    for d_idx, d in enumerate(dates):
        rows = [
            {
                "station_no": 1, "capacity": 10, "lat": 37.5, "lon": 127.0, "temp": 20.0, "precip": 0.0,
                "pop_total": 1000.0, "minute": 0, "dow": 0, "is_holiday": 0,
                "day": day_index(pd.Timestamp(d).date()), "horizon": 1,
                "return_lag_1h": 3.0 + i, "return_count": 5 + i + d_idx, "hour": 0,
            }
            for i in range(4)
        ]
        assert set(RETURN_FEATURE_COLUMNS) <= set(rows[0].keys())
        s3_io.write_parquet(pd.DataFrame(rows), f"{table_path}/date={d}/part-0000.parquet")

    write_champion_pointer("return", "models/archive/dt=test-shard/default")
    s3_io.write_json(
        model_json_key("return", "profile", "models/archive/dt=test-shard/default"),
        common_config.effective_profile(),
    )
    baseline = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.8, "rmse_test": 2.0}
    s3_io.write_json(
        model_json_key("return", "metrics", "models/archive/dt=test-shard/default"), baseline,
    )

    whole = evaluate_recent_performance("return", "return_count", None, lookback_months=1, as_of=as_of)

    period = (dates[0], dates[-1])
    shard_ranges = _split_date_range(*period, num_shards=3)
    shards = [
        evaluate_recent_performance_shard("return", "return_count", None, shard_range, horizon=1)
        for shard_range in shard_ranges
    ]
    combined = combine_evaluation_shards("return", period, shards, baseline)

    assert combined["n_rows"] == whole["n_rows"] == 20
    assert combined["current_deviance"] == pytest.approx(whole["current_deviance"])
    assert combined["current_rmse"] == pytest.approx(whole["current_rmse"])
    assert combined["current_coverage"] == pytest.approx(whole["current_coverage"])
    assert combined["deviance_relative_change"] == pytest.approx(whole["deviance_relative_change"])
    assert combined["coverage_drift"] == pytest.approx(whole["coverage_drift"])


def test_combine_evaluation_shards_raises_when_all_shards_empty():
    baseline = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.8, "rmse_test": 2.0}
    empty_shard = {"n_rows": 0, "sum_deviance_term": 0.0, "sum_sq_err": 0.0, "sum_coverage_hits": 0.0}
    with pytest.raises(ValueError, match="데이터가 없음"):
        combine_evaluation_shards("return", ("2026-08-01", "2026-08-02"), [empty_shard, empty_shard], baseline)
