"""quantile(P10/50/90) 학습이 poisson의 exposure offset(init_score)을 물려받지 않는지 검증한다.

`train_common.train_target()`은 메모리 절감을 위해 exposure_col이 없는 모델(반납)은
poisson용 `train_set`/`valid_set`을 quantile 학습에도 그대로 재사용한다 — LightGBM
Dataset은 objective와 무관한 데이터 컨테이너라 이 재사용 자체는 안전하다(직접 검증:
byte-identical 결과).

하지만 exposure_col이 있는 모델(대여)의 `train_set`/`valid_set`은 구성 시점에 이미
`init_score=log(exposure)`가 박혀 있고, `Dataset.set_init_score(None)`으로도 지워지지
않는다는 걸 직접 확인했다 — 만약 이 경우까지 재사용하면 quantile 예측이 그 offset을
몰래 물려받아 전부 0 근처로 붕괴한다(exp(음수 log-exposure)만큼 아래로 눌림). 이 테스트는
대여 모델처럼 exposure가 1보다 뚜렷이 작은 데이터로 학습했을 때 quantile 예측이 실제
라벨 규모와 동떨어지지 않는지 확인해 이 회귀를 잡는다.

**2026-08 전면 개편**: `train_target()`이 더 이상 `df`를 인자로 안 받는다 —
moto S3에 multi-horizon 테이블을 직접 심는다(`dev_train_target_mlflow.py`와 같은 방식).
"""

import weakref
from datetime import date

import pandas as pd
import pytest
from core import s3 as s3_io
from ml_core.day_index import day_index

from training import config
from training.train_common import train_target


@pytest.fixture(autouse=True)
def _local_mlflow(tmp_path, monkeypatch):
    from training import train_common

    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setattr(train_common.mlflow_tracking, "MLFLOW_TRACKING_URI", str(tmp_path / "mlruns"))


def _seed_rental_table(n_each: int = 8) -> None:
    """train(2일)/valid(11일)/test(17일)에 exposure가 뚜렷이 1보다 작은 rental 데이터."""
    table_path = config.RENTAL_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    for date_str in ("2025-01-02", "2025-01-11", "2025-01-17"):
        day_of_month = int(date_str[-2:])
        rows = [
            {
                "station_no": 1,
                "capacity": 10,
                "lat": 37.5,
                "lon": 127.0,
                "temp": 20.0,
                "precip": 0.0,
                "pop_total": 1000.0,
                "minute": 0,
                "dow": 0,
                "is_holiday": 0,
                "day": day_index(date(2025, 1, day_of_month)),
                "horizon": 1,
                "rental_lag_1h": 3.0 + i,
                "rental_exposure": 0.2,  # log(0.2) ~= -1.6 — offset이 새면 예측이 뚜렷이 눌림
                "rental_count": 8 + i,
            }
            for i in range(n_each)
        ]
        s3_io.write_parquet(pd.DataFrame(rows), f"{table_path}/date={date_str}/part-0000.parquet")


def test_rental_quantile_predictions_are_not_contaminated_by_exposure_offset(monkeypatch):
    from training import train_common

    monkeypatch.setattr(config, "TRAIN_WINDOW_START", date(2025, 1, 1))
    monkeypatch.setattr(config, "TRAIN_WINDOW_END", date(2025, 12, 31))
    monkeypatch.setattr(config, "LGB_NUM_BOOST_ROUND", 5)
    monkeypatch.setattr(config, "LGB_EARLY_STOPPING_ROUNDS", 5)

    original_build = train_common.lazy_train_dataset.build_lazy_dataset
    poisson_dataset_refs: list[weakref.ReferenceType] = []
    build_count = 0

    def _tracking_build(*args, **kwargs):
        """quantile construct 전에 poisson train/valid Dataset이 실제 해제됐는지 확인한다."""
        nonlocal build_count
        if build_count == 2:
            assert all(ref() is None for ref in poisson_dataset_refs)
        result = original_build(*args, **kwargs)
        if build_count < 2:
            poisson_dataset_refs.append(weakref.ref(result[0]))
        build_count += 1
        return result

    monkeypatch.setattr(train_common.lazy_train_dataset, "build_lazy_dataset", _tracking_build)

    _seed_rental_table()
    metrics = train_target(
        "rental_count", "rental", models_prefix="models/test/rental-quantile-offset", exposure_col="rental_exposure"
    )

    # rental_count 라벨은 8~15 범위 — quantile offset이 새면(exp(log(0.2)) 배로 눌림)
    # P10~P90 커버리지가 사실상 0에 가깝게 무너진다. 라벨 규모를 못 맞추더라도(작은
    # 합성 데이터라 학습이 완벽하진 않음) 완전 붕괴는 아니어야 한다.
    assert metrics["p10_p90_coverage_raw_test"] > 0.0
    assert build_count == 4
