"""LGB_NUM_MACHINES>1일 때 train_target()이 station_shard를 실제로 계산해
lazy_train_dataset의 모든 호출부에 넘기는지 검증한다.

**실제 다중 머신 소켓 핸드셰이크는 여기서 검증하지 않는다** — `LGB_TREE_LEARNER`가
"serial"이면 `_distributed_params()`가 빈 dict를 반환해(`train_common.py` 참고)
`lgb.train()`은 평소처럼 단일 프로세스로 동작한다. `LGB_NUM_MACHINES`만 1보다 크게
주면 station 샤딩 계산·전달 경로만 분리해서 실제 소켓 없이 검증할 수 있다 —
진짜 다중 머신 학습은 ADR-0005/0007에 문서화된 대로 실제 인프라 없이는
End-to-End 검증이 불가능하다.
"""

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


#  crc32(station_id)%2로 실제 확인한 값 — station 1/2/3은 rank 1, station 4는 rank
# 0으로 떨어진다(`_shard_for_this_machine()`이 이 해시를 그대로 쓰므로 결정적).
# 두 rank 모두 최소 하나의 station을 갖도록 4개를 심는다.
_STATION_NOS = (1, 2, 3, 4)


def _seed_two_station_return_table(n_each: int = 8) -> None:
    """train(1일)/valid(3일)/test(10일)에 station_no 4개를 심는다 — 분산 학습
    샤딩이 실제로 두 rank 모두에 데이터를 나눠주는지 보려면 station이 여러 개
    필요하다(station 1개뿐이면 한쪽 rank가 통째로 빈 샤드를 받을 수 있음)."""
    table_path = config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    for date_str in ("2025-01-01", "2025-01-03", "2025-01-10"):
        day_of_month = int(date_str[-2:])
        rows = [
            {
                "station_no": station_no,
                "capacity": 10,
                "lat": 37.5,
                "lon": 127.0,
                "temp": 20.0,
                "precip": 0.0,
                "pop_total": 1000.0,
                "minute": 480,
                "dow": 0,
                "is_holiday": 0,
                "day": day_index(date(2025, 1, day_of_month)),
                "horizon": 1,
                "return_lag_1h": 3.0 + i,
                "return_count": 5 + i,
            }
            for station_no in _STATION_NOS
            for i in range(n_each)
        ]
        s3_io.write_parquet(pd.DataFrame(rows), f"{table_path}/date={date_str}/part-0000.parquet")


def test_train_target_computes_and_threads_station_shard_when_num_machines_over_one(monkeypatch):
    from training import train_common

    monkeypatch.setattr(config, "TRAIN_WINDOW_START", date(2025, 1, 1))
    monkeypatch.setattr(config, "TRAIN_WINDOW_END", date(2025, 12, 31))
    monkeypatch.setattr(config, "LGB_NUM_BOOST_ROUND", 5)
    monkeypatch.setattr(config, "LGB_EARLY_STOPPING_ROUNDS", 5)
    monkeypatch.setattr(config, "LGB_NUM_MACHINES", 2)
    monkeypatch.setattr(config, "LGB_MACHINE_RANK", 0)

    seen_shards: list[frozenset | None] = []
    original_build = train_common.lazy_train_dataset.build_lazy_dataset
    original_predict = train_common.lazy_train_dataset.predict_over_dates

    def _tracking_build(*args, **kwargs):
        seen_shards.append(kwargs.get("station_shard"))
        return original_build(*args, **kwargs)

    def _tracking_predict(*args, **kwargs):
        seen_shards.append(kwargs.get("station_shard"))
        return original_predict(*args, **kwargs)

    monkeypatch.setattr(train_common.lazy_train_dataset, "build_lazy_dataset", _tracking_build)
    monkeypatch.setattr(train_common.lazy_train_dataset, "predict_over_dates", _tracking_predict)

    _seed_two_station_return_table()
    metrics = train_target("return_count", "return", models_prefix="models/test/return-distributed", exposure_col=None)

    # station_categories_for_dates()는 이 샤딩과 무관하게 항상 station 1·2 전체를 봐야
    # 한다 — station_dtype이 두 station 모두를 알고 있어야 카테고리 코드가 rank와
    # 무관하게 고정된다(같은 station_dtype 값이 build_lazy_dataset 호출마다 그대로
    # 전달됐는지는 train_target 내부에서 이미 고정된 변수를 재사용하므로 여기서는
    # station_shard 쪽만 확인한다).
    assert seen_shards, "build_lazy_dataset/predict_over_dates가 한 번도 안 불림"
    assert all(shard is not None for shard in seen_shards), "LGB_NUM_MACHINES>1인데 station_shard가 None으로 전달됨"
    # rank 0의 샤드는 전체 station의 부분집합이어야 하고, 모든 호출이 같은 샤드를 봐야 한다.
    assert all(shard == seen_shards[0] for shard in seen_shards)
    assert seen_shards[0] <= set(_STATION_NOS)
    assert seen_shards[0], "rank 0 샤드가 비어 있음 — _STATION_NOS 구성을 다시 확인할 것"
    assert metrics["model_name"] == "return"


def test_train_target_passes_no_station_shard_when_num_machines_is_one(monkeypatch):
    """기존 단일 머신 동작(LGB_NUM_MACHINES=1, 기본값)은 station_shard=None을 그대로
    유지해야 한다 — 분산 학습 배선이 기본 경로에 회귀를 만들면 안 된다."""
    from training import train_common

    monkeypatch.setattr(config, "TRAIN_WINDOW_START", date(2025, 1, 1))
    monkeypatch.setattr(config, "TRAIN_WINDOW_END", date(2025, 12, 31))
    monkeypatch.setattr(config, "LGB_NUM_BOOST_ROUND", 5)
    monkeypatch.setattr(config, "LGB_EARLY_STOPPING_ROUNDS", 5)
    assert config.LGB_NUM_MACHINES == 1  # 기본값 확인(다른 테스트가 monkeypatch로 바꿔뒀다면 이 테스트가 걸러냄)

    seen_shards: list[frozenset | None] = []
    original_build = train_common.lazy_train_dataset.build_lazy_dataset

    def _tracking_build(*args, **kwargs):
        seen_shards.append(kwargs.get("station_shard"))
        return original_build(*args, **kwargs)

    monkeypatch.setattr(train_common.lazy_train_dataset, "build_lazy_dataset", _tracking_build)

    _seed_two_station_return_table()
    train_target("return_count", "return", models_prefix="models/test/return-serial", exposure_col=None)

    assert seen_shards and all(shard is None for shard in seen_shards)
