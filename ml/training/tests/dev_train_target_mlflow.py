"""train_target()이 MLflow run을 항상 정확히 종료하는지 검증한다.

`with mlflow.start_run(...)` 도입 전에는 성공 경로 끝에서만 `mlflow.end_run()`을
불러서, `lgb.train()`/S3 업로드/평가 도중 예외가 나면 run이 RUNNING 상태로
영구 방치됐다(train_common.py의 train_target() 참고) — 이 테스트는 정상 종료 시
FINISHED, 예외 발생 시 FAILED로 항상 닫히는지 확인한다. 실제 MLflow 서버 대신
로컬 파일 기반 tracking store(tmp_path)를 쓴다.

**2026-08 전면 개편**: `train_target()`이 더 이상 `df`를 인자로 안 받는다 —
`lazy_train_dataset`을 통해 S3에서 직접 날짜 파티션 단위로 읽으므로, 이 테스트도
in-memory df 대신 moto S3에 multi-horizon 테이블을 직접 심는다(`core.s3.write_parquet`,
`conftest.py`의 `_bucket` autouse fixture가 이미 목킹된 S3를 준비해둠).
"""

from datetime import date

import mlflow
import pandas as pd
import pytest
from core import s3 as s3_io
from ml_core.day_index import day_index

from training import config, train_common
from training.train_common import train_target


@pytest.fixture(autouse=True)
def _local_mlflow(tmp_path, monkeypatch):
    # mlflow 3.x는 파일시스템 backend(./mlruns)를 기본으로 막는다(sqlalchemy 기반
    # backend로의 이관을 유도) — sqlalchemy를 테스트 전용으로 추가하고 싶지 않아
    # (training은 의도적으로 mlflow-skinny만 씀) 이 플래그로 로컬 파일 backend를 쓴다.
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setattr(train_common.mlflow_tracking, "MLFLOW_TRACKING_URI", str(tmp_path / "mlruns"))
    monkeypatch.setattr(config, "TRAIN_YEAR", 2025)
    monkeypatch.setattr(config, "safety_cutoff_date", lambda as_of=None: date(2025, 12, 31))
    monkeypatch.setattr(config, "LGB_NUM_BOOST_ROUND", 3)
    monkeypatch.setattr(config, "LGB_EARLY_STOPPING_ROUNDS", 3)


def _seed_return_table(n_each: int = 8) -> None:
    """train(2일)/valid(11일)/test(17일) 기본값에 맞춰 반납 모델용 multi-horizon
    테이블을 moto S3에 날짜 파티션으로 심는다(VALID_DAYS_OF_MONTH/TEST_DAYS_OF_MONTH
    기본값 {11,13}/{17,19}, TRAIN_DAY_DIVISOR 기본값 1을 그대로 씀)."""
    table_path = config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET
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
                "return_lag_1h": 3.0 + i,
                "return_count": 5 + i,
            }
            for i in range(n_each)
        ]
        s3_io.write_parquet(pd.DataFrame(rows), f"{table_path}/date={date_str}/part-0000.parquet")


def _latest_run(experiment_name: str):
    client = mlflow.tracking.MlflowClient()
    exp = client.get_experiment_by_name(experiment_name)
    return client.search_runs([exp.experiment_id], max_results=1)[0]


def test_train_target_ends_run_as_finished_on_success():
    _seed_return_table()

    metrics = train_target("return_count", "return", models_prefix="models/test/finished")

    assert "rmse_test" in metrics
    run = _latest_run(config.MLFLOW_EXPERIMENT_NAME)
    assert run.info.status == "FINISHED"
    assert run.data.params["train_day_divisor"] == str(config.TRAIN_DAY_DIVISOR)
    assert "learning_rate" in run.data.params  # LGB_PARAMS_COMMON이 로깅됐는지
    assert "rmse_test" in run.data.metrics
    assert mlflow.active_run() is None


def test_train_target_ends_run_as_failed_on_exception(monkeypatch):
    _seed_return_table()
    monkeypatch.setattr(
        train_common, "_conformal_correction", lambda *a, **kw: (_ for _ in ()).throw(RuntimeError("boom"))
    )

    with pytest.raises(RuntimeError, match="boom"):
        train_target("return_count", "return", models_prefix="models/test/failed")

    run = _latest_run(config.MLFLOW_EXPERIMENT_NAME)
    assert run.info.status == "FAILED"
    assert mlflow.active_run() is None
