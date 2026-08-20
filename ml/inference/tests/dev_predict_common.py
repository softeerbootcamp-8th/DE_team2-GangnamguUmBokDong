"""predict_common.run_predict_cli()가 multi-horizon 테이블을 date_range로 읽는지 검증한다.

**회귀 배경**: `date_range`/`dates` 없이 `s3_io.read_parquet(table_path)`를 부르면
"prefix 전체 나열" 경로를 타는데, 이 경로는 Spark Hive 파티션 컬럼인 "date"를
파일 내용에서 복원해주지 않는다(그 복원은 `_read_parquet_by_dates()`에서만 일어남).
그 상태로 바로 `df[df["date"] >= ...]`를 하면 `KeyError: 'date'`가 난다(리뷰 지적).
`s3_io.read_parquet`을 직접 monkeypatch해서 실제로 `date_range=`를 넘겨 부르는지,
그리고 그 결과로 받은 "date" 컬럼이 있는 df를 그대로 잘 쓰는지 확인한다 — 실제
S3/booster까지 다 갖추는 대신(다른 파일들과 같은 이 패키지의 monkeypatch 컨벤션),
`predict()`/`_load_station_master()`가 의존하는 걸 전부 가짜로 바꾼다.
"""

import pandas as pd
import pytest
from ml_core.model_contract import RETURN_FEATURE_COLUMNS

from inference import predict_common as pc


def _multi_horizon_df() -> pd.DataFrame:
    rows = []
    for i in range(3):
        row = dict.fromkeys(RETURN_FEATURE_COLUMNS, 0)
        row.update({
            "station_no": 1, "day": 1, "horizon": 1, "hour": 8,
            "date": "2025-06-07", "return_count": 5 + i, "return_lag_1h": 3.0,
        })
        rows.append(row)
    return pd.DataFrame(rows)


@pytest.fixture(autouse=True)
def _fakes(monkeypatch):
    monkeypatch.setattr(pc, "_load_station_master", lambda: pd.DataFrame({"station_id": ["ST-1"], "station_no": [1]}))
    monkeypatch.setattr(pc, "predict", lambda df, model_name, exposure_col=None: pd.DataFrame({
        "station_no": df["station_no"].to_numpy(),
        "pred_mean": [3.0] * len(df), "pred_p10": [1.0] * len(df), "pred_p50": [3.0] * len(df), "pred_p90": [5.0] * len(df),
    }))
    monkeypatch.setattr(pc.s3_io, "write_parquet", lambda df, key: None)


def test_run_predict_cli_reads_table_with_date_range(monkeypatch):
    calls = []

    def _fake_read_parquet(key, **kwargs):
        calls.append((key, kwargs))
        if key == pc.STATION_MASTER_PARQUET:
            return pd.DataFrame({"station_id": ["ST-1"], "station_no": ["1"]})
        return _multi_horizon_df()

    monkeypatch.setattr(pc.s3_io, "read_parquet", _fake_read_parquet)
    monkeypatch.setattr(
        "sys.argv", ["predict_return_demand", "--start-date", "2025-06-07", "--end-date", "2025-06-07"]
    )

    preds = pc.run_predict_cli("return", "return_count", None, "out/default.parquet")

    assert len(preds) == 3
    table_calls = [(key, kwargs) for key, kwargs in calls if key != pc.STATION_MASTER_PARQUET]
    assert len(table_calls) == 1
    _key, kwargs = table_calls[0]
    assert kwargs.get("date_range") == ("2025-06-07", "2025-06-07")


def test_run_predict_cli_raises_clear_error_when_table_missing(monkeypatch):
    def _fake_read_parquet(key, **kwargs):
        if key == pc.STATION_MASTER_PARQUET:
            return pd.DataFrame({"station_id": ["ST-1"], "station_no": ["1"]})
        return None

    monkeypatch.setattr(pc.s3_io, "read_parquet", _fake_read_parquet)
    monkeypatch.setattr(
        "sys.argv", ["predict_return_demand", "--start-date", "2025-06-07", "--end-date", "2025-06-07"]
    )

    with pytest.raises(FileNotFoundError):
        pc.run_predict_cli("return", "return_count", None, "out/default.parquet")
