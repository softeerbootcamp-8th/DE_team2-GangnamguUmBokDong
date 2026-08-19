"""urgency.compute_all()의 S3 읽기 -> 계산 -> DataFrame 조립까지의 통합 동작을
moto로 S3를 목킹해 검증한다. 개별 계산 함수 자체는 test_urgency.py(순수 함수
단위 테스트)가 이미 다루므로, 여기서는 reader.py가 실제로 올바른 S3 키에서
데이터를 읽어와 urgency_score까지 정확히 이어지는지만 확인한다.
"""

import io
from datetime import datetime

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

import urgency
from tests.conftest import TEST_BUCKET
from urgency import compute_all

# ml/inference의 _target_timestamp와 같은 이유로 pd.Timestamp를 쓴다(naive
# datetime을 datetime.strptime/datetime()으로 직접 만들면 tzinfo 누락으로
# 오해되기 쉬움 — reader.py/main.py도 실제로는 이렇게 naive KST 벽시계를 쓴다).
ANCHOR = pd.Timestamp(2026, 8, 16, 14, 5)

_GANGNAM_LAT, _GANGNAM_LON = 37.5172, 127.0473

# _known_station_ids()는 RDS(stations 테이블)를 실제로 조회한다. 이 파일의
# 테스트는 S3(moto)만 다루므로, 그 필터 자체를 검증하는
# test_compute_all_excludes_stations_missing_from_stations_table 외에는 전부
# 통과시키도록 기본값을 넉넉하게 스텁한다.
_ALL_TEST_STATION_IDS = {"101", "102", "999"}


@pytest.fixture(autouse=True)
def _stub_known_station_ids(monkeypatch):
    monkeypatch.setattr(urgency, "_known_station_ids", lambda: _ALL_TEST_STATION_IDS)


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    boto3.client("s3", region_name="us-east-1").put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


def _put_tick(window_start: datetime, rows: list[dict]) -> None:
    key = f"silver/bike_station_realtime/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{window_start:%H%M}.parquet"
    _put_parquet(key, pa.Table.from_pylist(rows))


def _tick_row(sta_id: str, current: int, hold_cnt: int) -> dict:
    return {
        "stationId": sta_id,
        "parkingBikeTotCnt": current,
        "rackTotCnt": hold_cnt,
        "stationLatitude": _GANGNAM_LAT,
        "stationLongitude": _GANGNAM_LON,
    }


def _put_predictions(station_ids: list[str]) -> None:
    key = f"predictions/dt={ANCHOR:%Y-%m-%d}/hh={ANCHOR:%H}/inference_{ANCHOR:%H%M}.parquet"
    rows = [
        {
            "station_id": station_id,
            "date": "2026-08-16",
            "hour": 15,
            "minute": 5,
            "horizon": 1,
            "rental_pred_mean": 0.0,
            "return_pred_mean": 0.0,
        }
        for station_id in station_ids
    ]
    _put_parquet(key, pa.Table.from_pylist(rows))


def test_compute_all_detects_declining_trend_with_prediction_artifact():
    # 14:00에 10대 -> 14:05(anchor)에 5대. hold_cnt(rackTotCnt)=10.
    _put_tick(pd.Timestamp(2026, 8, 16, 14, 0), [_tick_row("101", current=10, hold_cnt=10)])
    _put_tick(ANCHOR, [_tick_row("101", current=5, hold_cnt=10)])
    _put_predictions(["101"])

    result = compute_all(ANCHOR)

    assert list(result.columns) == [
        "sta_id",
        "lat",
        "lon",
        "urgency_score",
        "minutes_until_critical",
        "action_type",
        "bike_qty",
    ]
    [row] = result.to_dict("records")
    assert row["sta_id"] == "101"
    assert (row["lat"], row["lon"]) == (_GANGNAM_LAT, _GANGNAM_LON)
    # 분당 -1대 추세, 현재 5대 -> 0석까지 5분. 예측 없어(0) severity 0, bike_qty도 0.
    assert (row["urgency_score"], row["minutes_until_critical"], row["action_type"], row["bike_qty"]) == (
        0.0,
        5,
        "supply_needed",
        0,
    )


def test_compute_all_combines_stock_history_with_predictions():
    _put_tick(pd.Timestamp(2026, 8, 16, 14, 0), [_tick_row("101", current=5, hold_cnt=10)])
    _put_tick(ANCHOR, [_tick_row("101", current=5, hold_cnt=10)])

    predictions_key = f"predictions/dt={ANCHOR:%Y-%m-%d}/hh={ANCHOR:%H}/inference_{ANCHOR:%H%M}.parquet"
    predictions = pd.DataFrame(
        [
            {
                "station_id": "101",
                "date": "2026-08-16",
                "hour": 15,
                "minute": 5,
                "horizon": 1,
                "rental_pred_mean": 1.0,
                "return_pred_mean": 0.0,
            },
            {
                "station_id": "101",
                "date": "2026-08-16",
                "hour": 16,
                "minute": 5,
                "horizon": 2,
                "rental_pred_mean": 8.0,
                "return_pred_mean": 0.0,
            },
        ]
    )
    _put_parquet(predictions_key, pa.Table.from_pandas(predictions, preserve_index=False))

    result = compute_all(ANCHOR)

    [row] = result.to_dict("records")
    # 재고 추세는 평탄(0)이라 트렌드 감지 없음 -> 예측 감지로 2번째(index 1)
    # 지점에서 처음 supply_needed -> (1+1)*60=120분 뒤. severity는 _max_deficit(현재
    # 5 -> 1건 대여로 4 -> 8건 대여로 -4, 최대 부족량 4) 기준 ratio=4/10, score=8.3.
    # bike_qty도 같은 부족량(4)이지만 빈 거치대 수(hold_cnt-current=5)보다 작아
    # 클램프 없이 그대로 4.
    assert row["sta_id"] == "101"
    assert (row["urgency_score"], row["minutes_until_critical"], row["action_type"], row["bike_qty"]) == (
        8.3,
        120,
        "supply_needed",
        4,
    )


def test_compute_all_requires_prediction_parquet():
    _put_tick(ANCHOR, [_tick_row("101", current=5, hold_cnt=10)])

    with pytest.raises(FileNotFoundError, match="prediction parquet not found"):
        compute_all(ANCHOR)


def test_compute_all_excludes_and_logs_station_without_model_prediction(caplog):
    _put_tick(
        ANCHOR,
        [
            _tick_row("101", current=5, hold_cnt=10),
            _tick_row("102", current=5, hold_cnt=10),
        ],
    )
    _put_predictions(["101"])

    result = compute_all(ANCHOR)

    assert result["sta_id"].tolist() == ["101"]
    assert "excluding 1 current-stock stations without model predictions" in caplog.text


def test_compute_all_requires_anchor_stock_snapshot():
    _put_tick(pd.Timestamp(2026, 8, 16, 14, 0), [_tick_row("101", current=5, hold_cnt=10)])
    _put_predictions(["101"])

    with pytest.raises(FileNotFoundError, match="stock snapshot parquet not found"):
        compute_all(ANCHOR)


def test_compute_all_rejects_anchor_outside_five_minute_grid():
    with pytest.raises(ValueError, match="anchor must align to a 5-minute tick"):
        compute_all(pd.Timestamp(2026, 8, 16, 14, 33))


@pytest.mark.parametrize(
    ("last_observed_at", "included"),
    [
        (pd.Timestamp(2026, 8, 16, 14, 5), True),
        (pd.Timestamp(2026, 8, 16, 14, 0), False),
        (pd.Timestamp(2026, 8, 16, 13, 45), False),
    ],
)
def test_compute_all_uses_only_anchor_stock_snapshot(last_observed_at, included):
    _put_tick(last_observed_at, [_tick_row("101", current=5, hold_cnt=10)])
    prediction_station_ids = ["101"]
    if last_observed_at != ANCHOR:
        # anchor 파일 자체는 존재하지만 101이 빠진 상황으로 station freshness를 검증한다.
        _put_tick(ANCHOR, [_tick_row("999", current=5, hold_cnt=10)])
        prediction_station_ids.append("999")
    _put_predictions(prediction_station_ids)

    result = compute_all(ANCHOR)

    assert ("101" in result["sta_id"].tolist()) is included


def test_compute_all_excludes_stations_missing_nan_coordinates(caplog):
    """수집 스키마에서 위경도는 optional이라 결측이면 NaN으로 들어온다(#114 리뷰).
    NaN 좌표는 nearest_region 거리 계산을 깨뜨리므로 읽는 시점에 걸러야 한다."""
    _put_tick(
        ANCHOR,
        [
            _tick_row("101", current=5, hold_cnt=10),
            {"stationId": "102", "parkingBikeTotCnt": 5, "rackTotCnt": 10, "stationLatitude": None, "stationLongitude": None},
        ],
    )
    _put_predictions(["101", "102"])

    result = compute_all(ANCHOR)

    assert result["sta_id"].tolist() == ["101"]


def test_compute_all_excludes_stations_missing_from_stations_table(monkeypatch, caplog):
    """서울 자치구 경계 밖 좌표는 stations 테이블에 안 들어간다
    (loader/transform.py:stations_from_silver). 그런 대여소가 여기 남으면
    station_urgency/rebalance_route_stops 양쪽 다 FK 위반이 나므로 걸러야 한다."""
    monkeypatch.setattr(urgency, "_known_station_ids", lambda: {"101"})
    _put_tick(
        ANCHOR,
        [
            _tick_row("101", current=5, hold_cnt=10),
            _tick_row("102", current=5, hold_cnt=10),
        ],
    )
    _put_predictions(["101", "102"])

    result = compute_all(ANCHOR)

    assert result["sta_id"].tolist() == ["101"]
    assert "excluding 1 stations not present in stations table" in caplog.text
