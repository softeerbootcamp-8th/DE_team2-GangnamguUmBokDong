"""S3에서 재고 이력과 예측 결과를 읽어온다.

loader/reader.py와 같은 이유로 libs/ml_core를 런타임에 import하지 않는다(무거운
lightgbm 등을 배포 환경에 끌고 오지 않기 위해) — S3 키 규칙은 이 파일이 자체
복제한다. 키 포맷이 libs/ml_core.silver_schema와 갈라지지 않는지는
tests/test_reader_key_contract.py(dev 전용 ml_core 의존)가 검증한다.
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pandas as pd
from core.s3 import read_parquet

_BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
_BIKE_REALTIME_TICK_MINUTES = 5


def _silver_key(source_id: str, window_start: datetime) -> str:
    return f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{window_start:%H%M}.parquet"


def _predictions_key(window_start: datetime) -> str:
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def _floor_to_tick(dt: datetime, tick_minutes: int) -> datetime:
    return dt - timedelta(minutes=dt.minute % tick_minutes, seconds=dt.second, microseconds=dt.microsecond)


def _bike_realtime_tick_keys(anchor: datetime, lookback_minutes: int) -> list[tuple[datetime, str]]:
    """anchor부터 과거 lookback_minutes 동안의 5분 tick (시각, 키) 목록(오래된 것부터 최신 순)."""
    floored = _floor_to_tick(anchor, _BIKE_REALTIME_TICK_MINUTES)
    n_ticks = lookback_minutes // _BIKE_REALTIME_TICK_MINUTES + 1
    ticks = [floored - timedelta(minutes=_BIKE_REALTIME_TICK_MINUTES * i) for i in range(n_ticks - 1, -1, -1)]
    return [(t, _silver_key(_BIKE_REALTIME_SOURCE_ID, t)) for t in ticks]


def read_recent_stock(anchor: datetime, lookback_minutes: int = 25) -> dict[str, list[dict]]:
    """최근 lookback_minutes 동안의 5분 tick 재고 이력을 대여소별로 묶어서 반환한다.

    각 포인트는 {"observed_at", "parking_bike_tot_cnt", "hold_cnt"}를 담는다 —
    urgency.urgency_score의 stock_history 인자(과거 apps/api/queries.py의
    fetch_all_stock_history와 동일한 형태)로 바로 쓸 수 있고, hold_cnt는 가장 최근
    포인트에서 "현재 재고/정원"을 뽑아 쓰는 용도다(rebalance/urgency.py 참고). 같은
    tick 파일(bike_station_realtime)에 재고(parkingBikeTotCnt)와 정원(rackTotCnt)이
    함께 있어 별도 RDS 조회가 필요 없다.
    """
    history: dict[str, list[dict]] = {}
    for observed_at, key in _bike_realtime_tick_keys(anchor, lookback_minutes):
        df = read_parquet(key, columns=["stationId", "parkingBikeTotCnt", "rackTotCnt"])
        if df is None or df.empty:
            continue
        for row in df.itertuples(index=False):
            history.setdefault(str(row.stationId), []).append(
                {
                    "observed_at": observed_at,
                    "parking_bike_tot_cnt": int(row.parkingBikeTotCnt),
                    "hold_cnt": int(row.rackTotCnt),
                }
            )
    return history


def read_predictions(window_start: datetime) -> pd.DataFrame:
    """예측 배치 결과를 읽고, 해당 anchor의 산출물이 없으면 실패한다.

    compute_urgency는 run_inference의 직접 downstream이므로 파일 부재는 정상적인
    trend-only 입력이 아니라 upstream 산출물 계약 위반이다.
    """
    key = _predictions_key(window_start)
    predictions = read_parquet(key)
    if predictions is None:
        raise FileNotFoundError(f"prediction parquet not found for {window_start}: {key}")
    return predictions
