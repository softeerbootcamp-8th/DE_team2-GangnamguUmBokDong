"""S3에서 재고 이력과 예측 결과를 읽어온다.

loader/reader.py와 같은 이유로 libs/ml_core를 런타임에 import하지 않는다(무거운
lightgbm 등을 배포 환경에 끌고 오지 않기 위해) — S3 키 규칙은 이 파일이 자체
복제한다. 키 포맷이 libs/ml_core.silver_schema와 갈라지지 않는지는
tests/test_reader_key_contract.py(dev 전용 ml_core 의존)가 검증한다.
"""

from __future__ import annotations

import math
from datetime import datetime, timedelta

import pandas as pd
from core.s3 import read_parquet

_BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
_BIKE_REALTIME_TICK_MINUTES = 5


def anchor_timestamp(date: str, hour: int, minute: int) -> pd.Timestamp:
    """date+hour+minute(KST 벽시계 시각)을 합쳐 anchor 시각을 만든다. S3 dt=/hh=/HHMM
    파티션 키(추론기·loader와 동일 규칙)를 그대로 여기서도 쓴다. main.py/routes_main.py
    둘 다 쓰는 공용 헬퍼라 reader.py에 둔다. ml/inference의 _target_timestamp와 같은
    이유로 pd.Timestamp를 쓴다(naive datetime을 datetime.strptime으로 직접 만들면
    tzinfo 누락으로 오해되기 쉬움)."""
    return pd.Timestamp(date) + pd.Timedelta(hours=hour, minutes=minute)


def _silver_key(source_id: str, window_start: datetime) -> str:
    return f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/{window_start:%H%M}.parquet"


def _predictions_key(window_start: datetime) -> str:
    return f"predictions/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/inference_{window_start:%H%M}.parquet"


def _urgency_key(window_start: datetime) -> str:
    """main.py(compute_urgency)가 쓰는 것과 같은 키 포맷 — loader/reader.py의
    _urgency_key와도 반드시 같아야 한다(그쪽은 이 파일을 읽어 station_urgency에
    적재한다). routes.py(compute_routes)도 read_urgency_result를 통해 같은
    파일을 읽는다."""
    return f"urgency/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/urgency_{window_start:%H%M}.parquet"


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

    각 포인트는 {"observed_at", "parking_bike_tot_cnt", "hold_cnt", "lat", "lon"}를
    담는다 — urgency.urgency_score의 stock_history 인자(과거 apps/api/queries.py의
    fetch_all_stock_history와 동일한 형태)로 바로 쓸 수 있고, hold_cnt/lat/lon은
    가장 최근 포인트에서 "현재 재고/정원/위경도"를 뽑아 쓰는 용도다(rebalance/urgency.py,
    routes.py 참고 — routes.py가 권역 배정에 쓰는 위경도도 같은 tick 파일에 있어
    별도 RDS 조회가 필요 없다). stationLatitude/Longitude는 수집 스키마에서
    optional이라 결측이면 NaN으로 들어오는데, 그대로 두면 nearest_region의 거리
    계산이 전부 NaN이 되어 대여소가 엉뚱한 권역(항상 min()의 첫 항목)으로
    배정되므로 이 시점에 걸러낸다.
    """
    history: dict[str, list[dict]] = {}
    columns = ["stationId", "parkingBikeTotCnt", "rackTotCnt", "stationLatitude", "stationLongitude"]
    anchor_tick = _floor_to_tick(anchor, _BIKE_REALTIME_TICK_MINUTES)
    for observed_at, key in _bike_realtime_tick_keys(anchor, lookback_minutes):
        df = read_parquet(key, columns=columns)
        if observed_at == anchor_tick and df is None:
            raise FileNotFoundError(f"stock snapshot parquet not found for {anchor_tick}: {key}")
        if df is None or df.empty:
            continue
        for row in df.itertuples(index=False):
            lat, lon = float(row.stationLatitude), float(row.stationLongitude)
            if math.isnan(lat) or math.isnan(lon):
                continue
            history.setdefault(str(row.stationId), []).append(
                {
                    "observed_at": observed_at,
                    "parking_bike_tot_cnt": int(row.parkingBikeTotCnt),
                    "hold_cnt": int(row.rackTotCnt),
                    "lat": lat,
                    "lon": lon,
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


def read_urgency_result(anchor: datetime) -> pd.DataFrame:
    """compute_urgency 배치(main.py)가 이미 계산해 S3에 써둔 urgency 결과를
    읽는다. routes.py(compute_routes)가 urgency.compute_all()을 다시 부르지
    않고 이 결과를 그대로 재사용하기 위한 함수다 — 둘 다 같은 anchor를 두 번
    계산하는 낭비를 없애고, Airflow의 compute_urgency >> compute_routes
    의존성이 실제 데이터 흐름과 일치하게 만든다. compute_routes는
    compute_urgency의 직접 downstream이므로 파일 부재는 upstream 산출물
    계약 위반이다(read_predictions와 같은 이유로 실패시킨다)."""
    key = _urgency_key(anchor)
    result = read_parquet(key)
    if result is None:
        raise FileNotFoundError(f"urgency result parquet not found for {anchor}: {key}")
    return result
