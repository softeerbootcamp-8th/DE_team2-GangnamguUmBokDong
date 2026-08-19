"""S3 실버 계층의 생활인구 격자 및 실시간 POI 데이터 I/O를 처리한다."""

from __future__ import annotations

import io
from datetime import date, datetime

# pyrefly: ignore [missing-import]
import pyarrow as pa

# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from core.s3 import (
    get_object_bytes,
    list_keys,
    write_json,
    write_parquet,
)

GRID_SOURCE_ID = "living_population_grid"
REALTIME_SOURCE_ID = "population_realtime"
NORMALIZED_SOURCE_ID = "living_population_normalized"
STATION_MASTER_SOURCE_ID = "bike_station_master"
BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
ENRICHED_STATION_MASTER_SOURCE_ID = "station_master_enriched"
_NOWCAST_FILENAME = "nowcast.parquet"


class PartitionNotFoundError(RuntimeError):
    """요청한 Silver 파티션이 S3에 존재하지 않을 때 발생하는 예외."""


def _silver_key(source_id: str, window_start: datetime, ext: str = "parquet") -> str:
    """수집 윈도우 시각에 대응하는 Silver Parquet S3 키를 생성한다."""
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def _silver_date_prefix(source_id: str, baseline_date: date) -> str:
    """해당 일자의 Silver S3 접두사(prefix)를 생성한다."""
    return f"silver/{source_id}/dt={baseline_date:%Y-%m-%d}/"


def _nowcast_key(target_date: date) -> str:
    """해당 일자의 nowcaster 추정치 parquet S3 키를 반환한다(`nowcaster/storage.py`와 같은 규칙)."""
    return f"{_silver_date_prefix(GRID_SOURCE_ID, target_date)}hh=00/{_NOWCAST_FILENAME}"


def read_nowcast_grid(target_date: date) -> pa.Table:
    """해당 일자의 nowcaster 추정 격자(`nowcast.parquet`)를 읽는다.

    **실측(`living_population_grid` 원본)을 쓰지 않는다.** 이 소스는 관측일이 수집일보다
    4~5일 늦어(`docs/collector/source-config-audit.md` 5-20) `dt=오늘` 파티션 안의 값이
    실은 4~5일 전 것이다. 그래서 "오늘"과 "12시간 뒤"의 baseline은 nowcaster가 만든
    추정치(D-3~D+3)만이 제공할 수 있다. 스키마는 실측과 호환된다
    (`H_DNG_CD`/`CELL_ID`/`TT`/`SPOP`/연령 28개 + `is_estimated`/`estimation_method`).

    args:
        target_date: 대상 일자(미래일 수 있다 — nowcaster가 D+3까지 만든다)
    returns:
        읽어온 PyArrow Table
    raises:
        PartitionNotFoundError: 해당 일자의 추정치 파일이 없을 때
    """
    key = _nowcast_key(target_date)
    body = get_object_bytes(key)
    if body is None:
        raise PartitionNotFoundError(f"{GRID_SOURCE_ID}의 nowcast 추정치 없음: {key}")
    return pq.read_table(io.BytesIO(body))


def read_realtime_silver(window_start: datetime) -> pa.Table:
    """해당 윈도우 시각의 실시간 POI 인구 Parquet 파일을 읽어 반환한다.

    args:
        window_start: 수집 기준 시각
    returns:
        읽어온 PyArrow Table
    raises:
        PartitionNotFoundError: 해당 시각의 파일이 없을 때
    """
    key = _silver_key(REALTIME_SOURCE_ID, window_start)
    body = get_object_bytes(key)
    if body is None:
        raise PartitionNotFoundError(f"{REALTIME_SOURCE_ID} silver 파일 없음: {key}")
    return pq.read_table(io.BytesIO(body))


def read_station_master_silver(window_start: datetime) -> pa.Table:
    """Collector가 같은 window에 쓴 대여소 master Silver를 읽는다."""
    key = _silver_key(STATION_MASTER_SOURCE_ID, window_start)
    body = get_object_bytes(key)
    if body is None:
        raise PartitionNotFoundError(f"{STATION_MASTER_SOURCE_ID} silver 파일 없음: {key}")
    return pq.read_table(io.BytesIO(body))


def read_latest_bike_realtime_silver(window_start: datetime) -> pa.Table | None:
    """window 시각 이전의 최신 실시간 대여소 Silver를 읽는다."""
    cutoff = _silver_key(BIKE_REALTIME_SOURCE_ID, window_start)
    keys = [
        key
        for key in list_keys(f"silver/{BIKE_REALTIME_SOURCE_ID}/")
        if key.endswith(".parquet") and key <= cutoff
    ]
    if not keys:
        return None
    body = get_object_bytes(max(keys))
    return pq.read_table(io.BytesIO(body)) if body else None


def write_normalized_silver(window_start: datetime, table: pa.Table) -> str:
    """정규화된 생활인구 테이블을 Silver Parquet 파일로 저장한다."""
    key = _silver_key(NORMALIZED_SOURCE_ID, window_start)
    write_parquet(table, key)
    return key


def write_enriched_station_master(window_start: datetime, table: pa.Table) -> str:
    """CELL_ID가 보강된 대여소 master를 파티션 Silver로 저장한다."""
    key = _silver_key(ENRICHED_STATION_MASTER_SOURCE_ID, window_start)
    write_parquet(table, key)
    return key


def _manifest_key(
    window_start: datetime,
    source_id: str = NORMALIZED_SOURCE_ID,
) -> str:
    """수집 윈도우 시각과 source_id에 대응하는 Manifest JSON S3 키를 생성한다."""
    return (
        f"_manifest/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def write_manifest(
    window_start: datetime,
    data: dict,
    source_id: str = NORMALIZED_SOURCE_ID,
) -> str:
    """해당 source의 정규화 실행 메타데이터를 Manifest JSON 파일로 저장한다."""
    key = _manifest_key(window_start, source_id)
    write_json(key, data)
    return key
