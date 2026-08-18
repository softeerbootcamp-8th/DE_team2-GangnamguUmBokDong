"""S3 실버 계층의 생활인구 격자 및 실시간 POI 데이터 I/O를 처리한다."""

from __future__ import annotations

import io
from datetime import date, datetime

# pyrefly: ignore [missing-import]
import pyarrow as pa
# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq
from botocore.exceptions import ClientError

# pyrefly: ignore [missing-import]
from core.s3 import (
    get_object_bytes,
    list_common_prefixes,
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


def list_partition_dates(source_id: str) -> list[date]:
    """해당 소스의 S3 파티션에 존재하는 모든 날짜 목록을 오름차순으로 반환한다.

    args:
        source_id: 소스 식별자
    returns:
        정렬된 파티션 날짜 목록
    """
    prefix = f"silver/{source_id}/"
    dates: list[date] = []

    for common_prefix in list_common_prefixes(prefix):
        dt_segment = common_prefix[len(prefix):].rstrip("/")
        if dt_segment.startswith("dt="):
            dates.append(datetime.strptime(dt_segment[len("dt="):], "%Y-%m-%d").date())  # noqa: DTZ007
    return sorted(dates)


def partition_exists(source_id: str, baseline_date: date) -> bool:
    """해당 소스의 특정 일자 파티션이 S3에 존재하는지 확인한다."""
    return baseline_date in list_partition_dates(source_id)


def find_latest_partition_date(source_id: str) -> date:
    """해당 소스의 S3 파티션 중 가장 최신 날짜를 반환한다.

    args:
        source_id: 소스 식별자
    returns:
        가장 최신 파티션 날짜
    raises:
        PartitionNotFoundError: 존재하는 파티션이 없을 때
    """
    dates = list_partition_dates(source_id)
    if not dates:
        raise PartitionNotFoundError(f"{source_id}에 존재하는 dt= 파티션이 없음")
    return dates[-1]


def find_latest_partition_date_on_or_before(source_id: str, reference_date: date) -> date:
    """기준일보다 미래가 아닌 가장 최신 파티션 날짜를 반환한다."""
    dates = [item for item in list_partition_dates(source_id) if item <= reference_date]
    if not dates:
        raise PartitionNotFoundError(
            f"{source_id}에 dt<={reference_date:%Y-%m-%d} 파티션이 없음"
        )
    return dates[-1]


def read_grid_silver(baseline_date: date) -> pa.Table:
    """해당 베이스라인 날짜의 생활인구 격자 Parquet 파일들을 모두 읽어 단일 테이블로 병합한다.

    args:
        baseline_date: 대상 베이스라인 날짜
    returns:
        병합된 PyArrow Table
    raises:
        PartitionNotFoundError: 해당 날짜의 파티션이 없을 때
    """
    prefix = _silver_date_prefix(GRID_SOURCE_ID, baseline_date)
    keys = [k for k in list_keys(prefix) if k.endswith(".parquet")]

    if not keys:
        raise PartitionNotFoundError(
            f"{GRID_SOURCE_ID}의 dt={baseline_date:%Y-%m-%d} 파티션이 없음"
        )

    tables = []
    for key in sorted(keys):
        body = get_object_bytes(key)
        if body:
            tables.append(pq.read_table(io.BytesIO(body)))
    return pa.concat_tables(tables)


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
