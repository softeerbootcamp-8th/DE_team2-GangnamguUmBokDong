"""S3 읽기(living_population_grid, population_realtime silver) /
쓰기(living_population_normalized silver, manifest) / baseline 파티션 탐색.

collector/storage.py의 경로 컨벤션(`{layer}/{source_id}/dt=.../hh=.../HHMM.parquet`)을
그대로 재사용하되, collector 코드를 import하지 않고 이 파일 안에서 다시 구현한다.
"""

from __future__ import annotations

import io
from datetime import date, datetime

import pyarrow as pa
import pyarrow.parquet as pq
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
_NOWCAST_FILENAME = "nowcast.parquet"


class PartitionNotFoundError(RuntimeError):
    """요청한 silver 파티션(날짜 또는 window)이 S3에 없을 때."""


def _silver_key(source_id: str, window_start: datetime, ext: str = "parquet") -> str:
    return (
        f"silver/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.{ext}"
    )


def _silver_date_prefix(source_id: str, baseline_date: date) -> str:
    return f"silver/{source_id}/dt={baseline_date:%Y-%m-%d}/"


def list_partition_dates(source_id: str) -> list[date]:
    """`silver/{source_id}/` 아래 존재하는 dt= 파티션 날짜를 오름차순으로 나열한다."""
    prefix = f"silver/{source_id}/"
    dates: list[date] = []
    
    for common_prefix in list_common_prefixes(prefix):
        dt_segment = common_prefix[len(prefix):].rstrip("/")
        if dt_segment.startswith("dt="):
            dates.append(datetime.strptime(dt_segment[len("dt="):], "%Y-%m-%d").date())  # noqa: DTZ007
    return sorted(dates)


def partition_exists(source_id: str, baseline_date: date) -> bool:
    """해당 날짜의 dt= 파티션이 존재하는지 확인한다."""
    return baseline_date in list_partition_dates(source_id)


def find_latest_partition_date(source_id: str) -> date:
    """존재하는 dt= 파티션 중 가장 최신 날짜를 반환한다(`latest` baseline 모드용)."""
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
    """해당 baseline 날짜의 collector 실측 grid silver만 읽어 이어붙인다.

    같은 날짜 prefix에 nowcaster가 저장한 ``nowcast.parquet``은 추정치이며 실측과
    스키마·의미가 다르므로 제외한다. 실측 parquet이 없으면 기존과 동일하게
    ``PartitionNotFoundError``를 발생시킨다.
    """
    prefix = _silver_date_prefix(GRID_SOURCE_ID, baseline_date)
    keys = [
        key
        for key in list_keys(prefix)
        if key.endswith(".parquet") and not key.endswith(_NOWCAST_FILENAME)
    ]

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
    """해당 window의 population_realtime silver 파일 하나를 읽는다. 없으면 예외."""
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
    """living_population_normalized silver를 parquet으로 저장하고 저장된 key를 반환한다."""
    key = _silver_key(NORMALIZED_SOURCE_ID, window_start)
    write_parquet(table, key)
    return key


def write_enriched_station_master(window_start: datetime, table: pa.Table) -> str:
    """CELL_ID가 보강된 대여소 master를 파티션 Silver로 저장한다."""
    key = _silver_key(ENRICHED_STATION_MASTER_SOURCE_ID, window_start)
    write_parquet(table, key)
    return key


def _manifest_key(window_start: datetime, source_id: str = NORMALIZED_SOURCE_ID) -> str:
    return (
        f"_manifest/{source_id}/dt={window_start:%Y-%m-%d}/hh={window_start:%H}/"
        f"{window_start:%H%M}.json"
    )


def write_manifest(
    window_start: datetime,
    data: dict,
    source_id: str = NORMALIZED_SOURCE_ID,
) -> str:
    """해당 window의 manifest(baseline_date, baseline_date_mode 등)를 json으로 저장한다."""
    key = _manifest_key(window_start, source_id)
    write_json(key, data)
    return key
