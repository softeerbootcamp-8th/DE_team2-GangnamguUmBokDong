"""S3 read/write: 아카이브(4주 lookback), collector 실측 silver 읽기, nowcast 추정 파일 쓰기/삭제.

생활인구 격자(`living_population_grid`) 한 소스만 다룬다.

archive 경로 규칙은 `core.layout`이 갖는다 — collector의 compaction도 같은 계층에
쓰므로 한쪽만 바뀌면 조용히 어긋난다.

silver 경로 컨벤션(`silver/{source_id}/dt=.../hh=.../HHMM.parquet`)은 아직 여기서 다시
구현한다. collector·loader·ml_core에도 같은 규칙이 흩어져 있어, 옮기려면 네 모듈을
동시에 건드려야 하므로 별도 작업으로 둔다.
"""

from __future__ import annotations

from datetime import date

# pyrefly: ignore [missing-import]
import pyarrow as pa

# pyrefly: ignore [missing-import]
from core.layout import archive_key as _archive_key
from core.layout import archive_prefix

# pyrefly: ignore [missing-import]
from core.s3 import (
    delete_object,
    list_keys,
    object_exists,
    read_parquet,
    write_parquet,
)

GRID_SOURCE_ID = "living_population_grid"
_NOWCAST_FILENAME = "nowcast.parquet"


def _grid_date_prefix(target_date: date) -> str:
    """해당 날짜의 격자 실버 S3 경로 접두사를 반환한다."""
    return f"silver/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}/"


def _nowcast_key(target_date: date) -> str:
    """해당 날짜의 추정치 parquet S3 키를 반환한다."""
    return f"{_grid_date_prefix(target_date)}hh=00/{_NOWCAST_FILENAME}"


def read_real_grid_silver(target_date: date) -> pa.Table | None:
    """해당 날짜의 실측 실버 parquet 파일들을 모두 읽어 단일 테이블로 병합한다.

    추정치 파일(nowcast.parquet)은 제외하며, 실측 파일이 없으면 None을 반환합니다.
    """
    prefix = _grid_date_prefix(target_date)
    keys = [key for key in list_keys(prefix) if key.endswith(".parquet") and not key.endswith(_NOWCAST_FILENAME)]
    if not keys:
        return None
    tables = []
    for key in sorted(keys):
        t = read_parquet(key, as_pandas=False)
        if t is not None:
            tables.append(t)
    return pa.concat_tables(tables) if tables else None


def write_archive(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 데이터를 아카이브 parquet으로 저장하고 저장된 key를 반환한다."""
    key = _archive_key(GRID_SOURCE_ID, target_date)
    write_parquet(table, key)
    return key


def list_archive_dates() -> list[date]:
    """아카이브에 저장된 데이터의 날짜 목록을 오름차순으로 반환한다."""
    prefix = archive_prefix(GRID_SOURCE_ID)
    dates = []
    for key in list_keys(prefix):
        if not key.endswith(".parquet"):
            continue
        filename = key[len(prefix):]
        dt_str = filename.removeprefix("dt=").removesuffix(".parquet")
        dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


def read_archive(target_date: date) -> pa.Table | None:
    """해당 날짜의 아카이브를 읽는다. 없으면 None(4주 lookback 중 결측으로 처리)."""
    key = _archive_key(GRID_SOURCE_ID, target_date)
    return read_parquet(key, as_pandas=False)


def write_nowcast(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 추정치 데이터를 nowcast.parquet 파일로 저장한다."""
    key = _nowcast_key(target_date)
    write_parquet(table, key)
    return key


def nowcast_exists(target_date: date) -> bool:
    """해당 날짜의 추정치 파일 존재 여부를 반환한다."""
    return object_exists(_nowcast_key(target_date))


def delete_nowcast(target_date: date) -> None:
    """해당 날짜의 기존 추정치 parquet 파일을 삭제한다."""
    delete_object(_nowcast_key(target_date))

