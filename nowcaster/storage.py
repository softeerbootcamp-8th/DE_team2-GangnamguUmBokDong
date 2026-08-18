"""생활인구 격자 데이터의 S3 아카이브, 실측 실버, 추정치 I/O를 처리한다."""

from __future__ import annotations

from datetime import date

# pyrefly: ignore [missing-import]
import pyarrow as pa

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


def _archive_key(target_date: date) -> str:
    """해당 날짜의 아카이브 parquet S3 키를 반환한다."""
    return f"archive/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}.parquet"


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
    """해당 날짜의 데이터를 아카이브 parquet 파일로 저장한다."""
    key = _archive_key(target_date)
    write_parquet(table, key)
    return key


def list_archive_dates() -> list[date]:
    """아카이브에 저장된 데이터의 날짜 목록을 오름차순으로 반환한다."""
    prefix = f"archive/{GRID_SOURCE_ID}/"
    dates = []
    for key in list_keys(prefix):
        if not key.endswith(".parquet"):
            continue
        filename = key[len(prefix):]
        dt_str = filename.removeprefix("dt=").removesuffix(".parquet")
        dates.append(date.fromisoformat(dt_str))
    return sorted(dates)


def read_archive(target_date: date) -> pa.Table | None:
    """해당 날짜의 아카이브 parquet 파일을 읽어 반환한다."""
    key = _archive_key(target_date)
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

