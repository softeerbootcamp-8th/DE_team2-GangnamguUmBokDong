"""S3 read/write: 아카이브(4주 lookback), collector 실측 silver 읽기, nowcast 추정 파일 쓰기/삭제.

collector/storage.py의 경로 컨벤션(`{layer}/{source_id}/dt=.../hh=.../HHMM.parquet`)을
그대로 재사용하되, collector 코드를 import하지 않고 이 파일 안에서 다시 구현한다.
"""

from __future__ import annotations

from datetime import date

import pyarrow as pa

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
    return f"silver/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}/"


def _nowcast_key(target_date: date) -> str:
    return f"{_grid_date_prefix(target_date)}hh=00/{_NOWCAST_FILENAME}"


def _archive_key(target_date: date) -> str:
    return f"archive/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}.parquet"


def read_real_grid_silver(target_date: date) -> pa.Table | None:
    """해당 날짜의 collector 실측 silver를 전부 읽어 이어붙인다.

    같은 dt= 프리픽스 아래 이 모듈이 써둔 nowcast.parquet가 있으면 제외한다
    (그건 추정치이지 실측이 아니므로). 실측 파일이 하나도 없으면 None.
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
    key = _archive_key(target_date)
    write_parquet(table, key)
    return key


def list_archive_dates() -> list[date]:
    """아카이브에 존재하는 날짜를 오름차순으로 나열한다."""
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
    """해당 날짜의 아카이브를 읽는다. 없으면 None(4주 lookback 중 결측으로 처리)."""
    key = _archive_key(target_date)
    return read_parquet(key, as_pandas=False)


def write_nowcast(target_date: date, table: pa.Table) -> str:
    """해당 날짜의 추정치를 nowcast.parquet 고정 키에 쓴다(같은 키이므로 재실행 시 덮어써짐)."""
    key = _nowcast_key(target_date)
    write_parquet(table, key)
    return key


def nowcast_exists(target_date: date) -> bool:
    return object_exists(_nowcast_key(target_date))


def delete_nowcast(target_date: date) -> None:
    """실측값이 도착한 날짜의 옛 추정 파일을 청소한다. 없어도 에러 없이 통과."""
    delete_object(_nowcast_key(target_date))
