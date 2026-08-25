"""S3 read/write: 아카이브(4주 lookback), collector 실측 silver 읽기, nowcast 추정 파일 쓰기/삭제.

생활인구 격자(`living_population_grid`) 한 소스만 다룬다.

archive 경로 규칙은 `core.layout`이 갖는다 — collector의 compaction도 같은 계층에
쓰므로 한쪽만 바뀌면 조용히 어긋난다.

silver 경로 컨벤션(`silver/{source_id}/dt=.../hh=.../HHMM.parquet`)은 아직 여기서 다시
구현한다. collector·loader·ml_core에도 같은 규칙이 흩어져 있어, 옮기려면 네 모듈을
동시에 건드려야 하므로 별도 작업으로 둔다.
"""

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

# pyrefly: ignore [missing-import]
import pandas as pd

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
    read_json,
    read_parquet,
    write_json,
    write_parquet,
)
from core.source_snapshot_io import (
    SourceSnapshotNotFoundError,
    read_latest_source_snapshot,
)

GRID_SOURCE_ID = "living_population_grid"
_NOWCAST_FILENAME = "nowcast.parquet"
_HISTORICAL_CACHE_PREFIX = f"derived/{GRID_SOURCE_ID}/historical_avg_cache"
_KST = ZoneInfo("Asia/Seoul")


def _grid_date_prefix(target_date: date) -> str:
    """해당 날짜의 격자 실버 S3 경로 접두사를 반환한다."""
    return f"silver/{GRID_SOURCE_ID}/dt={target_date:%Y-%m-%d}/"


def _nowcast_key(target_date: date) -> str:
    """해당 날짜의 추정치 parquet S3 키를 반환한다."""
    return f"{_grid_date_prefix(target_date)}hh=00/{_NOWCAST_FILENAME}"


def read_real_grid_silver(target_date: date) -> pa.Table | None:
    """해당 수집일의 최신 authoritative 실측 Silver만 읽는다.

    Collector는 PARTIAL에도 immutable Silver를 쓸 수 있으므로 Silver prefix를 직접
    나열하면 아직 게시되지 않은 부분 성공이 nowcast baseline으로 승격될 수 있다.
    Source snapshot manifest가 연 ``SUCCEEDED`` authority만 읽고, 해당 KST 날짜에
    성공 authority가 없으면 None을 반환한다. 계약 위반은 숨기지 않고 전파한다.
    """
    cutoff = datetime.combine(target_date, time.max, tzinfo=_KST)
    try:
        snapshot = read_latest_source_snapshot(
            GRID_SOURCE_ID,
            cutoff,
            lookback=timedelta(days=1),
        )
    except SourceSnapshotNotFoundError:
        return None
    if snapshot.manifest.logical_dttm.astimezone(_KST).date() != target_date:
        return None
    return snapshot.table


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


# --- 과거 전체 평균(estimate_day.historical_average_cached) 캐시 ---
#
# 패턴(평일/휴일)별로 누적 합계·카운트와, 이미 반영한 날짜 목록을 저장한다.
# archive가 쌓일수록 매번 전체를 다시 훑으면 실행 시간이 archive 크기에 비례해
# 계속 늘어난다(2026-08 실측: 594일 backfill 직후 한 번 실행에 약 20분) — 이
# 캐시로 "다음 실행부터 새로 추가된 날짜만" 읽게 한다.


def _historical_cache_key(pattern: str, part: str) -> str:
    return f"{_HISTORICAL_CACHE_PREFIX}/{pattern}_{part}"


def read_historical_cache(
    pattern: str,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, list[str]]:
    """패턴별 캐시된 (합계, 카운트, 이미 반영한 날짜 문자열 목록)을 읽는다.

    캐시가 아직 없으면 (None, None, [])을 반환한다 — 최초 실행은 여전히 전체를 훑는다.
    """
    sum_df = read_parquet(_historical_cache_key(pattern, "sum.parquet"))
    count_df = read_parquet(_historical_cache_key(pattern, "count.parquet"))
    manifest = read_json(_historical_cache_key(pattern, "dates.json"))
    included = manifest["dates"] if manifest else []
    if sum_df is not None:
        sum_df = sum_df.set_index(["H_DNG_CD", "CELL_ID", "TT"])
    if count_df is not None:
        count_df = count_df.set_index(["H_DNG_CD", "CELL_ID", "TT"])
    return sum_df, count_df, included


def write_historical_cache(
    pattern: str, sum_df: pd.DataFrame, count_df: pd.DataFrame, included_dates: list[str]
) -> None:
    """패턴별 누적 합계·카운트와 반영한 날짜 목록을 갱신한다."""
    write_parquet(sum_df.reset_index(), _historical_cache_key(pattern, "sum.parquet"))
    write_parquet(count_df.reset_index(), _historical_cache_key(pattern, "count.parquet"))
    write_json(_historical_cache_key(pattern, "dates.json"), {"dates": included_dates})
