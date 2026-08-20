"""S3 실버 계층의 생활인구 격자 및 실시간 POI 데이터 I/O를 처리한다."""

from __future__ import annotations

import io
from datetime import date, datetime, timedelta

# pyrefly: ignore [missing-import]
import pyarrow as pa

# pyrefly: ignore [missing-import]
import pyarrow.parquet as pq

# pyrefly: ignore [missing-import]
from core.s3 import (
    get_object_bytes,
    get_object_metadata,
    list_keys,
    write_json,
    write_parquet,
)
from core.source_snapshot_io import (
    SourceSnapshotNotFoundError,
    SourceSnapshotReadError,
    read_exact_source_snapshot,
    read_latest_source_snapshot,
    read_partial_source_snapshot,
)

GRID_SOURCE_ID = "living_population_grid"
REALTIME_SOURCE_ID = "population_realtime"
NORMALIZED_SOURCE_ID = "living_population_normalized"
STATION_MASTER_SOURCE_ID = "bike_station_master"
BIKE_REALTIME_SOURCE_ID = "bike_station_realtime"
ENRICHED_STATION_MASTER_SOURCE_ID = "station_master_enriched"
_NOWCAST_FILENAME = "nowcast.parquet"
_NORMALIZED_SOURCE_WINDOW_METADATA = "normalized_source_window_start"


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
    return (
        f"{_silver_date_prefix(GRID_SOURCE_ID, target_date)}hh=00/{_NOWCAST_FILENAME}"
    )


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


def read_latest_nowcast_grid(target_date: date) -> tuple[date, pa.Table]:
    """target_date 이전 최신 nowcast를 정적 격자 목록 용도로 읽는다.

    이 함수는 현재 시각의 인구값을 요구하는 실시간 normalizer가 아니라, CELL_ID
    geometry 목록만 쓰는 station master 보강 전용이다. 따라서 같은 날 daily
    nowcaster와의 스케줄 경쟁 때문에 실패시키지 않고 미래 파일은 제외한 최신 성공
    snapshot을 사용해도 시간 정합성을 해치지 않는다.

    args:
        target_date: 이 날짜보다 미래인 snapshot은 선택하지 않는 상한
    returns:
        실제 선택한 baseline 날짜와 PyArrow Table
    raises:
        PartitionNotFoundError: 상한 이전 nowcast가 하나도 없을 때
    """
    prefix = f"silver/{GRID_SOURCE_ID}/"
    suffix = f"hh=00/{_NOWCAST_FILENAME}"
    candidates: list[tuple[date, str]] = []
    for key in list_keys(prefix):
        if not key.endswith(suffix):
            continue
        try:
            partition = key.split("/dt=", 1)[1].split("/", 1)[0]
            snapshot_date = date.fromisoformat(partition)
        except (IndexError, ValueError):
            continue
        if snapshot_date <= target_date:
            candidates.append((snapshot_date, key))
    if not candidates:
        raise PartitionNotFoundError(
            f"{GRID_SOURCE_ID}의 {target_date.isoformat()} 이전 nowcast 추정치 없음"
        )
    snapshot_date, key = max(candidates)
    body = get_object_bytes(key)
    if body is None:
        raise PartitionNotFoundError(f"{GRID_SOURCE_ID}의 nowcast 추정치 없음: {key}")
    return snapshot_date, pq.read_table(io.BytesIO(body))


def read_realtime_silver(window_start: datetime) -> pa.Table:
    """해당 윈도우 시각의 실시간 POI 인구 Parquet 파일을 읽어 반환한다.

    args:
        window_start: 수집 기준 시각
    returns:
        읽어온 PyArrow Table
    raises:
        PartitionNotFoundError: 해당 시각의 파일이 없을 때
    """
    return _read_exact_collector_snapshot(
        REALTIME_SOURCE_ID,
        window_start,
        allow_partial=True,
    )


def read_station_master_silver(window_start: datetime) -> pa.Table:
    """Collector가 같은 window에 쓴 대여소 master Silver를 읽는다."""
    return _read_exact_collector_snapshot(STATION_MASTER_SOURCE_ID, window_start)


def read_latest_bike_realtime_silver(window_start: datetime) -> pa.Table | None:
    """window 시각 이전 24시간의 최신 authoritative 대여소 Silver를 읽는다."""
    try:
        snapshot = read_latest_source_snapshot(
            BIKE_REALTIME_SOURCE_ID,
            window_start,
            lookback=timedelta(hours=24),
        )
    except SourceSnapshotNotFoundError:
        return None
    except SourceSnapshotReadError as exc:
        raise PartitionNotFoundError(str(exc)) from exc
    return snapshot.table


def _read_exact_collector_snapshot(
    source_id: str,
    window_start: datetime,
    *,
    allow_partial: bool = False,
) -> pa.Table:
    """Collector authority가 가리키는 exact-window Parquet을 읽는다."""
    try:
        snapshot = read_exact_source_snapshot(source_id, window_start)
    except SourceSnapshotNotFoundError as exc:
        if allow_partial:
            try:
                return read_partial_source_snapshot(source_id, window_start)
            except SourceSnapshotReadError as partial_exc:
                raise PartitionNotFoundError(str(partial_exc)) from partial_exc
        raise PartitionNotFoundError(str(exc)) from exc
    except SourceSnapshotReadError as exc:
        raise PartitionNotFoundError(str(exc)) from exc
    if snapshot.table is None:
        raise PartitionNotFoundError(
            f"{source_id} exact source snapshot이 EMPTY입니다: {window_start.isoformat()}"
        )
    return snapshot.table


def write_normalized_silver(
    target: datetime,
    table: pa.Table,
    *,
    source_window_start: datetime | None = None,
) -> str | None:
    """새 세대의 정규화 결과만 target Silver Parquet에 저장한다.

    미래 예보는 여러 수집 window가 같은 target 키를 갱신한다. 늦게 실행된 과거
    backfill이 더 최신 window의 예보나 실제 관측을 되돌리지 않도록, S3 user
    metadata에 원본 수집 시각을 기록하고 기존 세대가 더 최신이면 쓰기를 건너뛴다.

    args:
        target: 정규화 값이 의미하는 시각이자 출력 키 시각
        table: 저장할 정규화 격자 테이블
        source_window_start: 이 값을 만든 실시간 수집 window. 생략하면 target과 같음
    returns:
        저장한 S3 키. 더 최신 세대가 이미 있어 건너뛰면 None
    raises:
        ValueError: 기존 객체의 세대 metadata가 손상됐을 때
    """
    generation = source_window_start or target
    key = _silver_key(NORMALIZED_SOURCE_ID, target)
    metadata = get_object_metadata(key)
    if metadata is not None:
        raw_generation = metadata.get(_NORMALIZED_SOURCE_WINDOW_METADATA)
        if raw_generation is not None:
            try:
                existing_generation = datetime.fromisoformat(raw_generation)
            except ValueError as exc:
                raise ValueError(
                    f"정규화 세대 metadata가 잘못됐습니다: key={key}"
                ) from exc
            if existing_generation > generation:
                return None

    write_parquet(
        table,
        key,
        metadata={_NORMALIZED_SOURCE_WINDOW_METADATA: generation.isoformat()},
    )
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
