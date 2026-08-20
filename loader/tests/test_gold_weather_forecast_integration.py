"""weather source publisher의 complete buffer와 transaction 계약을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import boto3
import psycopg
import pyarrow as pa
import pytest
from core.gold_publication import (
    PublicationOutcome,
    S3ImmutableObjectStore,
    sha256_hex,
)
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold import weather_forecast as weather_module
from gold.common import parquet_bytes
from gold.dispatch_center import load_dispatch_center_seed, publish_dispatch_center
from gold.source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from gold.station_release import publish_station_realtime_release
from gold.weather_forecast import FORECAST_HOUR_COUNT, publish_weather_forecast
from gold.weather_grid import load_weather_grid_seed, publish_weather_grid
from psycopg import Connection

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_ROOT = Path(__file__).resolve().parents[2]
_BUCKET = "test-bucket"
_KST = ZoneInfo("Asia/Seoul")
_MASTER_LOOKBACK = timedelta(hours=48)
_REALTIME_LOOKBACK = timedelta(hours=24)
_SHORT_LOOKBACK = timedelta(hours=24)
_ULTRA_LOOKBACK = timedelta(hours=6)
_KMA_PARTS = tuple(
    sorted(
        f"grid-{nx:03d}x{ny:03d}"
        for nx, ny in (
            (57, 125),
            (57, 126),
            (57, 127),
            (58, 124),
            (58, 125),
            (58, 126),
            (58, 127),
            (59, 124),
            (59, 125),
            (59, 126),
            (59, 127),
            (59, 128),
            (59, 129),
            (60, 124),
            (60, 125),
            (60, 126),
            (60, 127),
            (60, 128),
            (60, 129),
            (61, 124),
            (61, 125),
            (61, 126),
            (61, 127),
            (61, 128),
            (61, 129),
            (62, 124),
            (62, 125),
            (62, 126),
            (62, 127),
            (62, 128),
            (62, 129),
            (63, 125),
            (63, 126),
            (63, 127),
        )
    )
)


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 weather 통합 테스트 전후 비운다."""
    if _DATABASE_URL is None:
        pytest.skip(
            "GOLD_PUBLICATION_TEST_DATABASE_URL이 없어 PostGIS 통합 테스트를 건너뜁니다."
        )
    connection = psycopg.connect(_DATABASE_URL)
    with connection.cursor() as cursor:
        cursor.execute("SELECT current_database()")
        row = cursor.fetchone()
    connection.rollback()
    if row is None or not row[0].startswith("gold151_"):
        connection.close()
        pytest.fail("weather 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_weather_publish_replay_stale_correction_empty_and_atomic_rollback(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clean PostGIS에서 13시간 게시와 모든 version·rollback 경계를 검증한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    now = datetime.now(UTC).replace(minute=0, second=0, microsecond=0)
    anchor = now - timedelta(hours=2)
    station_logical = anchor - timedelta(minutes=10)
    _publish_topology(gold_connection, store, anchor)
    _publish_inactive_station(
        gold_connection,
        client,
        store,
        catalog,
        station_logical,
    )
    _set_station_active(gold_connection, True)

    source_logical = anchor - timedelta(minutes=10)
    short_v0 = _put_source_snapshot(
        client,
        source_id="weather_short_term_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="short_term", temperature=27.0),
        planned_parts=_KMA_PARTS,
    )
    ultra_v0 = _put_source_snapshot(
        client,
        source_id="weather_ultra_short_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="ultra_short", temperature=28.0),
        planned_parts=_KMA_PARTS,
    )
    first = _publish_weather(
        gold_connection,
        store,
        catalog,
        anchor=anchor,
        short=short_v0,
        ultra=ultra_v0,
    )
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    first_rows = _weather_metadata(gold_connection)
    assert len(first_rows) == FORECAST_HOUR_COUNT
    assert {row[2] for row in first_rows} == {28.0}
    assert _weather_state(gold_connection) == (anchor, 0, FORECAST_HOUR_COUNT)

    replay = _publish_weather(
        gold_connection,
        store,
        catalog,
        anchor=anchor,
        short=short_v0,
        ultra=ultra_v0,
    )
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _weather_metadata(gold_connection) == first_rows

    ultra_v1 = _put_source_snapshot(
        client,
        source_id="weather_ultra_short_forecast",
        logical=source_logical,
        revision=1,
        rows=_weather_rows(anchor, product="ultra_short", temperature=29.0),
        planned_parts=_KMA_PARTS,
    )
    correction = _publish_weather(
        gold_connection,
        store,
        catalog,
        anchor=anchor,
        short=short_v0,
        ultra=ultra_v1,
    )
    assert correction.result.outcome is PublicationOutcome.PUBLISHED
    corrected_rows = _weather_metadata(gold_connection)
    assert {row[2] for row in corrected_rows} == {29.0}
    assert tuple((row[0], row[1], row[3]) for row in corrected_rows) == tuple(
        (row[0], row[1], row[3]) for row in first_rows
    )
    assert _weather_state(gold_connection) == (anchor, 1, FORECAST_HOUR_COUNT)

    stale_anchor = anchor - timedelta(hours=1)
    stale_logical = stale_anchor - timedelta(minutes=10)
    stale_short = _put_source_snapshot(
        client,
        source_id="weather_short_term_forecast",
        logical=stale_logical,
        revision=0,
        rows=_weather_rows(stale_anchor, product="short_term", temperature=21.0),
        planned_parts=_KMA_PARTS,
    )
    stale_ultra = _put_source_snapshot(
        client,
        source_id="weather_ultra_short_forecast",
        logical=stale_logical,
        revision=0,
        rows=_weather_rows(stale_anchor, product="ultra_short", temperature=22.0),
        planned_parts=_KMA_PARTS,
    )
    stale = _publish_weather(
        gold_connection,
        store,
        catalog,
        anchor=stale_anchor,
        short=stale_short,
        ultra=stale_ultra,
    )
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _weather_metadata(gold_connection) == corrected_rows
    assert _weather_state(gold_connection) == (anchor, 1, FORECAST_HOUR_COUNT)

    ultra_v2 = _put_source_snapshot(
        client,
        source_id="weather_ultra_short_forecast",
        logical=source_logical,
        revision=2,
        rows=_weather_rows(anchor, product="ultra_short", temperature=30.0),
        planned_parts=_KMA_PARTS,
    )

    def fail_after_upsert(*_args: object, **_kwargs: object) -> None:
        """weather upsert 뒤 absent delete를 실패시켜 transaction rollback을 유도한다."""
        raise RuntimeError("forced weather reconcile failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            weather_module,
            "_delete_absent_weather_forecast_records",
            fail_after_upsert,
        )
        with pytest.raises(RuntimeError, match="forced weather reconcile failure"):
            _publish_weather(
                gold_connection,
                store,
                catalog,
                anchor=anchor,
                short=short_v0,
                ultra=ultra_v2,
            )
    assert _weather_metadata(gold_connection) == corrected_rows
    assert _weather_state(gold_connection) == (anchor, 1, FORECAST_HOUR_COUNT)

    _set_station_active(gold_connection, False)
    empty_anchor = anchor + timedelta(hours=1)
    empty_logical = empty_anchor - timedelta(minutes=10)
    empty_short = _put_source_snapshot(
        client,
        source_id="weather_short_term_forecast",
        logical=empty_logical,
        revision=0,
        rows=_weather_rows(empty_anchor, product="short_term", temperature=31.0),
        planned_parts=_KMA_PARTS,
    )
    empty_ultra = _put_source_snapshot(
        client,
        source_id="weather_ultra_short_forecast",
        logical=empty_logical,
        revision=0,
        rows=_weather_rows(empty_anchor, product="ultra_short", temperature=32.0),
        planned_parts=_KMA_PARTS,
    )
    emptied = _publish_weather(
        gold_connection,
        store,
        catalog,
        anchor=empty_anchor,
        short=empty_short,
        ultra=empty_ultra,
    )
    assert emptied.result.outcome is PublicationOutcome.PUBLISHED
    assert _weather_metadata(gold_connection) == ()
    assert _weather_state(gold_connection) == (empty_anchor, 0, 0)


def _publish_topology(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    logical: datetime,
) -> None:
    """station dependency인 weather grid와 dispatch center를 clean 게시한다."""
    weather = load_weather_grid_seed(
        _ROOT,
        seed_version="weather-grid-weather-integration-v1",
        effective_dttm=logical - timedelta(minutes=1),
    )
    publish_weather_grid(
        connection,
        store,
        seed=weather,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    publish_dispatch_center(
        connection,
        store,
        seed=load_dispatch_center_seed(_ROOT),
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )


def _publish_inactive_station(
    connection: Connection[Any],
    client: Any,
    store: S3ImmutableObjectStore,
    catalog: S3SourceSnapshotCatalog,
    logical: datetime,
) -> None:
    """#153 전 source publisher로 weather dependency station 한 행을 게시한다."""
    master = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=logical - timedelta(hours=1),
        revision=0,
        rows=(
            {
                "RNTLS_ID": "ST-1",
                "ADDR1": "서울시 강남구 테스트로",
                "ADDR2": None,
                "LAT": 37.5172,
                "LOT": 127.0473,
            },
        ),
        planned_parts=("page-00001-01000",),
    )
    realtime = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=logical,
        revision=0,
        rows=(
            {
                "stationId": "ST-1",
                "stationName": "강남 대여소",
                "rackTotCnt": 20,
                "parkingBikeTotCnt": 8,
                "shared": 0,
                "stationLatitude": 37.5172,
                "stationLongitude": 127.0473,
            },
        ),
        planned_parts=("page-00001-01000", "page-01001-02000"),
    )
    result = publish_station_realtime_release(
        connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
        master_lookback=_MASTER_LOOKBACK,
        realtime_lookback=_REALTIME_LOOKBACK,
    )
    assert result.result.outcome is PublicationOutcome.PUBLISHED


def _set_station_active(connection: Connection[Any], active: bool) -> None:
    """weather publisher의 active-grid integration fixture 상태를 설정한다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station SET is_active = %s WHERE sta_id = 'ST-1'",
            (active,),
        )


def _weather_rows(
    anchor: datetime,
    *,
    product: str,
    temperature: float,
) -> tuple[dict[str, object], ...]:
    """한 active grid의 다음 13개 정각 complete KMA 행을 반환한다."""
    first = anchor.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    base = anchor - timedelta(minutes=30)
    rows: list[dict[str, object]] = []
    for offset in range(FORECAST_HOUR_COUNT):
        target = first + timedelta(hours=offset)
        local_target = target.astimezone(_KST)
        local_base = base.astimezone(_KST)
        row: dict[str, object] = {
            "nx": 61,
            "ny": 126,
            "baseDate": local_base.strftime("%Y%m%d"),
            "baseTime": local_base.strftime("%H%M"),
            "fcstDate": local_target.strftime("%Y%m%d"),
            "fcstTime": local_target.strftime("%H%M"),
            "SKY": 1,
            "PTY": 0,
            "POP": 20.0,
            "REH": 55.0,
            "WSD": 2.5,
        }
        if product == "ultra_short":
            row.update({"T1H": temperature, "RN1": "강수없음"})
        else:
            row.update({"TMP": temperature, "PCP": "강수없음"})
        rows.append(row)
    return tuple(rows)


def _put_source_snapshot(
    client: Any,
    *,
    source_id: str,
    logical: datetime,
    revision: int,
    rows: tuple[dict[str, object], ...],
    planned_parts: tuple[str, ...],
) -> SourceManifestArtifact:
    """Silver와 canonical source authority revision을 moto S3에 기록한다."""
    silver = parquet_bytes(pa.Table.from_pylist(list(rows)))
    silver_sha = sha256_hex(silver)
    silver_key = (
        f"silver/{source_id}/dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"sha256={silver_sha}.parquet"
    )
    client.put_object(Bucket=_BUCKET, Key=silver_key, Body=silver)
    config = _ROOT / f"collector/sources/{source_id}.yaml"
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=f"sha256:{sha256_hex(config.read_bytes())}",
        silver_uri=f"s3://{_BUCKET}/{silver_key}",
        silver_byte_sha256=silver_sha,
        counts=SourceSnapshotCounts(len(rows), len(rows), len(rows), 0, 0),
        planned_parts=planned_parts,
        completed_parts=planned_parts,
    )
    key = (
        f"source_snapshot_manifest/{source_id}/dt={logical:%Y-%m-%d}/"
        f"hh={logical:%H}/logical={logical:%Y%m%dT%H%M%S}"
        f"{logical.microsecond:06d}Z/revision={revision:010d}.json"
    )
    client.put_object(Bucket=_BUCKET, Key=key, Body=manifest.canonical_bytes)
    return SourceManifestArtifact(
        manifest=manifest,
        uri=f"s3://{_BUCKET}/{key}",
        byte_sha256=manifest.sha256,
        payload=manifest.canonical_bytes,
    )


def _publish_weather(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    catalog: S3SourceSnapshotCatalog,
    *,
    anchor: datetime,
    short: SourceManifestArtifact,
    ultra: SourceManifestArtifact,
) -> Any:
    """테스트의 bounded lookback으로 weather publication을 실행한다."""
    return publish_weather_forecast(
        connection,
        store,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        source_catalog=catalog,
        scheduled_anchor=anchor,
        short_term_lookback=_SHORT_LOOKBACK,
        ultra_short_lookback=_ULTRA_LOOKBACK,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )


def _weather_metadata(
    connection: Connection[Any],
) -> tuple[tuple[str, datetime, float, datetime], ...]:
    """weather PK·온도·최초 생성 시각을 deterministic 순서로 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT weather_grid_id, forecast_dttm, temperature, created_dttm
              FROM weather_forecast
             ORDER BY weather_grid_id, forecast_dttm
            """
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple((row[0], row[1], float(row[2]), row[3]) for row in rows)


def _weather_state(connection: Connection[Any]) -> tuple[datetime, int, int]:
    """weather publication state의 logical·revision·row count를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT logical_dttm, revision_no, published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = 'weather_forecast'
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _reset_database(connection: Connection[Any]) -> None:
    """disposable DB의 Gold target과 publication state를 비운다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            TRUNCATE TABLE
                rebalance_route_stop,
                rebalance_route,
                station_urgency,
                event,
                weather_forecast,
                station_demand_forecast,
                station_stock,
                station,
                dispatch_center,
                weather_grid,
                gold_meta.publication_state
            RESTART IDENTITY CASCADE
            """
        )
