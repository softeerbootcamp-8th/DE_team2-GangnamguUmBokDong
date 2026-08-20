"""Gold event publisher의 source 격리·correction·EMPTY를 실제 PostGIS에서 검증한다."""

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
from core.gold_publication import PublicationOutcome, S3ImmutableObjectStore, sha256_hex
from core.gold_publication.errors import ContractViolation
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold import event as event_module
from gold.common import parquet_bytes
from gold.event import publish_cultural_event, publish_performance_event
from gold.source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from psycopg import Connection

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_ROOT = Path(__file__).resolve().parents[2]
_BUCKET = "test-bucket"
_KST = ZoneInfo("Asia/Seoul")


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적인 disposable gold151_* DB를 event 통합 테스트마다 비운다."""
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
        pytest.fail("event 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_event_publishers_correction_empty_replay_and_source_isolation(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """두 event source의 correction·stale·rollback·EMPTY를 실제 DB에서 검증한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    logical = datetime.now(UTC) - timedelta(minutes=10)
    event_date = logical.astimezone(_KST).date().isoformat()

    cultural_v0 = _put_source_snapshot(
        client,
        source_id="cultural_event",
        logical=logical,
        revision=0,
        rows=(
            {
                "TITLE": "서울 축제",
                "PLACE": "강남 광장",
                "STRTDATE": event_date,
                "END_DATE": event_date,
                "LOT": 127.0473,
                "LAT": 37.5172,
            },
        ),
    )
    first = publish_cultural_event(
        gold_connection,
        store,
        source_artifact=cultural_v0,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    assert _source_names(gold_connection, "cultural_event") == ("서울 축제",)
    [first_metadata] = _source_metadata(gold_connection, "cultural_event")

    replay = publish_cultural_event(
        gold_connection,
        store,
        source_artifact=cultural_v0,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _source_metadata(gold_connection, "cultural_event") == (first_metadata,)

    performance = _put_source_snapshot(
        client,
        source_id="performance_event",
        logical=logical,
        revision=0,
        rows=(
            {
                "SCH_SEQ": "event-1",
                "TITLE": "야구 경기",
                "SDATE": event_date,
                "EDATE": event_date,
                "SCH_CODE_B": "8",
                "CODE_TITLE_B": "잠실야구장",
            },
        ),
    )
    performance_result = publish_performance_event(
        gold_connection,
        store,
        source_artifact=performance,
        source_catalog=catalog,
        stadium_asset_path=_ROOT / "loader/assets/stadium_coords.json",
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert performance_result.result.outcome is PublicationOutcome.PUBLISHED
    assert _source_names(gold_connection, "performance_event") == ("야구 경기",)

    cultural_v1 = _put_source_snapshot(
        client,
        source_id="cultural_event",
        logical=logical,
        revision=1,
        rows=(
            {
                "TITLE": "서울 축제",
                "PLACE": "강남 광장",
                "STRTDATE": event_date,
                "END_DATE": event_date,
                "LOT": 127.0573,
                "LAT": 37.5172,
            },
        ),
    )
    correction = publish_cultural_event(
        gold_connection,
        store,
        source_artifact=cultural_v1,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert correction.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revision(gold_connection, "event:cultural_event") == 1
    assert _source_names(gold_connection, "cultural_event") == ("서울 축제",)
    assert _source_names(gold_connection, "performance_event") == ("야구 경기",)
    [corrected_metadata] = _source_metadata(gold_connection, "cultural_event")
    assert corrected_metadata[0] == first_metadata[0]
    assert corrected_metadata[1] == pytest.approx(127.0573)
    assert corrected_metadata[2] == first_metadata[2]
    with pytest.raises(ContractViolation, match="correction"):
        publish_cultural_event(
            gold_connection,
            store,
            source_artifact=cultural_v0,
            source_catalog=catalog,
            object_base_uri=f"s3://{_BUCKET}/gold-publication",
        )

    stale_source = _put_source_snapshot(
        client,
        source_id="cultural_event",
        logical=logical - timedelta(hours=1),
        revision=0,
        rows=(
            {
                "TITLE": "과거 축제",
                "PLACE": "강남 광장",
                "STRTDATE": event_date,
                "END_DATE": event_date,
                "LOT": 127.0673,
                "LAT": 37.5172,
            },
        ),
    )
    stale = publish_cultural_event(
        gold_connection,
        store,
        source_artifact=stale_source,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _state_revision(gold_connection, "event:cultural_event") == 1
    assert _source_metadata(gold_connection, "cultural_event") == (corrected_metadata,)

    cultural_v2 = _put_source_snapshot(
        client,
        source_id="cultural_event",
        logical=logical,
        revision=2,
        rows=(
            {
                "TITLE": "서울 축제",
                "PLACE": "강남 광장",
                "STRTDATE": event_date,
                "END_DATE": event_date,
                "LOT": 127.0673,
                "LAT": 37.5172,
            },
        ),
    )

    def fail_after_upsert(*_args: object, **_kwargs: object) -> None:
        """event upsert 뒤 reconcile delete를 실패시켜 transaction rollback을 유도한다."""
        raise RuntimeError("forced event reconcile failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            event_module,
            "_delete_absent_event_records",
            fail_after_upsert,
        )
        with pytest.raises(RuntimeError, match="forced event reconcile failure"):
            publish_cultural_event(
                gold_connection,
                store,
                source_artifact=cultural_v2,
                source_catalog=catalog,
                object_base_uri=f"s3://{_BUCKET}/gold-publication",
            )
    assert _state_revision(gold_connection, "event:cultural_event") == 1
    assert _source_metadata(gold_connection, "cultural_event") == (corrected_metadata,)

    cultural_empty = _put_source_snapshot(
        client,
        source_id="cultural_event",
        logical=logical,
        revision=3,
        rows=None,
    )
    emptied = publish_cultural_event(
        gold_connection,
        store,
        source_artifact=cultural_empty,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
    )
    assert emptied.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revision(gold_connection, "event:cultural_event") == 2
    assert _source_names(gold_connection, "cultural_event") == ()
    assert _source_names(gold_connection, "performance_event") == ("야구 경기",)


def _put_source_snapshot(
    client: Any,
    *,
    source_id: str,
    logical: datetime,
    revision: int,
    rows: tuple[dict[str, object], ...] | None,
) -> SourceManifestArtifact:
    """source Silver와 canonical authority manifest를 moto S3에 기록한다."""
    config_path = _ROOT / f"collector/sources/{source_id}.yaml"
    config_version = f"sha256:{sha256_hex(config_path.read_bytes())}"
    if rows is None:
        status = SourceSnapshotStatus.EMPTY
        silver_uri = None
        silver_sha256 = None
        counts = SourceSnapshotCounts(0, 0, 0, 0, 0)
    else:
        status = SourceSnapshotStatus.SUCCEEDED
        silver = parquet_bytes(pa.Table.from_pylist(list(rows)))
        silver_sha256 = sha256_hex(silver)
        silver_key = (
            f"silver/{source_id}/dt={logical:%Y-%m-%d}/hh={logical:%H}/"
            f"{logical:%H%M}/sha256={silver_sha256}.parquet"
        )
        client.put_object(Bucket=_BUCKET, Key=silver_key, Body=silver)
        silver_uri = f"s3://{_BUCKET}/{silver_key}"
        counts = SourceSnapshotCounts(len(rows), len(rows), len(rows), 0, 0)
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=status,
        config_version=config_version,
        silver_uri=silver_uri,
        silver_byte_sha256=silver_sha256,
        counts=counts,
        planned_parts=("page-00001-01000",),
        completed_parts=("page-00001-01000",),
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


def _source_names(connection: Connection[Any], source_id: str) -> tuple[str, ...]:
    """source-scoped event 이름을 정렬해 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT event_name FROM event WHERE event_source_cd = %s ORDER BY event_name",
            (source_id,),
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(row[0] for row in rows)


def _source_metadata(
    connection: Connection[Any],
    source_id: str,
) -> tuple[tuple[str, float, datetime], ...]:
    """source event의 identity·경도·최초 생성 시각을 정렬해 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT event_id, ST_X(event_point), created_dttm
              FROM event
             WHERE event_source_cd = %s
             ORDER BY event_id
            """,
            (source_id,),
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple((row[0], float(row[1]), row[2]) for row in rows)


def _state_revision(connection: Connection[Any], publication_key: str) -> int:
    """publication state correction revision을 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT revision_no FROM gold_meta.publication_state WHERE publication_key = %s",
            (publication_key,),
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _reset_database(connection: Connection[Any]) -> None:
    """disposable DB의 Gold target과 publication state를 빈 상태로 되돌린다."""
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
