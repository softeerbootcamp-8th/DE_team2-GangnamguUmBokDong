"""station·stock source release의 replay·correction·stale·원자성을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from uuid import UUID

import boto3
import psycopg
import pyarrow as pa
import pytest
from core.gold_publication import (
    ContractViolation,
    PublicationOutcome,
    RelocationApproval,
    S3ImmutableObjectStore,
    build_station_relocation_approval,
    point_ewkb_xdr_hex,
    sha256_hex,
)
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold import station_release
from gold.common import parquet_bytes
from gold.dispatch_center import load_dispatch_center_seed, publish_dispatch_center
from gold.source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from gold.station_release import (
    publish_station_lifecycle_correction,
    publish_station_master_correction,
    publish_station_realtime_release,
)
from gold.weather_grid import load_weather_grid_seed, publish_weather_grid
from psycopg import Connection

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_ROOT = Path(__file__).resolve().parents[2]
_BUCKET = "test-bucket"
_MASTER_LOOKBACK = timedelta(hours=48)
_REALTIME_LOOKBACK = timedelta(hours=24)


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 station 통합 테스트마다 비운다."""
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
        pytest.fail("station 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_station_release_replay_correction_stale_and_atomic_rollback(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """clean PostGIS에서 두 key의 동시 전진·no-op·rollback을 검증한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    logical = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=10)
    master_logical = logical - timedelta(hours=1)
    _publish_topology(gold_connection, store, logical)
    master = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=master_logical,
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
    )
    realtime_v0 = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=logical,
        revision=0,
        rows=(_realtime_row(rack=20, stock=8),),
    )
    historical_logical = logical - timedelta(minutes=5)
    _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=historical_logical,
        revision=0,
        rows=(_realtime_row(rack=20, stock=7),),
    )

    first = _publish_release(gold_connection, store, catalog, master, realtime_v0)
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    assert first.result.publication_keys == ("station", "station_stock")
    assert _station_values(gold_connection) == (("ST-1", 20, logical, False),)
    assert _stock_values(gold_connection) == (("ST-1", logical, 8),)
    stock_created_dttm = _stock_created_dttm(gold_connection)

    replay = _publish_release(gold_connection, store, catalog, master, realtime_v0)
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _state_revisions(gold_connection) == (("station", 0), ("station_stock", 0))
    _set_station_hold_count(gold_connection, 99)
    with pytest.raises(ContractViolation, match="immutable projection"):
        _publish_release(gold_connection, store, catalog, master, realtime_v0)
    assert _state_revisions(gold_connection) == (("station", 0), ("station_stock", 0))
    _set_station_hold_count(gold_connection, 20)

    realtime_v1 = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=logical,
        revision=1,
        rows=(_realtime_row(rack=21, stock=9),),
    )
    correction = _publish_release(
        gold_connection,
        store,
        catalog,
        master,
        realtime_v1,
    )
    assert correction.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revisions(gold_connection) == (("station", 1), ("station_stock", 1))
    assert _station_values(gold_connection) == (("ST-1", 21, logical, False),)
    assert _stock_values(gold_connection) == (("ST-1", logical, 9),)
    assert _stock_created_dttm(gold_connection) == stock_created_dttm

    _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=historical_logical,
        revision=1,
        rows=(_realtime_row(rack=0, stock=7),),
    )
    lifecycle = _publish_release(
        gold_connection,
        store,
        catalog,
        master,
        realtime_v1,
    )
    assert lifecycle.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revisions(gold_connection) == (("station", 2), ("station_stock", 1))
    assert _station_values(gold_connection) == (("ST-1", 21, logical, False),)
    assert _stock_values(gold_connection) == (("ST-1", logical, 9),)
    lifecycle_replay = publish_station_lifecycle_correction(
        gold_connection,
        store,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
        realtime_lookback=_REALTIME_LOOKBACK,
    )
    assert lifecycle_replay.result.outcome is PublicationOutcome.EXACT_REPLAY

    corrected_master = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=master_logical,
        revision=1,
        rows=(
            {
                "RNTLS_ID": "ST-1",
                "ADDR1": "서울시 강남구 수정로",
                "ADDR2": None,
                "LAT": 37.5172,
                "LOT": 127.0473,
            },
        ),
    )
    master_correction = publish_station_master_correction(
        gold_connection,
        store,
        master_artifact=corrected_master,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
        realtime_lookback=_REALTIME_LOOKBACK,
    )
    assert master_correction.result.outcome is PublicationOutcome.PUBLISHED
    assert _state_revisions(gold_connection) == (("station", 3), ("station_stock", 1))
    assert _station_address(gold_connection) == ("서울시 강남구 수정로", logical)
    assert _stock_values(gold_connection) == (("ST-1", logical, 9),)

    stale_logical = logical - timedelta(minutes=10)
    stale_source = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=stale_logical,
        revision=0,
        rows=(_realtime_row(rack=19, stock=7),),
    )
    stale = _publish_release(
        gold_connection,
        store,
        catalog,
        corrected_master,
        stale_source,
    )
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _station_values(gold_connection) == (("ST-1", 21, logical, False),)

    next_logical = logical + timedelta(minutes=5)
    next_source = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=next_logical,
        revision=0,
        rows=(_realtime_row(rack=22, stock=10),),
    )

    def fail_after_station(*_args: object, **_kwargs: object) -> None:
        """station upsert 후 stock mutation을 실패시켜 rollback을 유도한다."""
        raise RuntimeError("forced stock failure")

    monkeypatch.setattr(station_release, "_replace_station_stock", fail_after_station)
    with pytest.raises(RuntimeError, match="forced stock failure"):
        _publish_release(
            gold_connection,
            store,
            catalog,
            corrected_master,
            next_source,
        )
    assert _state_revisions(gold_connection) == (("station", 3), ("station_stock", 1))
    assert _station_values(gold_connection) == (("ST-1", 21, logical, False),)
    assert _stock_values(gold_connection) == (("ST-1", logical, 9),)


def test_station_topology_changes_clear_only_affected_proposed_routes(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """pair·master·lifecycle topology 변경이 proposed만 station DML 전에 지운다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    logical = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=15)
    historical_logical = logical - timedelta(minutes=5)
    master_logical = logical - timedelta(hours=1)
    original_point = (127.0473, 37.5172)
    moved_point = (126.9845, 37.4982)
    corrected_point = (126.9846, 37.4982)
    _publish_topology(gold_connection, store, logical)
    master_v0 = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=master_logical,
        revision=0,
        rows=(_master_row(*original_point),),
    )
    _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=historical_logical,
        revision=0,
        rows=(_realtime_row(rack=20, stock=7),),
    )
    realtime_v0 = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=logical,
        revision=0,
        rows=(_realtime_row(rack=20, stock=8),),
    )

    original_builder = station_release.build_station_projection

    def activate_bootstrap_projection(*args: Any, **kwargs: Any) -> Any:
        """#153이 제공할 activation-ready prior를 통합 fixture에서만 만든다."""
        projection = original_builder(*args, **kwargs)
        return replace(
            projection,
            records=tuple(
                replace(record, is_active=True) for record in projection.records
            ),
        )

    monkeypatch.setattr(
        station_release,
        "build_station_projection",
        activate_bootstrap_projection,
    )
    first = _publish_release(
        gold_connection,
        store,
        catalog,
        master_v0,
        realtime_v0,
    )
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    monkeypatch.setattr(
        station_release,
        "build_station_projection",
        original_builder,
    )
    initial_center, initial_lon, initial_lat, initial_active = _station_topology(
        gold_connection
    )
    assert (initial_lon, initial_lat, initial_active) == (*original_point, True)

    mutation_order: list[tuple[str, tuple[str, ...]]] = []
    original_delete = station_release._delete_affected_proposed_routes
    original_upsert = station_release._upsert_station

    def track_route_cleanup(cursor: Any, station_ids: tuple[str, ...]) -> None:
        """실제 route DELETE를 호출하면서 station DML 전 순서를 기록한다."""
        mutation_order.append(("route", station_ids))
        original_delete(cursor, station_ids)

    def track_station_upsert(cursor: Any, records: tuple[Any, ...]) -> None:
        """실제 station upsert를 호출하면서 mutation 순서를 기록한다."""
        mutation_order.append(("station", tuple(record.sta_id for record in records)))
        original_upsert(cursor, records)

    monkeypatch.setattr(
        station_release,
        "_delete_affected_proposed_routes",
        track_route_cleanup,
    )
    monkeypatch.setattr(station_release, "_upsert_station", track_station_upsert)

    pair_routes = _insert_route_triplet(
        gold_connection,
        logical,
        UUID("00000000-0000-0000-0000-000000000100"),
    )
    master_v1 = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=master_logical,
        revision=1,
        rows=(_master_row(*moved_point),),
    )
    next_logical = logical + timedelta(minutes=5)
    realtime_next = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=next_logical,
        revision=0,
        rows=(
            _realtime_row(
                rack=21,
                stock=9,
                longitude=moved_point[0],
                latitude=moved_point[1],
            ),
        ),
    )
    relocation = build_station_relocation_approval(
        (
            RelocationApproval(
                approval_id="REL-INTEGRATION-CENTER",
                approved_by="integration-test",
                approved_dttm=logical,
                candidate_point_ewkb=point_ewkb_xdr_hex(*moved_point),
                comparison_cd="gold_vs_master",
                reference_point_ewkb=point_ewkb_xdr_hex(*original_point),
                sta_id="ST-1",
            ),
        )
    )
    original_replace_stock = station_release._replace_station_stock

    def fail_after_station_mutation(*_args: object, **_kwargs: object) -> None:
        """route cleanup·station upsert 뒤 실패해 aggregate rollback을 검증한다."""
        raise RuntimeError("forced route cleanup rollback")

    monkeypatch.setattr(
        station_release,
        "_replace_station_stock",
        fail_after_station_mutation,
    )
    with pytest.raises(RuntimeError, match="forced route cleanup rollback"):
        _publish_release(
            gold_connection,
            store,
            catalog,
            master_v1,
            realtime_next,
            relocation_approval_payload=relocation.canonical_bytes,
        )
    _assert_route_triplet_intact(gold_connection, pair_routes)
    assert _station_topology(gold_connection) == (
        initial_center,
        *original_point,
        True,
    )
    monkeypatch.setattr(
        station_release,
        "_replace_station_stock",
        original_replace_stock,
    )
    mutation_order.clear()
    pair = _publish_release(
        gold_connection,
        store,
        catalog,
        master_v1,
        realtime_next,
        relocation_approval_payload=relocation.canonical_bytes,
    )
    assert pair.result.outcome is PublicationOutcome.PUBLISHED
    assert mutation_order[:2] == [("route", ("ST-1",)), ("station", ("ST-1",))]
    _assert_proposed_removed_and_terminal_preserved(gold_connection, pair_routes)
    pair_center, pair_lon, pair_lat, pair_active = _station_topology(gold_connection)
    assert pair_center != initial_center
    assert (pair_lon, pair_lat, pair_active) == (*moved_point, True)

    master_routes = _insert_route_triplet(
        gold_connection,
        next_logical,
        UUID("00000000-0000-0000-0000-000000000200"),
    )
    master_v2 = _put_source_snapshot(
        client,
        source_id="bike_station_master",
        logical=master_logical,
        revision=2,
        rows=(_master_row(*corrected_point),),
    )
    mutation_order.clear()
    master_result = publish_station_master_correction(
        gold_connection,
        store,
        master_artifact=master_v2,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
        realtime_lookback=_REALTIME_LOOKBACK,
    )
    assert master_result.result.outcome is PublicationOutcome.PUBLISHED
    assert mutation_order[:2] == [("route", ("ST-1",)), ("station", ("ST-1",))]
    _assert_proposed_removed_and_terminal_preserved(gold_connection, master_routes)

    invalid_candidate = _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=next_logical,
        revision=1,
        rows=(
            _realtime_row(
                rack=0,
                stock=9,
                longitude=moved_point[0],
                latitude=moved_point[1],
            ),
        ),
    )
    invalid_pair = _publish_release(
        gold_connection,
        store,
        catalog,
        master_v2,
        invalid_candidate,
    )
    assert invalid_pair.result.outcome is PublicationOutcome.PUBLISHED
    assert _station_topology(gold_connection)[3] is True

    lifecycle_routes = _insert_route_triplet(
        gold_connection,
        next_logical,
        UUID("00000000-0000-0000-0000-000000000300"),
    )
    _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=logical,
        revision=1,
        rows=(_realtime_row(rack=0, stock=8),),
    )
    _put_source_snapshot(
        client,
        source_id="bike_station_realtime",
        logical=historical_logical,
        revision=1,
        rows=(_realtime_row(rack=0, stock=7),),
    )
    mutation_order.clear()
    lifecycle = _publish_release(
        gold_connection,
        store,
        catalog,
        master_v2,
        invalid_candidate,
    )
    assert lifecycle.result.outcome is PublicationOutcome.PUBLISHED
    assert mutation_order[:2] == [("route", ("ST-1",)), ("station", ("ST-1",))]
    assert _station_topology(gold_connection)[3] is False
    _assert_proposed_removed_and_terminal_preserved(
        gold_connection,
        lifecycle_routes,
    )


def _publish_topology(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    logical: datetime,
) -> None:
    """station dependency인 exact weather_grid·dispatch_center seed를 게시한다."""
    weather = load_weather_grid_seed(
        _ROOT,
        seed_version="weather-grid-integration-v1",
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


def _publish_release(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    catalog: S3SourceSnapshotCatalog,
    master: SourceManifestArtifact,
    realtime: SourceManifestArtifact,
    *,
    relocation_approval_payload: bytes | None = None,
) -> Any:
    """테스트의 explicit bounded lookback으로 realtime release를 실행한다."""
    return publish_station_realtime_release(
        connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        source_catalog=catalog,
        object_base_uri=f"s3://{_BUCKET}/gold-publication",
        master_lookback=_MASTER_LOOKBACK,
        realtime_lookback=_REALTIME_LOOKBACK,
        relocation_approval_payload=relocation_approval_payload,
    )


def _master_row(longitude: float, latitude: float) -> dict[str, object]:
    """지정 Point의 valid station master row를 반환한다."""
    return {
        "RNTLS_ID": "ST-1",
        "ADDR1": "서울시 강남구 테스트로",
        "ADDR2": None,
        "LAT": latitude,
        "LOT": longitude,
    }


def _realtime_row(
    *,
    rack: int,
    stock: int,
    longitude: float = 127.0473,
    latitude: float = 37.5172,
) -> dict[str, object]:
    """valid station realtime row를 반환한다."""
    return {
        "stationId": "ST-1",
        "stationName": "강남 대여소",
        "rackTotCnt": rack,
        "parkingBikeTotCnt": stock,
        "shared": 0,
        "stationLatitude": latitude,
        "stationLongitude": longitude,
    }


def _put_source_snapshot(
    client: Any,
    *,
    source_id: str,
    logical: datetime,
    revision: int,
    rows: tuple[dict[str, object], ...],
) -> SourceManifestArtifact:
    """Silver와 canonical authority manifest를 content-addressed S3에 기록한다."""
    silver = parquet_bytes(pa.Table.from_pylist(list(rows)))
    silver_sha = sha256_hex(silver)
    silver_key = (
        f"silver/{source_id}/dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"{logical:%H%M}/sha256={silver_sha}.parquet"
    )
    client.put_object(Bucket=_BUCKET, Key=silver_key, Body=silver)
    config = _ROOT / f"collector/sources/{source_id}.yaml"
    parts = (
        ("page-00001-01000", "page-01001-02000")
        if source_id == "bike_station_realtime"
        else ("page-00001-01000",)
    )
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=revision,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=f"sha256:{sha256_hex(config.read_bytes())}",
        silver_uri=f"s3://{_BUCKET}/{silver_key}",
        silver_byte_sha256=silver_sha,
        counts=SourceSnapshotCounts(len(rows), len(rows), len(rows), 0, 0),
        planned_parts=parts,
        completed_parts=parts,
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


def _station_values(
    connection: Connection[Any],
) -> tuple[tuple[str, int, datetime, bool], ...]:
    """station 검증 column을 ID 순으로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT sta_id, hold_cnt, last_seen_dttm, is_active FROM station ORDER BY sta_id"
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _stock_values(
    connection: Connection[Any],
) -> tuple[tuple[str, datetime, int], ...]:
    """station_stock 검증 column을 ID 순으로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT sta_id, base_dttm, parking_bike_tot_cnt "
            "FROM station_stock ORDER BY sta_id"
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _stock_created_dttm(connection: Connection[Any]) -> datetime:
    """surviving stock PK의 DB-managed created_dttm을 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT created_dttm FROM station_stock WHERE sta_id = 'ST-1'")
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0]


def _station_address(connection: Connection[Any]) -> tuple[str, datetime]:
    """master correction 검증용 station 주소·last-seen을 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT sta_addr, last_seen_dttm FROM station WHERE sta_id = 'ST-1'"
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _set_station_hold_count(connection: Connection[Any], hold_count: int) -> None:
    """drift 탐지 통합 테스트용 target 값을 직접 바꾼다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station SET hold_cnt = %s WHERE sta_id = 'ST-1'",
            (hold_count,),
        )


def _station_topology(
    connection: Connection[Any],
) -> tuple[str, float, float, bool]:
    """현재 station의 center·Point·active topology를 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT dispatch_center_id, ST_X(sta_point), ST_Y(sta_point), is_active
              FROM station
             WHERE sta_id = 'ST-1'
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row[0], float(row[1]), float(row[2]), row[3]


def _insert_route_triplet(
    connection: Connection[Any],
    proposed_dttm: datetime,
    base_route_id: UUID,
) -> tuple[tuple[UUID, str], ...]:
    """같은 station의 proposed·dispatched·completed route를 각각 삽입한다."""
    routes = tuple(
        (UUID(int=base_route_id.int + offset), status)
        for offset, status in enumerate(
            ("proposed", "dispatched", "completed"),
            start=1,
        )
    )
    for route_id, status in routes:
        _insert_route_aggregate(
            connection,
            route_id=route_id,
            proposed_dttm=proposed_dttm,
            status=status,
        )
    return routes


def _insert_route_aggregate(
    connection: Connection[Any],
    *,
    route_id: UUID,
    proposed_dttm: datetime,
    status: str,
) -> None:
    """현재 station center의 한-stop route를 요청한 lifecycle까지 전진시킨다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("SELECT dispatch_center_id FROM station WHERE sta_id = 'ST-1'")
        center = cursor.fetchone()
        assert center is not None
        cursor.execute(
            """
            INSERT INTO rebalance_route (
                route_id,
                dispatch_center_id,
                route_status_cd,
                proposed_dttm
            ) VALUES (%s, %s, 'proposed', %s)
            """,
            (route_id, center[0], proposed_dttm),
        )
        cursor.execute(
            """
            INSERT INTO rebalance_route_stop (
                route_id,
                visit_no,
                sta_id,
                route_action_type_cd,
                bike_cnt
            ) VALUES (%s, 1, 'ST-1', 'pickup', 1)
            """,
            (route_id,),
        )
        if status in {"dispatched", "completed"}:
            cursor.execute(
                """
                UPDATE rebalance_route
                   SET route_status_cd = 'dispatched',
                       dispatched_dttm = %s
                 WHERE route_id = %s
                """,
                (proposed_dttm + timedelta(seconds=1), route_id),
            )
        if status == "completed":
            cursor.execute(
                """
                UPDATE rebalance_route
                   SET route_status_cd = 'completed',
                       completed_dttm = %s
                 WHERE route_id = %s
                """,
                (proposed_dttm + timedelta(seconds=2), route_id),
            )


def _assert_proposed_removed_and_terminal_preserved(
    connection: Connection[Any],
    routes: tuple[tuple[UUID, str], ...],
) -> None:
    """proposed header·stop만 없고 dispatched/completed aggregate는 남았는지 확인한다."""
    with connection.cursor() as cursor:
        actual: list[tuple[UUID, str | None, int]] = []
        for route_id, _ in routes:
            cursor.execute(
                "SELECT route_status_cd FROM rebalance_route WHERE route_id = %s",
                (route_id,),
            )
            status_row = cursor.fetchone()
            cursor.execute(
                "SELECT count(*) FROM rebalance_route_stop WHERE route_id = %s",
                (route_id,),
            )
            count_row = cursor.fetchone()
            assert count_row is not None
            actual.append(
                (
                    route_id,
                    None if status_row is None else status_row[0],
                    count_row[0],
                )
            )
    connection.rollback()
    assert actual == [
        (routes[0][0], None, 0),
        (routes[1][0], "dispatched", 1),
        (routes[2][0], "completed", 1),
    ]


def _assert_route_triplet_intact(
    connection: Connection[Any],
    routes: tuple[tuple[UUID, str], ...],
) -> None:
    """실패 transaction이 세 route header·stop을 모두 원상복구했는지 확인한다."""
    with connection.cursor() as cursor:
        actual: list[tuple[UUID, str, int]] = []
        for route_id, _ in routes:
            cursor.execute(
                "SELECT route_status_cd FROM rebalance_route WHERE route_id = %s",
                (route_id,),
            )
            status_row = cursor.fetchone()
            cursor.execute(
                "SELECT count(*) FROM rebalance_route_stop WHERE route_id = %s",
                (route_id,),
            )
            count_row = cursor.fetchone()
            assert status_row is not None and count_row is not None
            actual.append((route_id, status_row[0], count_row[0]))
    connection.rollback()
    assert actual == [
        (routes[0][0], "proposed", 1),
        (routes[1][0], "dispatched", 1),
        (routes[2][0], "completed", 1),
    ]


def _state_revisions(
    connection: Connection[Any],
) -> tuple[tuple[str, int], ...]:
    """station·stock state revision을 key 순으로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT publication_key, revision_no
              FROM gold_meta.publication_state
             WHERE publication_key IN ('station', 'station_stock')
             ORDER BY publication_key
            """
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _reset_database(connection: Connection[Any]) -> None:
    """disposable DB의 Gold target·publication state를 비운다."""
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
