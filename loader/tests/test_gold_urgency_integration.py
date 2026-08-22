"""Gold urgency publisher의 immutable 계산과 원자 reconcile 계약을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import psycopg
import pyarrow as pa
import pytest
from core.gold_publication import (
    Dependency,
    InputArtifact,
    Parameter,
    PublicationOutcome,
    S3ImmutableObjectStore,
    build_id_set,
    sha256_hex,
)
from core.scoring_config import URGENCY_STOCK_HISTORY_OFFSETS_MINUTES
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from psycopg import Connection

from gold import urgency as urgency_module
from gold.common import (
    OutputObject,
    build_prepared_publication,
    materialize_publication,
    parquet_bytes,
)
from gold.demand import DemandForecastRecord, demand_records_to_parquet
from gold.source_catalog import S3SourceSnapshotCatalog
from gold.state import load_dependencies, load_publication_state, read_state_manifest
from gold.station_release import _stock_records_to_parquet
from gold.station_stock import StationStockRecord
from gold.urgency import publish_station_urgency

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_BUCKET = "test-bucket"
_BASE_URI = f"s3://{_BUCKET}/gold-publication"


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 urgency 통합 테스트 전후 비운다."""
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
        pytest.fail("urgency 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_urgency_publish_replay_correction_rollback_stale_and_empty(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean PostGIS에서 urgency 재실행·correction·rollback·stale·EMPTY를 검증한다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    source_catalog = S3SourceSnapshotCatalog(
        boto3.client("s3", region_name="us-east-1"),
        store,
        bucket=_BUCKET,
    )
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)
    _insert_topology(gold_connection, store, anchor)
    station_dependency = load_dependencies(gold_connection, ("station",))[0]
    _put_stock_dependency(gold_connection, store, anchor, 0, current=1)
    _put_demand_dependency(
        gold_connection,
        store,
        station_dependency,
        anchor,
        0,
        rent=3,
        returned=1,
    )
    first_history = _put_history_manifests(store, anchor, historical=2)

    first = _publish(gold_connection, store, source_catalog, first_history)

    assert first.result.outcome is PublicationOutcome.PUBLISHED
    first_rows = _urgency_rows(gold_connection)
    assert len(first_rows) == 1
    assert first_rows[0][:5] == ("ST-1", anchor, 53.5, 0, "supply_needed")
    assert _urgency_state(gold_connection) == (anchor, 0, 1)
    assert tuple(
        artifact.role
        for artifact in first.evidence[0].input_fingerprint.input_artifacts
    ) == (
        "demand_publication_manifest",
        "stock_history_manifest_01",
        "stock_history_manifest_02",
        "stock_history_manifest_03",
        "stock_history_manifest_04",
        "stock_history_manifest_05",
        "stock_publication_manifest",
        "urgency_output",
    )

    replay = _publish(gold_connection, store, source_catalog, first_history)
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _urgency_rows(gold_connection) == first_rows

    _put_demand_dependency(
        gold_connection,
        store,
        station_dependency,
        anchor,
        1,
        rent=5,
        returned=1,
    )
    corrected = _publish(gold_connection, store, source_catalog, first_history)
    corrected_rows = _urgency_rows(gold_connection)
    assert corrected.result.outcome is PublicationOutcome.PUBLISHED
    assert corrected_rows[0][2] > first_rows[0][2]
    assert corrected_rows[0][5] == first_rows[0][5]
    assert _urgency_state(gold_connection) == (anchor, 1, 1)

    _put_demand_dependency(
        gold_connection,
        store,
        station_dependency,
        anchor,
        2,
        rent=7,
        returned=1,
    )
    reconcile = urgency_module._reconcile_urgency_records

    def fail_after_reconcile(cursor: Any, records: Any) -> None:
        """Target mutation 뒤 강제 예외로 state와 target 동시 rollback을 검증한다."""
        reconcile(cursor, records)
        raise RuntimeError("forced urgency reconcile failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            urgency_module,
            "_reconcile_urgency_records",
            fail_after_reconcile,
        )
        with pytest.raises(RuntimeError, match="forced urgency reconcile failure"):
            _publish(gold_connection, store, source_catalog, first_history)
    assert _urgency_rows(gold_connection) == corrected_rows
    assert _urgency_state(gold_connection) == (anchor, 1, 1)

    stale_guard_anchor = anchor + timedelta(minutes=5)
    _advance_urgency_state_for_stale(gold_connection, stale_guard_anchor)
    stale = _publish(gold_connection, store, source_catalog, first_history)
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _urgency_rows(gold_connection) == corrected_rows

    empty_anchor = anchor + timedelta(minutes=10)
    empty_station_dependency = _deactivate_station(
        gold_connection,
        store,
        empty_anchor,
    )
    _put_stock_dependency(gold_connection, store, empty_anchor, 0, current=5)
    _put_demand_dependency(
        gold_connection,
        store,
        empty_station_dependency,
        empty_anchor,
        0,
        rent=None,
        returned=None,
    )
    empty_history = _put_history_manifests(store, empty_anchor, historical=2)
    emptied = _publish(gold_connection, store, source_catalog, empty_history)
    assert emptied.result.outcome is PublicationOutcome.PUBLISHED
    assert _urgency_rows(gold_connection) == ()
    assert _urgency_state(gold_connection) == (empty_anchor, 0, 0)
    assert emptied.evidence[0].manifest.artifacts == ()


def _publish(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    source_catalog: S3SourceSnapshotCatalog,
    history: tuple[tuple[int, str, str], ...],
) -> Any:
    """Fixture의 five-window identities로 urgency publisher를 실행한다."""
    release_refs = {}
    for key in ("station", "station_demand_forecast", "station_stock"):
        state = load_publication_state(connection, key)
        assert state is not None
        manifest = read_state_manifest(store, state)
        release_refs[key] = (state.manifest_uri, manifest.sha256)
    return publish_station_urgency(
        connection,
        store,
        source_catalog=source_catalog,
        stock_history_manifest_refs=history,
        serving_release_manifest_refs=release_refs,
        object_base_uri=_BASE_URI,
    )


def _put_history_manifests(
    store: S3ImmutableObjectStore,
    anchor: datetime,
    *,
    historical: int,
) -> tuple[tuple[int, str, str], ...]:
    """t-25..-5분 complete realtime source manifest와 Silver를 기록한다."""
    references = []
    for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES:
        logical_dttm = anchor + timedelta(minutes=offset)
        silver = parquet_bytes(
            pa.table(
                {
                    "stationId": pa.array(["ST-1"], type=pa.string()),
                    "parkingBikeTotCnt": pa.array([historical], type=pa.int64()),
                }
            )
        )
        silver_sha = sha256_hex(silver)
        silver_uri = _uri("history/silver", silver_sha, "parquet")
        store.put_once(silver_uri, silver, expected_sha256=silver_sha)
        manifest = build_source_snapshot_manifest(
            source_id="bike_station_realtime",
            logical_dttm=logical_dttm,
            revision_no=0,
            status=SourceSnapshotStatus.SUCCEEDED,
            config_version="sha256:urgency-integration-v1",
            silver_uri=silver_uri,
            silver_byte_sha256=silver_sha,
            counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
            planned_parts=("page-00001-00001",),
            completed_parts=("page-00001-00001",),
        )
        manifest_uri = _source_manifest_uri(manifest)
        store.put_once(
            manifest_uri,
            manifest.canonical_bytes,
            expected_sha256=manifest.sha256,
            require_canonical_json=True,
        )
        references.append((offset, manifest_uri, manifest.sha256))
    return tuple(references)


def _put_stock_dependency(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    anchor: datetime,
    revision_no: int,
    *,
    current: int,
) -> None:
    """Actual fixed-schema stock publication와 target/state fixture를 함께 바꾼다."""
    records = (StationStockRecord("ST-1", anchor, current),)
    materials = materialize_publication(
        store,
        base_uri=_BASE_URI,
        publication_key="station_stock",
        input_artifacts=(
            InputArtifact(
                byte_sha256="1" * 64,
                role="bike_station_realtime_manifest",
                uri=f"s3://{_BUCKET}/fixture/stock-source-{'1' * 64}.json",
            ),
        ),
        parameters=(
            Parameter("station_stock_policy_version", "gold-station-stock-policy-v1"),
        ),
        outputs=(
            OutputObject(
                role="station_stock",
                payload=_stock_records_to_parquet(records),
                row_count=1,
            ),
        ),
    )
    prepared = build_prepared_publication(
        base_uri=_BASE_URI,
        publication_key="station_stock",
        logical_dttm=anchor,
        publisher_version="stock-integration-v1",
        revision_no=revision_no,
        target_row_counts={"station_stock": 1},
        materials=materials,
    )
    _store_prepared_manifest(store, prepared)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("DELETE FROM station_stock")
        cursor.execute(
            """
            INSERT INTO station_stock (sta_id, base_dttm, parking_bike_tot_cnt)
            VALUES ('ST-1', %s, %s)
            """,
            (anchor, current),
        )
        _upsert_state(cursor, prepared)


def _put_demand_dependency(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    station_dependency: Dependency,
    anchor: datetime,
    revision_no: int,
    *,
    rent: int | None,
    returned: int | None,
) -> None:
    """Actual demand fingerprint/output과 target/state fixture를 함께 바꾼다."""
    records = (
        ()
        if rent is None or returned is None
        else tuple(
            DemandForecastRecord(
                base_dttm=anchor,
                sta_id="ST-1",
                predicted_dttm=anchor + timedelta(hours=horizon),
                predicted_rent_cnt=rent,
                predicted_rtn_cnt=returned,
            )
            for horizon in range(1, 13)
        )
    )
    expected_ids = build_id_set(() if not records else ("ST-1",))
    outputs = (
        ()
        if not records
        else (
            OutputObject(
                role="station_demand_forecast",
                payload=demand_records_to_parquet(
                    records,
                    expected_sta_ids=("ST-1",),
                ),
                row_count=len(records),
            ),
        )
    )
    materials = materialize_publication(
        store,
        base_uri=_BASE_URI,
        publication_key="station_demand_forecast",
        dependencies=(station_dependency,),
        input_artifacts=tuple(
            InputArtifact(
                byte_sha256=character * 64,
                role=role,
                uri=f"s3://{_BUCKET}/fixture/{role}-{character * 64}.json",
            )
            for role, character in (
                ("inference_output", "2"),
                ("rental_model_manifest", "3"),
                ("return_model_manifest", "4"),
            )
        ),
        parameters=(
            Parameter("expected_sta_id_sha256", expected_ids.sha256),
            Parameter("horizon_count", "12"),
            Parameter("rounding_mode", "roundTiesToEven"),
        ),
        outputs=outputs,
    )
    prepared = build_prepared_publication(
        base_uri=_BASE_URI,
        publication_key="station_demand_forecast",
        logical_dttm=anchor,
        publisher_version="demand-integration-v1",
        revision_no=revision_no,
        target_row_counts={"station_demand_forecast": len(records)},
        materials=materials,
        conditional_empty_candidate=not records,
    )
    _store_prepared_manifest(store, prepared)
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("DELETE FROM station_demand_forecast")
        if records:
            cursor.executemany(
                """
                INSERT INTO station_demand_forecast (
                    base_dttm,
                    sta_id,
                    predicted_dttm,
                    predicted_rent_cnt,
                    predicted_rtn_cnt
                ) VALUES (%s, %s, %s, %s, %s)
                """,
                [
                    (
                        record.base_dttm,
                        record.sta_id,
                        record.predicted_dttm,
                        record.predicted_rent_cnt,
                        record.predicted_rtn_cnt,
                    )
                    for record in records
                ],
            )
        _upsert_state(cursor, prepared)


def _store_prepared_manifest(store: S3ImmutableObjectStore, prepared: Any) -> None:
    """Fixture publication manifest를 manifest-last 형식으로 immutable 저장한다."""
    store.put_once(
        prepared.manifest_uri,
        prepared.manifest.canonical_bytes,
        expected_sha256=prepared.manifest.sha256,
        require_canonical_json=True,
    )


def _upsert_state(cursor: Any, prepared: Any) -> None:
    """Prepared fixture의 exact state-owned fields를 publication_state에 반영한다."""
    manifest = prepared.manifest
    cursor.execute(
        """
        INSERT INTO gold_meta.publication_state (
            publication_key,
            logical_dttm,
            revision_no,
            manifest_uri,
            artifact_set_sha256,
            input_fingerprint_sha256,
            published_row_cnt
        ) VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (publication_key) DO UPDATE
        SET logical_dttm = EXCLUDED.logical_dttm,
            revision_no = EXCLUDED.revision_no,
            manifest_uri = EXCLUDED.manifest_uri,
            artifact_set_sha256 = EXCLUDED.artifact_set_sha256,
            input_fingerprint_sha256 = EXCLUDED.input_fingerprint_sha256,
            published_row_cnt = EXCLUDED.published_row_cnt
        """,
        (
            manifest.publication_key,
            manifest.logical_dttm,
            manifest.revision_no,
            prepared.manifest_uri,
            manifest.artifact_set_sha256,
            manifest.input_fingerprint_sha256,
            manifest.published_row_cnt,
        ),
    )


def _insert_topology(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    anchor: datetime,
) -> None:
    """한 active station과 station dependency state를 clean DB에 만든다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO weather_grid (weather_grid_id, weather_grid_x_no, weather_grid_y_no) "
            "VALUES ('1_1', 1, 1)"
        )
        cursor.execute(
            """
            INSERT INTO dispatch_center (
                dispatch_center_id,
                dispatch_center_nm,
                dispatch_center_point,
                location_accuracy_cd,
                location_source_desc,
                is_active
            ) VALUES (
                'center',
                '테스트 센터',
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'verified_site',
                'urgency integration fixture',
                true
            )
            """
        )
        cursor.execute(
            """
            INSERT INTO station (
                sta_id,
                sta_nm,
                sta_addr,
                hold_cnt,
                sta_point,
                sta_point_source_cd,
                weather_grid_id,
                dispatch_center_id,
                master_base_dttm,
                last_seen_dttm,
                is_active
            ) VALUES (
                'ST-1',
                '테스트 대여소',
                '서울시 테스트로',
                20,
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'bike_station_master',
                '1_1',
                'center',
                %s,
                %s,
                true
            )
            """,
            (anchor - timedelta(hours=1), anchor - timedelta(minutes=5)),
        )
    _put_station_dependency(
        connection,
        store,
        anchor - timedelta(minutes=5),
    )


def _deactivate_station(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    logical_dttm: datetime,
) -> Dependency:
    """Station을 inactive로 바꾸고 exact dependency state를 다음 revision으로 옮긴다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("UPDATE station SET is_active = false WHERE sta_id = 'ST-1'")
    _put_station_dependency(connection, store, logical_dttm)
    return load_dependencies(connection, ("station",))[0]


def _put_station_dependency(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    logical_dttm: datetime,
) -> None:
    """Station dependency state와 actual immutable manifest를 함께 기록한다."""
    materials = materialize_publication(
        store,
        base_uri=_BASE_URI,
        publication_key="station",
        dependencies=tuple(
            Dependency(
                artifact_set_sha256=character * 64,
                input_fingerprint_sha256=character * 64,
                logical_dttm=logical_dttm - timedelta(hours=1),
                manifest_uri=(
                    f"s3://{_BUCKET}/{publication_key}/"
                    f"publication-{character * 64}.json"
                ),
                publication_key=publication_key,
                revision_no=0,
            )
            for publication_key, character in (
                ("dispatch_center", "b"),
                ("weather_grid", "c"),
            )
        ),
        input_artifacts=(
            InputArtifact(
                byte_sha256="d" * 64,
                role="bike_station_master_manifest",
                uri=f"s3://{_BUCKET}/fixture/station-master-{'d' * 64}.json",
            ),
            InputArtifact(
                byte_sha256="e" * 64,
                role="station_realtime_window_set",
                uri=f"s3://{_BUCKET}/fixture/station-window-{'e' * 64}.json",
            ),
        ),
        parameters=(
            Parameter("center_assignment_version", "integration-v1"),
            Parameter("grid_conversion_version", "integration-v1"),
            Parameter("station_policy_version", "integration-v1"),
        ),
        outputs=(
            OutputObject(
                role="station",
                payload=parquet_bytes(
                    pa.table({"sta_id": pa.array(["ST-1"], type=pa.string())})
                ),
                row_count=1,
            ),
        ),
    )
    prepared = build_prepared_publication(
        base_uri=_BASE_URI,
        publication_key="station",
        logical_dttm=logical_dttm,
        publisher_version="station-integration-v1",
        revision_no=0,
        target_row_counts={"station": 1},
        materials=materials,
    )
    _store_prepared_manifest(store, prepared)
    with connection.transaction(), connection.cursor() as cursor:
        _upsert_state(cursor, prepared)


def _advance_urgency_state_for_stale(
    connection: Connection[Any],
    logical_dttm: datetime,
) -> None:
    """Target은 유지한 채 current state만 미래 fixture로 전진시켜 stale을 만든다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gold_meta.publication_state
               SET logical_dttm = %s,
                   revision_no = 0,
                   manifest_uri = %s,
                   artifact_set_sha256 = %s,
                   input_fingerprint_sha256 = %s,
                   published_row_cnt = 1
             WHERE publication_key = 'station_urgency'
            """,
            (
                logical_dttm,
                f"s3://{_BUCKET}/station-urgency/publication-{'b' * 64}.json",
                "c" * 64,
                "d" * 64,
            ),
        )


def _urgency_rows(
    connection: Connection[Any],
) -> tuple[tuple[str, datetime, float, int, str, datetime], ...]:
    """Urgency target 값과 최초 생성시각을 deterministic 순서로 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT sta_id,
                   base_dttm,
                   urgency_score,
                   critical_remaining_min,
                   rebalance_need_type_cd,
                   created_dttm
              FROM station_urgency
             ORDER BY sta_id COLLATE "C"
            """
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _urgency_state(connection: Connection[Any]) -> tuple[datetime, int, int]:
    """Urgency publication state의 logical·revision·row count를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT logical_dttm, revision_no, published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = 'station_urgency'
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _uri(prefix: str, checksum: str, extension: str) -> str:
    """SHA filename을 가진 fixture S3 URI를 만든다."""
    return f"s3://{_BUCKET}/{prefix}/sha256={checksum}.{extension}"


def _source_manifest_uri(manifest: Any) -> str:
    """Source catalog가 탐색하는 canonical authority manifest URI를 만든다."""
    logical = manifest.logical_dttm.astimezone(UTC)
    return (
        f"s3://{_BUCKET}/source_snapshot_manifest/{manifest.source_id}/"
        f"dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"logical={logical:%Y%m%dT%H%M%S}{logical.microsecond:06d}Z/"
        f"revision={manifest.revision_no:010d}.json"
    )


def _reset_database(connection: Connection[Any]) -> None:
    """Disposable DB의 Gold target과 publication state를 비운다."""
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
