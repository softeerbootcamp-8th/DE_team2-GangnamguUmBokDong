"""Gold route publisher의 immutable lineage와 원자 aggregate 계약을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from typing import Any

import boto3
import psycopg
import pyarrow as pa
import pytest
from core.gold_publication import (
    Artifact,
    InputArtifact,
    Parameter,
    PublicationOutcome,
    S3ImmutableObjectStore,
    build_artifact_set,
    build_id_set,
    build_input_fingerprint,
    build_publication_manifest,
    sha256_hex,
)
from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from gold import rebalance_route as route_module
from gold.common import parquet_bytes
from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import publish_rebalance_route
from gold.state import load_dependencies
from psycopg import Connection

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_BUCKET = "test-bucket"
_BASE_URI = f"s3://{_BUCKET}/gold-publication"
_URGENCY_SCHEMA = pa.schema(
    (
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("urgency_score", pa.float64(), nullable=False),
        pa.field("critical_remaining_min", pa.int32(), nullable=False),
        pa.field("rebalance_need_type_cd", pa.string(), nullable=False),
        pa.field("bike_qty", pa.int32(), nullable=False),
    )
)


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 route 통합 테스트 전후 비운다."""
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
        pytest.fail("route 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_route_publish_replay_coverage_correction_stale_empty_and_rollback(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean PostGIS에서 version·coverage·single-flight·원자성을 검증한다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)
    _insert_topology_and_dependency_states(gold_connection, anchor)

    first_uri, first_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=anchor,
        revision_no=0,
        pickup_qty=5,
        dropoff_qty=5,
    )
    first = _publish(gold_connection, store, first_uri, first_sha)
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    first_parameters = {
        parameter.name: parameter.value
        for parameter in first.evidence[0].input_fingerprint.parameters
    }
    assert first_parameters["route_algorithm_version"] == (
        route_module.ROUTE_ALGORITHM_VERSION
    )
    assert first_parameters["fleet_capacities"] == ",".join(
        str(capacity) for capacity in route_module.FLEET_CAPACITIES
    )
    assert first_parameters["fleet_config_version"] == (
        route_module.FLEET_CONFIG_VERSION
    )
    assert first_parameters["supply_trigger_stock_ratio"] == "0.20"
    assert first_parameters["supply_visit_target_stock_ratio"] == "0.40"
    assert first_parameters["route_max_duration_minutes"] == "120.00"
    assert first_parameters["route_road_distance_factor"] == "1.25"
    assert first_parameters["route_assumed_speed_kmh"] == "18.00"
    assert first_parameters["route_service_minutes_per_stop"] == "4.00"
    assert first_parameters["route_bike_handling_minutes"] == "0.50"
    assert _route_state(gold_connection) == (anchor, 0, 1)
    first_routes, first_stops = _route_rows(gold_connection)
    assert len(first_routes) == 1
    assert [(row[2], row[3]) for row in first_stops] == [
        ("pickup", 5),
        ("dropoff", 5),
    ]

    replay = _publish(gold_connection, store, first_uri, first_sha)
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _route_rows(gold_connection) == (first_routes, first_stops)

    route_id = first_routes[0][0]
    _dispatch_route(gold_connection, route_id, anchor + timedelta(minutes=1))
    terminal_before = _terminal_rows(gold_connection)
    covered = _publish(gold_connection, store, first_uri, first_sha)
    assert covered.result.outcome is PublicationOutcome.PUBLISHED
    assert _route_state(gold_connection) == (anchor, 1, 0)
    assert _proposed_rows(gold_connection) == ((), ())
    assert _terminal_rows(gold_connection) == terminal_before
    coverage_input = next(
        item
        for item in covered.evidence[0].input_fingerprint.input_artifacts
        if item.role == "route_coverage"
    )
    coverage_parameter = next(
        item
        for item in covered.evidence[0].input_fingerprint.parameters
        if item.name == "route_coverage_sha256"
    )
    assert coverage_input.byte_sha256 == coverage_parameter.value

    correction_uri, correction_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=anchor,
        revision_no=1,
        pickup_qty=8,
        dropoff_qty=8,
    )
    correction = _publish(
        gold_connection,
        store,
        correction_uri,
        correction_sha,
    )
    assert correction.result.outcome is PublicationOutcome.PUBLISHED
    assert correction.evidence[0].manifest.artifacts == ()
    assert _route_state(gold_connection) == (anchor, 2, 0)
    corrected_routes, corrected_stops = _proposed_rows(gold_connection)
    assert (corrected_routes, corrected_stops) == ((), ())
    assert _terminal_rows(gold_connection) == terminal_before

    rollback_uri, rollback_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=anchor,
        revision_no=2,
        pickup_qty=10,
        dropoff_qty=10,
    )
    reconcile = route_module._reconcile_route_plan

    def fail_after_reconcile(cursor: Any, plan: Any) -> None:
        """Target reconcile 뒤 예외로 route state와 aggregate rollback을 확인한다."""
        reconcile(cursor, plan)
        raise RuntimeError("forced route reconcile failure")

    with monkeypatch.context() as patch:
        patch.setattr(route_module, "_reconcile_route_plan", fail_after_reconcile)
        with pytest.raises(RuntimeError, match="forced route reconcile failure"):
            _publish(gold_connection, store, rollback_uri, rollback_sha)
    assert _route_state(gold_connection) == (anchor, 2, 0)
    assert _proposed_rows(gold_connection) == (corrected_routes, corrected_stops)
    assert _terminal_rows(gold_connection) == terminal_before

    future_route_logical = anchor + timedelta(minutes=5)
    _advance_route_state_logical(gold_connection, future_route_logical)
    stale = _publish(gold_connection, store, rollback_uri, rollback_sha)
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _route_state(gold_connection) == (future_route_logical, 0, 0)
    assert _proposed_rows(gold_connection) == (corrected_routes, corrected_stops)

    empty_anchor = anchor + timedelta(minutes=10)
    _advance_route_input_states(gold_connection, empty_anchor)
    empty_uri, empty_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=empty_anchor,
        revision_no=0,
        pickup_qty=0,
        dropoff_qty=0,
    )
    emptied = _publish(gold_connection, store, empty_uri, empty_sha)
    assert emptied.result.outcome is PublicationOutcome.PUBLISHED
    assert emptied.evidence[0].manifest.artifacts == ()
    assert _route_state(gold_connection) == (empty_anchor, 0, 0)
    assert _proposed_rows(gold_connection) == ((), ())
    assert _terminal_rows(gold_connection) == terminal_before


def test_route_rejects_last_known_good_urgency_after_current_inputs_advance(
    gold_connection: Connection[Any],
) -> None:
    """Dashboard용 stale urgency는 current route 입력으로 재사용하지 않는다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)
    _insert_topology_and_dependency_states(gold_connection, anchor)
    manifest_uri, manifest_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=anchor,
        revision_no=0,
        pickup_qty=5,
        dropoff_qty=5,
    )
    _advance_route_input_states(gold_connection, anchor + timedelta(minutes=5))

    with pytest.raises(
        route_module.ContractViolation,
        match="같은 anchor의 urgency만 사용할 수",
    ):
        _publish(gold_connection, store, manifest_uri, manifest_sha)


def test_concurrent_same_route_publication_is_publish_plus_replay(
    gold_connection: Connection[Any],
) -> None:
    """동일 route candidate 동시 실행은 한 publish와 한 exact replay로 직렬화된다."""
    assert _DATABASE_URL is not None
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)
    _insert_topology_and_dependency_states(gold_connection, anchor)
    manifest_uri, manifest_sha = _put_urgency_publication(
        gold_connection,
        store,
        logical_dttm=anchor,
        revision_no=0,
        pickup_qty=5,
        dropoff_qty=5,
    )

    def run_once() -> PublicationOutcome:
        """별도 DB connection으로 같은 immutable urgency를 게시한다."""
        connection = psycopg.connect(_DATABASE_URL)
        try:
            return _publish(
                connection,
                store,
                manifest_uri,
                manifest_sha,
            ).result.outcome
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(executor.map(lambda _index: run_once(), range(2)))
    assert sorted(outcomes) == sorted(
        (PublicationOutcome.PUBLISHED, PublicationOutcome.EXACT_REPLAY)
    )
    assert _route_state(gold_connection) == (anchor, 0, 1)


def test_pickup_cooldown_uses_terminal_lifecycle_semantics(
    gold_connection: Connection[Any],
) -> None:
    """오래된 dispatched와 최근 completed는 막고 cooldown 밖 completed는 허용한다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)

    def publish_fresh_route() -> str:
        """각 lifecycle 반례를 독립 DB 상태에서 만들고 route ID를 반환한다."""
        _reset_database(gold_connection)
        _insert_topology_and_dependency_states(gold_connection, anchor)
        manifest_uri, manifest_sha = _put_urgency_publication(
            gold_connection,
            store,
            logical_dttm=anchor,
            revision_no=0,
            pickup_qty=5,
            dropoff_qty=5,
        )
        _publish(gold_connection, store, manifest_uri, manifest_sha)
        return _route_rows(gold_connection)[0][0][0]

    logical = anchor + timedelta(hours=10)
    dispatched_at = anchor + timedelta(minutes=1)
    route_id = publish_fresh_route()
    _dispatch_route(gold_connection, route_id, dispatched_at)
    snapshot = route_module._load_route_database_snapshot(gold_connection, logical)
    assert snapshot.pickup_cooldown_sta_ids == ("ST-1",)

    route_id = publish_fresh_route()
    _dispatch_route(gold_connection, route_id, dispatched_at)
    _complete_route(gold_connection, route_id, logical - timedelta(minutes=1))
    snapshot = route_module._load_route_database_snapshot(gold_connection, logical)
    assert snapshot.pickup_cooldown_sta_ids == ("ST-1",)

    route_id = publish_fresh_route()
    _dispatch_route(gold_connection, route_id, dispatched_at)
    cooldown = DEFAULT_REBALANCE_POLICY.pickup_cooldown_minutes
    _complete_route(gold_connection, route_id, logical - timedelta(minutes=cooldown))
    snapshot = route_module._load_route_database_snapshot(gold_connection, logical)
    assert snapshot.pickup_cooldown_sta_ids == ()


def _put_urgency_publication(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    *,
    logical_dttm: datetime,
    revision_no: int,
    pickup_qty: int,
    dropoff_qty: int,
) -> tuple[str, str]:
    """Route fixture용 station_urgency manifest·fingerprint·output을 기록한다."""
    rows = []
    if pickup_qty > 0 or dropoff_qty > 0:
        rows = [
            {
                "sta_id": "ST-1",
                "base_dttm": logical_dttm,
                "urgency_score": 90.0,
                "critical_remaining_min": 10,
                "rebalance_need_type_cd": "retrieval_needed",
                "bike_qty": pickup_qty,
            },
            {
                "sta_id": "ST-2",
                "base_dttm": logical_dttm,
                "urgency_score": 80.0,
                "critical_remaining_min": 15,
                "rebalance_need_type_cd": "supply_needed",
                "bike_qty": dropoff_qty,
            },
        ]
    output_payload = parquet_bytes(pa.Table.from_pylist(rows, schema=_URGENCY_SCHEMA))
    output_sha = sha256_hex(output_payload)
    computed_uri = _object_uri("urgency/computed", output_sha, "parquet")
    store.put_once(computed_uri, output_payload, expected_sha256=output_sha)
    expected_ids = build_id_set(tuple(row["sta_id"] for row in rows))
    dependencies = load_dependencies(
        connection,
        ("station", "station_demand_forecast", "station_stock"),
    )
    dependency_by_key = {item.publication_key: item for item in dependencies}
    input_artifacts = (
        InputArtifact(
            "d" * 64,
            "demand_publication_manifest",
            dependency_by_key["station_demand_forecast"].manifest_uri,
        ),
        *tuple(
            InputArtifact(
                str(index) * 64,
                f"stock_history_manifest_m{offset:02d}",
                _object_uri(f"urgency/history/{index}", str(index) * 64, "json"),
            )
            for index, offset in enumerate((5, 10, 15, 20, 25), start=1)
        ),
        InputArtifact(
            "e" * 64,
            "stock_publication_manifest",
            dependency_by_key["station_stock"].manifest_uri,
        ),
        InputArtifact(output_sha, "urgency_output", computed_uri),
    )
    fingerprint = build_input_fingerprint(
        "station_urgency",
        dependencies,
        input_artifacts,
        (
            Parameter("expected_sta_id_sha256", expected_ids.sha256),
            Parameter(
                "rebalance_policy_config",
                DEFAULT_REBALANCE_POLICY.canonical_json,
            ),
            Parameter("scoring_config_version", URGENCY_SCORING_CONFIG_VERSION),
            Parameter("stock_history_offsets", "-25,-20,-15,-10,-5"),
            Parameter("stock_window_count", "6"),
        ),
    )
    fingerprint_uri = _object_uri(
        "station_urgency/fingerprint",
        fingerprint.sha256,
        "json",
    )
    store.put_once(
        fingerprint_uri,
        fingerprint.canonical_bytes,
        expected_sha256=fingerprint.sha256,
        require_canonical_json=True,
    )
    artifacts = ()
    if rows:
        output_uri = _object_uri(
            "station_urgency/output",
            output_sha,
            "parquet",
        )
        store.put_once(output_uri, output_payload, expected_sha256=output_sha)
        artifacts = (Artifact(output_sha, "station_urgency", len(rows), output_uri),)
    artifact_set = build_artifact_set(artifacts)
    manifest = build_publication_manifest(
        publication_key="station_urgency",
        artifact_set=artifact_set,
        input_fingerprint=fingerprint,
        input_fingerprint_uri=fingerprint_uri,
        logical_dttm=logical_dttm,
        publisher_version="urgency-integration-v2",
        revision_no=revision_no,
        target_row_counts={"station_urgency": len(rows)},
        conditional_empty_proven=not rows,
    )
    manifest_uri = (
        f"{_BASE_URI}/station_urgency/manifests/publication-{manifest.sha256}.json"
    )
    store.put_once(
        manifest_uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    _replace_urgency_state(connection, manifest_uri, manifest)
    return manifest_uri, manifest.sha256


def _insert_topology_and_dependency_states(
    connection: Connection[Any],
    anchor: datetime,
) -> None:
    """두 active station과 route가 요구하는 네 선행 state를 만든다."""
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
                'center_a',
                '테스트 센터',
                ST_SetSRID(ST_MakePoint(127.0, 37.5), 4326),
                'verified_site',
                'route integration fixture',
                true
            )
            """
        )
        cursor.executemany(
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
                %s, %s, '서울시 테스트로', 20,
                ST_SetSRID(ST_MakePoint(%s, 37.5), 4326),
                'bike_station_master', '1_1', 'center_a', %s, %s, true
            )
            """,
            [
                ("ST-1", "회수 대여소", 127.001, anchor - timedelta(hours=1), anchor),
                ("ST-2", "공급 대여소", 127.002, anchor - timedelta(hours=1), anchor),
            ],
        )
        cursor.executemany(
            """
            INSERT INTO station_stock (
                sta_id,
                base_dttm,
                parking_bike_tot_cnt
            ) VALUES (%s, %s, %s)
            """,
            (("ST-1", anchor, 20), ("ST-2", anchor, 0)),
        )
        for index, (key, logical) in enumerate(
            (
                ("dispatch_center", anchor - timedelta(hours=1)),
                ("station", anchor - timedelta(minutes=5)),
                ("station_demand_forecast", anchor),
                ("station_stock", anchor),
            ),
            start=1,
        ):
            checksum = f"{index:x}" * 64
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
                ) VALUES (%s, %s, 0, %s, %s, %s, 2)
                """,
                (
                    key,
                    logical,
                    f"{_BASE_URI}/{key}/publication-{checksum}.json",
                    checksum,
                    checksum,
                ),
            )


def _replace_urgency_state(
    connection: Connection[Any],
    manifest_uri: str,
    manifest: Any,
) -> None:
    """Fixture urgency target과 publication_state를 같은 transaction에서 교체한다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("DELETE FROM station_urgency")
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
            ) VALUES (
                'station_urgency', %s, %s, %s, %s, %s, %s
            )
            ON CONFLICT (publication_key) DO UPDATE
            SET logical_dttm = EXCLUDED.logical_dttm,
                revision_no = EXCLUDED.revision_no,
                manifest_uri = EXCLUDED.manifest_uri,
                artifact_set_sha256 = EXCLUDED.artifact_set_sha256,
                input_fingerprint_sha256 = EXCLUDED.input_fingerprint_sha256,
                published_row_cnt = EXCLUDED.published_row_cnt
            """,
            (
                manifest.logical_dttm,
                manifest.revision_no,
                manifest_uri,
                manifest.artifact_set_sha256,
                manifest.input_fingerprint_sha256,
                manifest.published_row_cnt,
            ),
        )


def _publish(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    manifest_uri: str,
    manifest_sha: str,
) -> Any:
    """Fixture urgency identity로 route publisher를 실행한다."""
    return publish_rebalance_route(
        connection,
        store,
        urgency_manifest_uri=manifest_uri,
        urgency_manifest_sha256=manifest_sha,
        object_base_uri=_BASE_URI,
    )


def _dispatch_route(
    connection: Connection[Any],
    route_id: str,
    dispatched_dttm: datetime,
) -> None:
    """Proposed fixture route를 정상 lifecycle로 dispatched 전환한다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE rebalance_route
               SET route_status_cd = 'dispatched',
                   dispatched_dttm = %s
             WHERE route_id = %s
            """,
            (dispatched_dttm, route_id),
        )


def _complete_route(
    connection: Connection[Any],
    route_id: str,
    completed_dttm: datetime,
) -> None:
    """Dispatched fixture route를 지정 시각에 completed로 전환한다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE rebalance_route
               SET route_status_cd = 'completed',
                   completed_dttm = %s
             WHERE route_id = %s
            """,
            (completed_dttm, route_id),
        )


def _advance_route_state_logical(
    connection: Connection[Any],
    logical_dttm: datetime,
) -> None:
    """Stale classifier 검증용으로 route state version만 미래 logical로 전진시킨다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gold_meta.publication_state
               SET logical_dttm = %s,
                   revision_no = 0
             WHERE publication_key = 'rebalance_route'
            """,
            (logical_dttm,),
        )


def _advance_route_input_states(
    connection: Connection[Any],
    logical_dttm: datetime,
) -> None:
    """Route 안전성 fixture의 current stock/demand anchor를 함께 전진시킨다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute(
            """
            UPDATE gold_meta.publication_state
               SET logical_dttm = %s
             WHERE publication_key IN (
                 'station_stock', 'station_demand_forecast'
             )
            """,
            (logical_dttm,),
        )


def _route_state(connection: Connection[Any]) -> tuple[datetime, int, int]:
    """Route publication state의 logical·revision·header count를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT logical_dttm, revision_no, published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = 'rebalance_route'
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _route_rows(
    connection: Connection[Any],
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    """모든 route와 stop의 identity·business·metadata 값을 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT route_id::TEXT,
                   route_status_cd,
                   proposed_dttm,
                   dispatched_dttm,
                   completed_dttm,
                   created_dttm,
                   updated_dttm
              FROM rebalance_route
             ORDER BY route_id::TEXT COLLATE "C"
            """
        )
        routes = tuple(cursor.fetchall())
        cursor.execute(
            """
            SELECT route_id::TEXT,
                   sta_id,
                   route_action_type_cd,
                   bike_cnt,
                   visit_no,
                   created_dttm
              FROM rebalance_route_stop
             ORDER BY route_id::TEXT COLLATE "C", visit_no
            """
        )
        stops = tuple(cursor.fetchall())
    connection.rollback()
    return routes, stops


def _proposed_rows(
    connection: Connection[Any],
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    """현재 proposed route와 stop만 metadata 포함해 반환한다."""
    routes, stops = _route_rows(connection)
    proposed_ids = {row[0] for row in routes if row[1] == "proposed"}
    return (
        tuple(row for row in routes if row[0] in proposed_ids),
        tuple(row for row in stops if row[0] in proposed_ids),
    )


def _terminal_rows(
    connection: Connection[Any],
) -> tuple[tuple[tuple[Any, ...], ...], tuple[tuple[Any, ...], ...]]:
    """현재 dispatched/completed route와 stop만 metadata 포함해 반환한다."""
    routes, stops = _route_rows(connection)
    terminal_ids = {row[0] for row in routes if row[1] != "proposed"}
    return (
        tuple(row for row in routes if row[0] in terminal_ids),
        tuple(row for row in stops if row[0] in terminal_ids),
    )


def _object_uri(prefix: str, checksum: str, suffix: str) -> str:
    """Fixture content-addressed S3 URI를 만든다."""
    return f"s3://{_BUCKET}/{prefix}/sha256={checksum}.{suffix}"


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
