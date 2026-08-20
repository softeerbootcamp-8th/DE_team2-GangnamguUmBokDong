"""Gold demand publisher의 immutable inference와 원자 reconcile 계약을 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import psycopg
import pytest
from core.gold_publication import (
    PublicationOutcome,
    S3ImmutableObjectStore,
    build_id_set,
    sha256_hex,
)
from core.inference_snapshot import (
    ImmutableInputRef,
    InferenceSnapshotCounts,
    InferenceSnapshotStatus,
    ParquetOutputRef,
    ServingPlanRef,
    ServingReleaseRef,
    build_inference_snapshot_manifest,
    build_model_manifest_ref,
    canonicalize_inference_output_table,
    serialize_inference_output_parquet,
)
from core.model_snapshot import (
    MODEL_ARTIFACT_ROLES,
    IdSetArtifactRef,
    ModelArtifact,
    ModelKind,
    build_id_set_artifact_ref,
    build_model_snapshot_manifest,
)
from gold import demand as demand_module
from gold.demand import HORIZON_COUNT, publish_station_demand_forecast
from gold.state import load_dependencies
from psycopg import Connection

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_BUCKET = "test-bucket"
_BASE_URI = f"s3://{_BUCKET}/gold-publication"
_KST = ZoneInfo("Asia/Seoul")
_EFFECTIVE_PROFILE_BYTES = b'{"schema_version":"effective-profile-test-v1"}'
_EFFECTIVE_PROFILE_SHA = sha256_hex(_EFFECTIVE_PROFILE_BYTES)
_EFFECTIVE_CONTRACT_VERSION = f"sha256:{_EFFECTIVE_PROFILE_SHA}"


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 demand 통합 테스트 전후 비운다."""
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
        pytest.fail("demand 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        _reset_database(connection)
        connection.close()


def test_demand_publish_replay_stale_correction_empty_and_atomic_rollback(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Clean PostGIS에서 demand의 version·EMPTY·rollback·created 보존을 검증한다."""
    store = S3ImmutableObjectStore(boto3.client("s3", region_name="us-east-1"))
    anchor = datetime.now(UTC).replace(second=0, microsecond=0) - timedelta(minutes=30)
    _insert_station_topology(gold_connection, anchor)
    station_dependency = load_dependencies(gold_connection, ("station",))[0]

    first_uri, first_sha = _put_inference_snapshot(
        store,
        logical_dttm=anchor,
        revision_no=0,
        expected_sta_ids=("ST-1",),
        support_sta_ids=("ST-1", "ST-9"),
        station_dependency=station_dependency,
        rental_offset=0.5,
    )
    first = _publish(gold_connection, store, first_uri, first_sha)
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    first_rows = _demand_rows(gold_connection)
    assert len(first_rows) == HORIZON_COUNT
    assert {row[0] for row in first_rows} == {"ST-1"}
    assert {row[2] for row in first_rows} == {2}
    assert _demand_state(gold_connection) == (anchor, 0, HORIZON_COUNT)
    assert tuple(
        item.role for item in first.evidence[0].input_fingerprint.input_artifacts
    ) == (
        "inference_output",
        "rental_model_manifest",
        "return_model_manifest",
    )

    replay = _publish(gold_connection, store, first_uri, first_sha)
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _demand_rows(gold_connection) == first_rows

    correction_uri, correction_sha = _put_inference_snapshot(
        store,
        logical_dttm=anchor,
        revision_no=1,
        expected_sta_ids=("ST-1",),
        support_sta_ids=("ST-1", "ST-9"),
        station_dependency=station_dependency,
        rental_offset=4.5,
    )
    correction = _publish(
        gold_connection,
        store,
        correction_uri,
        correction_sha,
    )
    assert correction.result.outcome is PublicationOutcome.PUBLISHED
    corrected_rows = _demand_rows(gold_connection)
    assert {row[2] for row in corrected_rows} == {6}
    assert tuple((row[0], row[1], row[4]) for row in corrected_rows) == tuple(
        (row[0], row[1], row[4]) for row in first_rows
    )
    assert _demand_state(gold_connection) == (anchor, 1, HORIZON_COUNT)

    stale_uri, stale_sha = _put_inference_snapshot(
        store,
        logical_dttm=anchor - timedelta(minutes=5),
        revision_no=0,
        expected_sta_ids=("ST-1",),
        support_sta_ids=("ST-1",),
        station_dependency=station_dependency,
        rental_offset=8.5,
    )
    stale = _publish(gold_connection, store, stale_uri, stale_sha)
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _demand_rows(gold_connection) == corrected_rows

    rollback_uri, rollback_sha = _put_inference_snapshot(
        store,
        logical_dttm=anchor,
        revision_no=2,
        expected_sta_ids=("ST-1",),
        support_sta_ids=("ST-1",),
        station_dependency=station_dependency,
        rental_offset=10.5,
    )
    reconcile = demand_module._reconcile_demand_records

    def fail_after_reconcile(cursor: Any, records: Any) -> None:
        """Target mutation 뒤 예외를 내 state와 rows의 동시 rollback을 확인한다."""
        reconcile(cursor, records)
        raise RuntimeError("forced demand reconcile failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            demand_module,
            "_reconcile_demand_records",
            fail_after_reconcile,
        )
        with pytest.raises(RuntimeError, match="forced demand reconcile failure"):
            _publish(gold_connection, store, rollback_uri, rollback_sha)
    assert _demand_rows(gold_connection) == corrected_rows
    assert _demand_state(gold_connection) == (anchor, 1, HORIZON_COUNT)

    empty_anchor = anchor + timedelta(minutes=5)
    _deactivate_station_and_advance_state(gold_connection, empty_anchor)
    empty_dependency = load_dependencies(gold_connection, ("station",))[0]
    empty_uri, empty_sha = _put_inference_snapshot(
        store,
        logical_dttm=empty_anchor,
        revision_no=0,
        expected_sta_ids=(),
        support_sta_ids=("ST-1",),
        station_dependency=empty_dependency,
        rental_offset=0.0,
    )
    emptied = _publish(gold_connection, store, empty_uri, empty_sha)
    assert emptied.result.outcome is PublicationOutcome.PUBLISHED
    assert _demand_rows(gold_connection) == ()
    assert _demand_state(gold_connection) == (empty_anchor, 0, 0)


def _put_inference_snapshot(
    store: S3ImmutableObjectStore,
    *,
    logical_dttm: datetime,
    revision_no: int,
    expected_sta_ids: tuple[str, ...],
    support_sta_ids: tuple[str, ...],
    station_dependency: Any,
    rental_offset: float,
) -> tuple[str, str]:
    """Model·ID·output·inference manifest 전체를 content-addressed S3에 기록한다."""
    rental_model = _put_model_snapshot(store, ModelKind.RENTAL, support_sta_ids)
    return_model = _put_model_snapshot(store, ModelKind.RETURN, support_sta_ids)
    expected_ids = build_id_set(expected_sta_ids)
    expected_ref = _put_id_set(store, "inference/expected", expected_ids)
    release_bytes = b'{"release":"integration-v1"}'
    release_sha = sha256_hex(release_bytes)
    serving_release = ServingReleaseRef(
        byte_sha256=release_sha,
        effective_contract_version=_EFFECTIVE_CONTRACT_VERSION,
        release_version=f"sha256:{release_sha}",
        uri=_uri("serving-release", release_sha, "json"),
    )
    plan_bytes = b'{"plan":"demand-integration-v1"}'
    plan_sha = sha256_hex(plan_bytes)
    serving_plan = ServingPlanRef(
        byte_sha256=plan_sha,
        uri=_uri("serving-plan", plan_sha, "json"),
    )

    if expected_sta_ids:
        output_bytes = _inference_output_bytes(
            logical_dttm,
            expected_sta_ids,
            rental_offset=rental_offset,
        )
        output_sha = sha256_hex(output_bytes)
        output_uri = _uri("inference/output", output_sha, "parquet")
        store.put_once(output_uri, output_bytes, expected_sha256=output_sha)
        row_count = len(expected_sta_ids) * HORIZON_COUNT
        status = InferenceSnapshotStatus.SUCCEEDED
        output = ParquetOutputRef(output_sha, row_count, output_uri)
        inputs = (
            ImmutableInputRef(
                "a" * 64,
                "feature_snapshot",
                f"s3://{_BUCKET}/inference/input/sha256={'a' * 64}.parquet",
            ),
        )
    else:
        row_count = 0
        status = InferenceSnapshotStatus.EMPTY
        output = None
        inputs = ()
    station_count = len(expected_sta_ids)
    manifest = build_inference_snapshot_manifest(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=status,
        producer_version="inference-producer-integration-v1",
        serving_release=serving_release,
        serving_plan=serving_plan,
        rental_model_manifest=build_model_manifest_ref(
            rental_model,
            _model_uri(rental_model),
        ),
        return_model_manifest=build_model_manifest_ref(
            return_model,
            _model_uri(return_model),
        ),
        station_dependency=station_dependency,
        inputs=inputs,
        expected_sta_ids=expected_ref,
        counts=InferenceSnapshotCounts(
            station_count,
            station_count,
            0,
            row_count,
            row_count,
            0,
        ),
        horizon_count=HORIZON_COUNT,
        output=output,
    )
    manifest_uri = _uri("inference/manifest", manifest.sha256, "json")
    store.put_once(
        manifest_uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    return manifest_uri, manifest.sha256


def _put_model_snapshot(
    store: S3ImmutableObjectStore,
    kind: ModelKind,
    support_sta_ids: tuple[str, ...],
) -> Any:
    """Support ID set을 소유한 inference-ready model manifest를 기록한다."""
    support = build_id_set(support_sta_ids)
    support_ref = _put_id_set(store, f"model/{kind.value}/support", support)
    artifacts = []
    for role in MODEL_ARTIFACT_ROLES:
        extension = "txt" if role.startswith("booster_") else "json"
        checksum = (
            _EFFECTIVE_PROFILE_SHA
            if role == "effective_profile"
            else sha256_hex(f"{kind.value}:{role}".encode())
        )
        artifacts.append(
            ModelArtifact(
                byte_sha256=checksum,
                role=role,
                uri=_uri(f"model/{kind.value}/{role}", checksum, extension),
            )
        )
    manifest = build_model_snapshot_manifest(
        model_kind=kind,
        effective_contract_version=_EFFECTIVE_CONTRACT_VERSION,
        artifacts=tuple(artifacts),
        support_sta_ids=support_ref,
    )
    uri = _model_uri(manifest)
    store.put_once(
        uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    return manifest


def _put_id_set(
    store: S3ImmutableObjectStore,
    prefix: str,
    id_set: Any,
) -> IdSetArtifactRef:
    """Canonical Gold ID set을 저장하고 exact artifact ref를 반환한다."""
    uri = _uri(prefix, id_set.sha256, "json")
    store.put_once(
        uri,
        id_set.canonical_bytes,
        expected_sha256=id_set.sha256,
        require_canonical_json=True,
    )
    return build_id_set_artifact_ref(id_set, uri)


def _inference_output_bytes(
    logical_dttm: datetime,
    station_ids: tuple[str, ...],
    *,
    rental_offset: float,
) -> bytes:
    """Extra audit column이 있는 producer rows를 exact authority Parquet으로 만든다."""
    local_base = logical_dttm.astimezone(_KST)
    rows = []
    for station_id in station_ids:
        for horizon in range(1, HORIZON_COUNT + 1):
            target = local_base + timedelta(hours=horizon - 1)
            rows.append(
                {
                    "station_id": station_id,
                    "date": target.date().isoformat(),
                    "hour": target.hour,
                    "minute": target.minute,
                    "horizon": horizon,
                    "rental_pred_mean": rental_offset + 1.0,
                    "return_pred_mean": 2.5,
                    "rental_pred_p50": 999.0,
                }
            )
    table = canonicalize_inference_output_table(
        pd.DataFrame(rows),
        logical_dttm=logical_dttm,
        expected_sta_ids=build_id_set(station_ids),
    )
    return serialize_inference_output_parquet(table)


def _publish(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    manifest_uri: str,
    manifest_sha256: str,
) -> Any:
    """Integration fixture의 inference identity로 demand publisher를 실행한다."""
    return publish_station_demand_forecast(
        connection,
        store,
        inference_manifest_uri=manifest_uri,
        inference_manifest_sha256=manifest_sha256,
        object_base_uri=_BASE_URI,
    )


def _insert_station_topology(
    connection: Connection[Any],
    anchor: datetime,
) -> None:
    """한 active station과 exact station publication state를 clean DB에 만든다."""
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
                'demand integration fixture',
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
            ) VALUES ('station', %s, 0, %s, %s, %s, 1)
            """,
            (
                anchor - timedelta(minutes=5),
                f"s3://{_BUCKET}/station/publication-{'1' * 64}.json",
                "2" * 64,
                "3" * 64,
            ),
        )


def _deactivate_station_and_advance_state(
    connection: Connection[Any],
    logical_dttm: datetime,
) -> None:
    """Station topology와 dependency state를 같은 transaction에서 다음 window로 옮긴다."""
    with connection.transaction(), connection.cursor() as cursor:
        cursor.execute("UPDATE station SET is_active = false WHERE sta_id = 'ST-1'")
        cursor.execute(
            """
            UPDATE gold_meta.publication_state
               SET logical_dttm = %s,
                   revision_no = 0,
                   manifest_uri = %s,
                   artifact_set_sha256 = %s,
                   input_fingerprint_sha256 = %s,
                   published_row_cnt = 1
             WHERE publication_key = 'station'
            """,
            (
                logical_dttm,
                f"s3://{_BUCKET}/station/publication-{'4' * 64}.json",
                "5" * 64,
                "6" * 64,
            ),
        )


def _demand_rows(
    connection: Connection[Any],
) -> tuple[tuple[str, datetime, int, int, datetime], ...]:
    """Demand key·수량·최초 생성시각을 deterministic 순서로 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT sta_id,
                   predicted_dttm,
                   predicted_rent_cnt,
                   predicted_rtn_cnt,
                   created_dttm
              FROM station_demand_forecast
             ORDER BY sta_id COLLATE "C", predicted_dttm
            """
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _demand_state(connection: Connection[Any]) -> tuple[datetime, int, int]:
    """Demand publication state의 logical·revision·row count를 반환한다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT logical_dttm, revision_no, published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = 'station_demand_forecast'
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _model_uri(manifest: Any) -> str:
    """Model manifest의 content-addressed fixture URI를 반환한다."""
    return _uri(f"model/{manifest.model_kind.value}/manifest", manifest.sha256, "json")


def _uri(prefix: str, checksum: str, extension: str) -> str:
    """Contract helper가 요구하는 sha256 filename S3 URI를 만든다."""
    return f"s3://{_BUCKET}/{prefix}/sha256={checksum}.{extension}"


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
