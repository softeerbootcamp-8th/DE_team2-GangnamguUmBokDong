"""Clean PostGIS에서 serving plan 네 publication의 원자 release를 검증한다."""

from __future__ import annotations

import os
from collections.abc import Iterator
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime, timedelta
from threading import Barrier
from typing import Any
from zoneinfo import ZoneInfo

import boto3
import pandas as pd
import psycopg
import pytest
from core.gold_publication import (
    ContractViolation,
    InputArtifact,
    PublicationOutcome,
    S3ImmutableObjectStore,
    build_id_set,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from core.inference_catalog import (
    InferenceRevisionRecord,
    InMemoryInferenceRevisionCatalog,
)
from core.inference_snapshot import (
    ImmutableInputRef,
    InferenceSnapshotCounts,
    InferenceSnapshotStatus,
    ParquetOutputRef,
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
from gold import serving_plan as serving_module
from gold.demand import HORIZON_COUNT
from gold.serving_plan import (
    SourceLookbacks,
    prepare_serving_plan as _prepare_serving_plan,
    publish_serving_plan,
)
from gold.source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from psycopg import Connection
from test_gold_station_release_integration import (
    _publish_topology,
    _reset_database,
)
from test_gold_station_release_integration import (
    _put_source_snapshot as _put_station_source,
)
from test_gold_weather_forecast_integration import (
    _KMA_PARTS,
    _weather_rows,
)
from test_gold_weather_forecast_integration import (
    _put_source_snapshot as _put_weather_source,
)

_DATABASE_URL = os.environ.get("GOLD_PUBLICATION_TEST_DATABASE_URL")
_BUCKET = "test-bucket"
_BASE_URI = f"s3://{_BUCKET}/gold-publication"
_KST = ZoneInfo("Asia/Seoul")
_LOOKBACKS = SourceLookbacks(
    master=timedelta(hours=48),
    realtime=timedelta(hours=24),
    short_term=timedelta(hours=24),
    ultra_short=timedelta(hours=6),
)
_EFFECTIVE_CONTRACT_VERSION = f"sha256:{'e' * 64}"
_RELEASE_SHA = "a" * 64
_ENRICHED_MASTER_SHA = "9" * 64


def _shared_plan_identities() -> dict[str, object]:
    """Integration plan과 inference가 공유할 exact identity fixture를 만든다."""
    return {
        "serving_release": ServingReleaseRef(
            byte_sha256=_RELEASE_SHA,
            effective_contract_version=_EFFECTIVE_CONTRACT_VERSION,
            release_version=f"sha256:{_RELEASE_SHA}",
            uri=_uri("serving-release", _RELEASE_SHA, "json"),
        ),
        "station_master_enriched": InputArtifact(
            byte_sha256=_ENRICHED_MASTER_SHA,
            role="station_master_enriched",
            uri=(
                f"s3://{_BUCKET}/silver/station_master_enriched/"
                "dt=2026-08-20/hh=00/0005.parquet"
            ),
        ),
    }


def prepare_serving_plan(*args: Any, **kwargs: Any) -> Any:
    """모든 integration plan writer에 v3 shared identity를 공통 주입한다."""
    kwargs.update(_shared_plan_identities())
    return _prepare_serving_plan(*args, **kwargs)


class _DriftingInferenceCatalog(InMemoryInferenceRevisionCatalog):
    """Final prepare 뒤 locked recheck 직전에 latest correction을 주입한다."""

    def __init__(self) -> None:
        """비활성 drift counter로 catalog를 시작한다."""
        super().__init__()
        self._armed = False
        self._armed_reads = 0

    def arm(self) -> None:
        """다음 두 snapshot 중 두 번째에 correction을 주입한다."""
        self._armed = True
        self._armed_reads = 0

    def snapshot(self, logical_dttm: datetime) -> Any:
        """Armed 두 번째 read에서 same-logical next revision을 claim한다."""
        current = super().snapshot(logical_dttm)
        if not self._armed:
            return current
        self._armed_reads += 1
        if self._armed_reads != 2:
            return current
        self._armed = False
        revision = len(current.records)
        checksum = "f" * 64
        self.claim(
            InferenceRevisionRecord(
                logical_dttm=logical_dttm,
                revision_no=revision,
                manifest_byte_sha256=checksum,
                manifest_uri=_uri("inference/manifests", checksum, "json"),
            )
        )
        return super().snapshot(logical_dttm)


@pytest.fixture
def inference_catalog() -> InMemoryInferenceRevisionCatalog:
    """Producer와 final이 공유하는 test inference revision catalog를 반환한다."""
    return InMemoryInferenceRevisionCatalog()


@pytest.fixture
def gold_connection() -> Iterator[Connection[Any]]:
    """명시적 disposable gold151_* DB를 serving release 테스트마다 비운다."""
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
        pytest.fail("serving plan 통합 테스트는 gold151_ disposable DB만 허용합니다.")
    _reset_database(connection)
    try:
        yield connection
    finally:
        connection.rollback()
        _reset_database(connection)
        connection.close()


def test_clean_bootstrap_activation_replay_correction_and_rollback(
    gold_connection: Connection[Any],
    monkeypatch: pytest.MonkeyPatch,
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """Target 밖 plan부터 네 key publish·replay·correction·rollback을 검증한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1",),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1",),
    )
    master, realtime_v0, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime_v0,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    assert _serving_counts(gold_connection) == (0, 0, 0, 0, 0)
    assert _serving_state(gold_connection) == ()
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )

    first = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        inference_uri,
        inference_sha,
    )
    assert first.result.outcome is PublicationOutcome.PUBLISHED
    assert first.result.publication_keys == (
        "station",
        "station_demand_forecast",
        "station_stock",
        "weather_forecast",
    )
    assert _serving_counts(gold_connection) == (2, 2, 2, HORIZON_COUNT, 13)
    assert _active_ids(gold_connection) == ("ST-1", "ST-2")
    assert _demand_ids(gold_connection) == ("ST-1",)
    initial_state = _serving_state(gold_connection)
    initial_created = _created_times(gold_connection)

    replay = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        inference_uri,
        inference_sha,
    )
    assert replay.result.outcome is PublicationOutcome.EXACT_REPLAY
    assert _serving_state(gold_connection) == initial_state
    assert _created_times(gold_connection) == initial_created

    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station_stock SET parking_bike_tot_cnt = 99 WHERE sta_id = 'ST-1'"
        )
    with pytest.raises(ContractViolation, match="station_stock"):
        _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station_stock SET parking_bike_tot_cnt = 8 WHERE sta_id = 'ST-1'"
        )

    realtime_v1 = _put_station_source(
        client,
        source_id="bike_station_realtime",
        logical=anchor,
        revision=1,
        rows=_realtime_rows(rack_count=21, stock_count=9),
    )
    corrected_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime_v1,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    corrected_inference_uri, corrected_inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=corrected_plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=4.0,
    )
    original_delete = (
        serving_module.weather_forecast._delete_absent_weather_forecast_records
    )

    def fail_after_weather(cursor: Any, records: Any) -> None:
        """네 target mutation 끝에서 예외를 내 전체 transaction rollback을 유도한다."""
        original_delete(cursor, records)
        raise RuntimeError("forced serving release failure")

    with monkeypatch.context() as patch:
        patch.setattr(
            serving_module.weather_forecast,
            "_delete_absent_weather_forecast_records",
            fail_after_weather,
        )
        with pytest.raises(RuntimeError, match="forced serving release failure"):
            _publish_plan(
                gold_connection,
                store,
                catalog,
                inference_catalog,
                corrected_plan.uri,
                corrected_plan.byte_sha256,
                corrected_inference_uri,
                corrected_inference_sha,
            )
    assert _serving_state(gold_connection) == initial_state
    assert _stock_counts(gold_connection) == (("ST-1", 8), ("ST-2", 8))

    corrected = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        corrected_plan.uri,
        corrected_plan.byte_sha256,
        corrected_inference_uri,
        corrected_inference_sha,
    )
    assert corrected.result.outcome is PublicationOutcome.PUBLISHED
    assert {row[1] for row in _serving_state(gold_connection)} == {1}
    assert _stock_counts(gold_connection) == (("ST-1", 9), ("ST-2", 9))
    assert _created_times(gold_connection) == initial_created

    next_anchor = anchor + timedelta(minutes=5)
    realtime_next = _put_station_source(
        client,
        source_id="bike_station_realtime",
        logical=next_anchor,
        revision=0,
        rows=_realtime_rows(rack_count=22, stock_count=10),
    )
    next_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime_next,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    next_inference_uri, next_inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=next_plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=6.0,
    )
    advanced = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        next_plan.uri,
        next_plan.byte_sha256,
        next_inference_uri,
        next_inference_sha,
    )
    assert advanced.result.outcome is PublicationOutcome.PUBLISHED
    with pytest.raises(ContractViolation, match="same-logical latest"):
        _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    stale = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        corrected_plan.uri,
        corrected_plan.byte_sha256,
        corrected_inference_uri,
        corrected_inference_sha,
    )
    assert stale.result.outcome is PublicationOutcome.STALE
    assert _stock_counts(gold_connection) == (("ST-1", 10), ("ST-2", 10))


def test_weather_drift_uses_plan_snapshot_but_inference_failure_is_atomic(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """Plan 뒤 날씨 correction은 허용하고 partial inference는 원자적으로 거부한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1",),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1",),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )
    _put_weather_source(
        client,
        source_id="weather_ultra_short_forecast",
        logical=ultra.manifest.logical_dttm,
        revision=1,
        rows=_weather_rows(anchor, product="ultra_short", temperature=31.0),
        planned_parts=_KMA_PARTS,
    )

    published = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        inference_uri,
        inference_sha,
    )
    assert published.result.outcome is PublicationOutcome.PUBLISHED
    published_counts = _serving_counts(gold_connection)
    published_state = _serving_state(gold_connection)
    partial_document = parse_canonical_json(
        store.read_bytes(
            inference_uri,
            inference_sha,
            require_canonical_json=True,
        )
    )
    assert isinstance(partial_document, dict)
    partial_document["status"] = "partial"
    partial_bytes = canonical_json_bytes(partial_document)
    partial_sha = sha256_hex(partial_bytes)
    partial_uri = _uri("inference/partial", partial_sha, "json")
    store.put_once(
        partial_uri,
        partial_bytes,
        expected_sha256=partial_sha,
        require_canonical_json=True,
    )
    with pytest.raises(ContractViolation):
        _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            partial_uri,
            partial_sha,
        )
    assert _serving_counts(gold_connection) == published_counts
    assert _serving_state(gold_connection) == published_state


def test_inference_only_and_weather_corrections_mutate_published_subsets(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Inference-only와 weather correction이 replay target을 다시 쓰지 않는다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1",),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1",),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    first_uri, first_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )
    _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        first_uri,
        first_sha,
    )

    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=7.0,
    )
    state_before_mixed = _serving_state(gold_connection)
    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station_stock SET parking_bike_tot_cnt = 99 WHERE sta_id = 'ST-1'"
        )
    with pytest.raises(ContractViolation, match="station_stock"):
        _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    assert _serving_state(gold_connection) == state_before_mixed
    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE station_stock SET parking_bike_tot_cnt = 8 WHERE sta_id = 'ST-1'"
        )

    def fail_replay_mutation(*_args: Any, **_kwargs: Any) -> None:
        """Replay target mutation이 호출되면 mixed subset 계약을 실패시킨다."""
        raise AssertionError("replay target mutation")

    with monkeypatch.context() as patch:
        patch.setattr(
            serving_module.station_release,
            "_upsert_station",
            fail_replay_mutation,
        )
        patch.setattr(
            serving_module.station_release,
            "_replace_station_stock",
            fail_replay_mutation,
        )
        patch.setattr(
            serving_module.weather_forecast,
            "_upsert_weather_forecast_records",
            fail_replay_mutation,
        )
        inference_corrected = _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    assert inference_corrected.result.outcome is PublicationOutcome.PUBLISHED
    revisions = {row[0]: row[1] for row in _serving_state(gold_connection)}
    assert revisions == {
        "station": 0,
        "station_demand_forecast": 1,
        "station_stock": 0,
        "weather_forecast": 0,
    }

    ultra_v1 = _put_weather_source(
        client,
        source_id="weather_ultra_short_forecast",
        logical=ultra.manifest.logical_dttm,
        revision=1,
        rows=_weather_rows(anchor, product="ultra_short", temperature=31.0),
        planned_parts=_KMA_PARTS,
    )
    weather_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra_v1,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    weather_inference_uri, weather_inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=weather_plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=7.0,
    )
    with monkeypatch.context() as patch:
        patch.setattr(
            serving_module.station_release,
            "_upsert_station",
            fail_replay_mutation,
        )
        patch.setattr(
            serving_module.station_release,
            "_replace_station_stock",
            fail_replay_mutation,
        )
        weather_corrected = _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            weather_plan.uri,
            weather_plan.byte_sha256,
            weather_inference_uri,
            weather_inference_sha,
        )
    assert weather_corrected.result.outcome is PublicationOutcome.PUBLISHED
    revisions = {row[0]: row[1] for row in _serving_state(gold_connection)}
    assert revisions == {
        "station": 0,
        "station_demand_forecast": 2,
        "station_stock": 0,
        "weather_forecast": 1,
    }
    assert _stock_counts(gold_connection) == (("ST-1", 8), ("ST-2", 8))


def test_cross_plan_inference_and_locked_catalog_drift_fail_closed(
    gold_connection: Connection[Any],
) -> None:
    """다른 plan inference와 DB lock 뒤 생긴 latest correction을 모두 거부한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    source_catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    inference_catalog = _DriftingInferenceCatalog()
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1",),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1",),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=source_catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    other_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=source_catalog,
        object_base_uri=f"{_BASE_URI}-other",
        source_lookbacks=_LOOKBACKS,
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )

    with pytest.raises(ContractViolation, match="serving_plan ref"):
        _publish_plan(
            gold_connection,
            store,
            source_catalog,
            inference_catalog,
            other_plan.uri,
            other_plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    assert _serving_counts(gold_connection) == (0, 0, 0, 0, 0)
    assert _serving_state(gold_connection) == ()

    inference_catalog.arm()
    with pytest.raises(ContractViolation, match="same-logical latest"):
        _publish_plan(
            gold_connection,
            store,
            source_catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    assert _serving_counts(gold_connection) == (0, 0, 0, 0, 0)
    assert _serving_state(gold_connection) == ()


def test_model_unsupported_station_activates_with_demand_empty(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """13h weather만 있으면 model 미지원 station을 active로 두고 EMPTY demand를 claim한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(store, ModelKind.RENTAL, ())
    return_model, return_support = _put_model_snapshot(store, ModelKind.RETURN, ())
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )

    result = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        inference_uri,
        inference_sha,
    )

    assert result.result.outcome is PublicationOutcome.PUBLISHED
    assert _active_ids(gold_connection) == ("ST-1", "ST-2")
    assert _demand_ids(gold_connection) == ()
    state = {row[0]: (row[1], row[2]) for row in _serving_state(gold_connection)}
    assert state["station_demand_forecast"] == (0, 0)


def test_missing_current_stock_candidate_stays_inactive(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """13h weather와 model support가 있어도 current parking 결측이면 활성화하지 않는다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1", "ST-2"),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1", "ST-2"),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
        missing_stock_ids=("ST-2",),
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    assert tuple(
        (record.sta_id, record.is_active)
        for record in _planned_station_rows(store, plan)
    ) == (("ST-1", True), ("ST-2", False))
    assert parse_expected_ids(store, plan.expected_sta_ids) == ("ST-1",)
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )

    result = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        plan.uri,
        plan.byte_sha256,
        inference_uri,
        inference_sha,
    )

    assert result.result.outcome is PublicationOutcome.PUBLISHED
    assert _active_ids(gold_connection) == ("ST-1",)
    assert _stock_counts(gold_connection) == (("ST-1", 8),)
    assert _demand_ids(gold_connection) == ("ST-1",)
    assert _serving_counts(gold_connection) == (2, 1, 1, HORIZON_COUNT, 13)


def test_missing_current_stock_keeps_existing_active_station_by_lifecycle_policy(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """Stock gate는 신규·재활성만 막고 기존 active는 lifecycle 정책으로 유지한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1", "ST-2"),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1", "ST-2"),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    initial_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    initial_inference_uri, initial_inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=initial_plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )
    _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        initial_plan.uri,
        initial_plan.byte_sha256,
        initial_inference_uri,
        initial_inference_sha,
    )
    assert _active_ids(gold_connection) == ("ST-1", "ST-2")

    next_anchor = anchor + timedelta(minutes=5)
    missing_stock_realtime = _put_station_source(
        client,
        source_id="bike_station_realtime",
        logical=next_anchor,
        revision=0,
        rows=_realtime_rows(
            rack_count=21,
            stock_count=9,
            missing_stock_ids=("ST-2",),
        ),
    )
    lifecycle_plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=missing_stock_realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    assert parse_expected_ids(
        store,
        lifecycle_plan.plan.activation_ready_sta_ids,
    ) == ("ST-1",)
    assert tuple(
        (record.sta_id, record.is_active)
        for record in _planned_station_rows(store, lifecycle_plan)
    ) == (("ST-1", True), ("ST-2", True))
    assert parse_expected_ids(store, lifecycle_plan.expected_sta_ids) == (
        "ST-1",
        "ST-2",
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=lifecycle_plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=1.0,
    )

    result = _publish_plan(
        gold_connection,
        store,
        catalog,
        inference_catalog,
        lifecycle_plan.uri,
        lifecycle_plan.byte_sha256,
        inference_uri,
        inference_sha,
    )

    assert result.result.outcome is PublicationOutcome.PUBLISHED
    assert _active_ids(gold_connection) == ("ST-1", "ST-2")
    assert _stock_counts(gold_connection) == (("ST-1", 9),)
    assert _demand_ids(gold_connection) == ("ST-1", "ST-2")
    assert _serving_counts(gold_connection) == (
        2,
        2,
        1,
        2 * HORIZON_COUNT,
        13,
    )


def test_missing_weather_candidate_stays_inactive_and_topology_drift_fails(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """13h가 없는 candidate를 미활성화하고 plan 뒤 topology 변경을 fail-closed 한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1", "ST-2"),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1", "ST-2"),
    )
    second_point = (126.9845, 37.4982)
    master = _put_station_source(
        client,
        source_id="bike_station_master",
        logical=anchor - timedelta(hours=1),
        revision=0,
        rows=_master_rows(second_point=second_point),
    )
    realtime = _put_station_source(
        client,
        source_id="bike_station_realtime",
        logical=anchor,
        revision=0,
        rows=_realtime_rows(
            rack_count=20,
            stock_count=8,
            second_point=second_point,
        ),
    )
    source_logical = anchor - timedelta(minutes=10)
    short = _put_weather_source(
        client,
        source_id="weather_short_term_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="short_term", temperature=27.0),
        planned_parts=_KMA_PARTS,
    )
    ultra = _put_weather_source(
        client,
        source_id="weather_ultra_short_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="ultra_short", temperature=28.0),
        planned_parts=_KMA_PARTS,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )
    planned_station = _planned_station_rows(store, plan)
    assert tuple(
        (record.sta_id, record.weather_grid_id, record.is_active)
        for record in planned_station
    ) == (("ST-1", "61_126", True), ("ST-2", "60_125", False))
    selected_center = planned_station[0].dispatch_center_id
    with gold_connection.transaction(), gold_connection.cursor() as cursor:
        cursor.execute(
            "UPDATE dispatch_center SET is_active = false "
            "WHERE dispatch_center_id = %s",
            (selected_center,),
        )

    with pytest.raises(ContractViolation):
        _publish_plan(
            gold_connection,
            store,
            catalog,
            inference_catalog,
            plan.uri,
            plan.byte_sha256,
            inference_uri,
            inference_sha,
        )
    assert _serving_counts(gold_connection) == (0, 0, 0, 0, 0)
    assert _serving_state(gold_connection) == ()


def test_same_plan_concurrent_publish_serializes_to_publish_and_replay(
    gold_connection: Connection[Any],
    inference_catalog: InMemoryInferenceRevisionCatalog,
) -> None:
    """동일 plan 동시 게시를 한 PUBLISHED와 한 exact replay로 직렬화한다."""
    client = boto3.client("s3", region_name="us-east-1")
    store = S3ImmutableObjectStore(client)
    catalog = S3SourceSnapshotCatalog(client, store, bucket=_BUCKET)
    anchor = _safe_anchor()
    _publish_topology(gold_connection, store, anchor)
    rental_model, rental_support = _put_model_snapshot(
        store,
        ModelKind.RENTAL,
        ("ST-1",),
    )
    return_model, return_support = _put_model_snapshot(
        store,
        ModelKind.RETURN,
        ("ST-1",),
    )
    master, realtime, short, ultra = _put_release_sources(
        client,
        anchor=anchor,
        realtime_revision=0,
        rack_count=20,
        stock_count=8,
    )
    plan = prepare_serving_plan(
        gold_connection,
        store,
        master_artifact=master,
        realtime_candidate=realtime,
        short_term_artifact=short,
        ultra_short_artifact=ultra,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        source_catalog=catalog,
        object_base_uri=_BASE_URI,
        source_lookbacks=_LOOKBACKS,
    )
    inference_uri, inference_sha = _put_inference(
        store,
        inference_catalog=inference_catalog,
        plan=plan,
        rental_model=rental_model,
        return_model=return_model,
        prediction_offset=0.0,
    )
    barrier = Barrier(2)

    def publish_from_new_connection() -> PublicationOutcome:
        """별도 DB connection으로 같은 sealed release를 동시에 게시한다."""
        assert _DATABASE_URL is not None
        connection = psycopg.connect(_DATABASE_URL)
        try:
            worker_client = boto3.client("s3", region_name="us-east-1")
            worker_store = S3ImmutableObjectStore(worker_client)
            worker_catalog = S3SourceSnapshotCatalog(
                worker_client,
                worker_store,
                bucket=_BUCKET,
            )
            barrier.wait(timeout=10)
            return _publish_plan(
                connection,
                worker_store,
                worker_catalog,
                inference_catalog,
                plan.uri,
                plan.byte_sha256,
                inference_uri,
                inference_sha,
            ).result.outcome
        finally:
            connection.close()

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            executor.map(lambda _: publish_from_new_connection(), range(2))
        )

    assert sorted(outcomes) == sorted(
        (PublicationOutcome.PUBLISHED, PublicationOutcome.EXACT_REPLAY)
    )
    assert _serving_counts(gold_connection) == (2, 2, 2, HORIZON_COUNT, 13)
    assert {row[1] for row in _serving_state(gold_connection)} == {0}


def _put_release_sources(
    client: Any,
    *,
    anchor: datetime,
    realtime_revision: int,
    rack_count: int,
    stock_count: int,
    missing_stock_ids: tuple[str, ...] = (),
) -> tuple[
    SourceManifestArtifact,
    SourceManifestArtifact,
    SourceManifestArtifact,
    SourceManifestArtifact,
]:
    """한 anchor의 complete master·realtime·두 weather source를 저장한다."""
    source_logical = anchor - timedelta(minutes=10)
    master = _put_station_source(
        client,
        source_id="bike_station_master",
        logical=anchor - timedelta(hours=1),
        revision=0,
        rows=_master_rows(),
    )
    realtime = _put_station_source(
        client,
        source_id="bike_station_realtime",
        logical=anchor,
        revision=realtime_revision,
        rows=_realtime_rows(
            rack_count=rack_count,
            stock_count=stock_count,
            missing_stock_ids=missing_stock_ids,
        ),
    )
    short = _put_weather_source(
        client,
        source_id="weather_short_term_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="short_term", temperature=27.0),
        planned_parts=_KMA_PARTS,
    )
    ultra = _put_weather_source(
        client,
        source_id="weather_ultra_short_forecast",
        logical=source_logical,
        revision=0,
        rows=_weather_rows(anchor, product="ultra_short", temperature=28.0),
        planned_parts=_KMA_PARTS,
    )
    return master, realtime, short, ultra


def _master_rows(
    *,
    second_point: tuple[float, float] | None = None,
) -> tuple[dict[str, object], ...]:
    """같은 weather grid를 쓰는 두 valid master row를 반환한다."""
    point_by_id = {
        "ST-1": (127.0473, 37.5172),
        "ST-2": second_point or (127.0473, 37.5172),
    }
    return tuple(
        {
            "RNTLS_ID": station_id,
            "ADDR1": "서울시 강남구 테스트로",
            "ADDR2": None,
            "LAT": point_by_id[station_id][1],
            "LOT": point_by_id[station_id][0],
        }
        for station_id in ("ST-1", "ST-2")
    )


def _realtime_rows(
    *,
    rack_count: int,
    stock_count: int,
    second_point: tuple[float, float] | None = None,
    missing_stock_ids: tuple[str, ...] = (),
) -> tuple[dict[str, object], ...]:
    """두 station의 same-anchor serving-valid realtime row를 반환한다."""
    missing = set(missing_stock_ids)
    point_by_id = {
        "ST-1": (127.0473, 37.5172),
        "ST-2": second_point or (127.0473, 37.5172),
    }
    return tuple(
        {
            "stationId": station_id,
            "stationName": f"강남 대여소 {station_id}",
            "rackTotCnt": rack_count,
            "parkingBikeTotCnt": None if station_id in missing else stock_count,
            "shared": 0,
            "stationLatitude": point_by_id[station_id][1],
            "stationLongitude": point_by_id[station_id][0],
        }
        for station_id in ("ST-1", "ST-2")
    )


def _put_model_snapshot(
    store: S3ImmutableObjectStore,
    kind: ModelKind,
    support_ids: tuple[str, ...],
) -> tuple[Any, IdSetArtifactRef]:
    """Plan과 inference가 함께 pin할 model manifest·support ID set을 저장한다."""
    support = build_id_set(support_ids)
    support_ref = _put_id_set(store, f"model/{kind.value}/support", support)
    artifacts = tuple(
        ModelArtifact(
            byte_sha256=sha256_hex(f"{kind.value}:{role}".encode()),
            role=role,
            uri=_uri(
                f"model/{kind.value}/{role}",
                sha256_hex(f"{kind.value}:{role}".encode()),
                "txt" if role.startswith("booster_") else "json",
            ),
        )
        for role in MODEL_ARTIFACT_ROLES
    )
    manifest = build_model_snapshot_manifest(
        model_kind=kind,
        effective_contract_version=_EFFECTIVE_CONTRACT_VERSION,
        artifacts=artifacts,
        support_sta_ids=support_ref,
    )
    uri = _uri(f"model/{kind.value}/manifest", manifest.sha256, "json")
    store.put_once(
        uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    return manifest, support_ref


def _put_inference(
    store: S3ImmutableObjectStore,
    *,
    inference_catalog: InMemoryInferenceRevisionCatalog,
    plan: Any,
    rental_model: Any,
    return_model: Any,
    prediction_offset: float,
) -> tuple[str, str]:
    """Plan dependency·expected ref를 그대로 쓰는 success/EMPTY inference를 저장한다."""
    expected_ids = parse_expected_ids(store, plan.expected_sta_ids)
    assert plan.plan.serving_release is not None
    assert plan.plan.station_master_enriched is not None
    release_ref = plan.plan.serving_release
    if expected_ids:
        output_bytes = _inference_output_bytes(
            plan.plan.logical_dttm,
            expected_ids,
            prediction_offset=prediction_offset,
        )
        output_sha = sha256_hex(output_bytes)
        output_uri = _uri("inference/output", output_sha, "parquet")
        store.put_once(output_uri, output_bytes, expected_sha256=output_sha)
        status = InferenceSnapshotStatus.SUCCEEDED
        output = ParquetOutputRef(
            output_sha,
            len(expected_ids) * HORIZON_COUNT,
            output_uri,
        )
        inputs = (
            ImmutableInputRef(
                byte_sha256="1" * 64,
                role="feature_snapshot",
                uri=_uri("inference/input", "1" * 64, "parquet"),
            ),
            ImmutableInputRef(
                byte_sha256=plan.plan.station_master_enriched.byte_sha256,
                role="station_master_enriched",
                uri=_uri(
                    "inference/input/station-master-enriched",
                    plan.plan.station_master_enriched.byte_sha256,
                    "parquet",
                ),
            ),
        )
    else:
        status = InferenceSnapshotStatus.EMPTY
        output = None
        inputs = ()
    row_count = len(expected_ids) * HORIZON_COUNT
    revision_no = len(inference_catalog.snapshot(plan.plan.logical_dttm).records)
    manifest = build_inference_snapshot_manifest(
        logical_dttm=plan.plan.logical_dttm,
        revision_no=revision_no,
        status=status,
        producer_version="serving-plan-integration-v1",
        serving_release=release_ref,
        serving_plan=plan.serving_plan_ref,
        rental_model_manifest=build_model_manifest_ref(
            rental_model,
            _uri("model/rental/manifest", rental_model.sha256, "json"),
        ),
        return_model_manifest=build_model_manifest_ref(
            return_model,
            _uri("model/return/manifest", return_model.sha256, "json"),
        ),
        station_dependency=plan.station_dependency,
        inputs=inputs,
        expected_sta_ids=plan.expected_sta_ids,
        counts=InferenceSnapshotCounts(
            len(expected_ids),
            len(expected_ids),
            0,
            row_count,
            row_count,
            0,
        ),
        horizon_count=HORIZON_COUNT,
        output=output,
    )
    uri = _uri("inference/manifests", manifest.sha256, "json")
    store.put_once(
        uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    inference_catalog.claim(
        InferenceRevisionRecord(
            logical_dttm=manifest.logical_dttm,
            revision_no=manifest.revision_no,
            manifest_byte_sha256=manifest.sha256,
            manifest_uri=uri,
        )
    )
    return uri, manifest.sha256


def parse_expected_ids(
    store: S3ImmutableObjectStore,
    reference: IdSetArtifactRef,
) -> tuple[str, ...]:
    """Plan expected ID set actual bytes를 integration producer 입력으로 읽는다."""
    from core.gold_publication import parse_id_set

    return parse_id_set(
        store.read_bytes(
            reference.uri,
            reference.byte_sha256,
            require_canonical_json=True,
        )
    ).ids


def _put_id_set(
    store: S3ImmutableObjectStore,
    prefix: str,
    id_set: Any,
) -> IdSetArtifactRef:
    """Canonical ID set을 model-compatible content-addressed URI에 저장한다."""
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
    prediction_offset: float,
) -> bytes:
    """Station×12 exact authority inference Parquet bytes를 만든다."""
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
                    "rental_pred_mean": prediction_offset + 1.0,
                    "rental_pred_p10": 0.25,
                    "rental_pred_p50": prediction_offset + 0.5,
                    "rental_pred_p90": prediction_offset + 1.5,
                    "return_pred_mean": 2.5,
                    "return_pred_p10": 1.0,
                    "return_pred_p50": 2.5,
                    "return_pred_p90": 3.5,
                }
            )
    table = canonicalize_inference_output_table(
        pd.DataFrame(rows),
        logical_dttm=logical_dttm,
        expected_sta_ids=build_id_set(station_ids),
    )
    return serialize_inference_output_parquet(table)


def _publish_plan(
    connection: Connection[Any],
    store: S3ImmutableObjectStore,
    catalog: S3SourceSnapshotCatalog,
    inference_catalog: InMemoryInferenceRevisionCatalog,
    plan_uri: str,
    plan_sha: str,
    inference_uri: str,
    inference_sha: str,
) -> Any:
    """Integration identity로 coordinated final API를 호출한다."""
    return publish_serving_plan(
        connection,
        store,
        plan_uri=plan_uri,
        plan_sha256=plan_sha,
        inference_manifest_uri=inference_uri,
        inference_manifest_sha256=inference_sha,
        inference_catalog=inference_catalog,
        source_catalog=catalog,
    )


def _serving_counts(connection: Connection[Any]) -> tuple[int, int, int, int, int]:
    """Station active와 네 serving target row count를 한 snapshot으로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT (SELECT count(*) FROM station),
                   (SELECT count(*) FROM station WHERE is_active),
                   (SELECT count(*) FROM station_stock),
                   (SELECT count(*) FROM station_demand_forecast),
                   (SELECT count(*) FROM weather_forecast)
            """
        )
        row = cursor.fetchone()
    connection.rollback()
    assert row is not None
    return row


def _serving_state(
    connection: Connection[Any],
) -> tuple[tuple[str, int, int], ...]:
    """네 coordinated publication의 revision·row count를 key 순서로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            """
            SELECT publication_key, revision_no, published_row_cnt
              FROM gold_meta.publication_state
             WHERE publication_key = ANY(%s::TEXT[])
             ORDER BY publication_key
            """,
            (
                [
                    "station",
                    "station_stock",
                    "station_demand_forecast",
                    "weather_forecast",
                ],
            ),
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _active_ids(connection: Connection[Any]) -> tuple[str, ...]:
    """Active station ID를 canonical 순서로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT sta_id FROM station WHERE is_active ORDER BY sta_id COLLATE "C"'
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(row[0] for row in rows)


def _demand_ids(connection: Connection[Any]) -> tuple[str, ...]:
    """Demand row가 있는 distinct station ID를 canonical 순서로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            "SELECT sta_id FROM station_demand_forecast "
            'GROUP BY sta_id ORDER BY sta_id COLLATE "C"'
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(row[0] for row in rows)


def _stock_counts(connection: Connection[Any]) -> tuple[tuple[str, int], ...]:
    """Station별 stock count를 canonical 순서로 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute(
            'SELECT sta_id, parking_bike_tot_cnt FROM station_stock ORDER BY sta_id COLLATE "C"'
        )
        rows = cursor.fetchall()
    connection.rollback()
    return tuple(rows)


def _created_times(connection: Connection[Any]) -> tuple[datetime, datetime, datetime]:
    """Correction에서 보존해야 하는 stock·demand·weather 최초 생성시각을 읽는다."""
    with connection.cursor() as cursor:
        cursor.execute("SELECT min(created_dttm) FROM station_stock")
        stock = cursor.fetchone()
        cursor.execute("SELECT min(created_dttm) FROM station_demand_forecast")
        demand = cursor.fetchone()
        cursor.execute("SELECT min(created_dttm) FROM weather_forecast")
        weather = cursor.fetchone()
    connection.rollback()
    assert stock is not None and demand is not None and weather is not None
    return stock[0], demand[0], weather[0]


def _planned_station_rows(
    store: S3ImmutableObjectStore,
    plan: Any,
) -> tuple[Any, ...]:
    """Plan의 sealed station output actual rows를 읽는다."""
    reference = next(
        item
        for item in plan.plan.prepared_publications
        if item.publication_key == "station"
    )
    prepared = serving_module._read_prepared_manifest(store, reference)
    output = serving_module.station_release._single_output(
        prepared.manifest,
        "station",
    )
    return serving_module.station_release._station_records_from_parquet(
        store.read_bytes(output.uri, output.byte_sha256)
    )


def _safe_anchor() -> datetime:
    """Business time 제한 안에서 다음 5분 tick도 같은 forecast hour인 anchor를 만든다."""
    return datetime.now(UTC).replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=1
    )


def _uri(prefix: str, checksum: str, extension: str) -> str:
    """Core model/inference contract가 요구하는 sha256 filename URI를 만든다."""
    return f"s3://{_BUCKET}/{prefix}/sha256={checksum}.{extension}"
