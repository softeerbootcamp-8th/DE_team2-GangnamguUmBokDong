"""station master·realtime authority를 station·stock으로 원자 게시한다."""

from __future__ import annotations

import re
import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Literal

import pyarrow as pa
from core.gold_publication import (
    Artifact,
    ImmutableObjectStore,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PreparedPublication,
    PublicationManifest,
    StationRealtimeWindow,
    StationRealtimeWindowSet,
    StationRelocationApproval,
    VerifiedPublicationEvidence,
    build_station_realtime_window_set,
    parse_input_fingerprint,
    parse_station_realtime_window_set,
    parse_station_relocation_approval,
    validate_station_conditional_inputs,
    validate_station_stock_release,
)
from core.gold_publication.errors import ContractViolation
from core.source_snapshot import (
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    parse_source_snapshot_manifest,
)
from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .common import (
    OutputObject,
    PublicationExecution,
    build_prepared_publication,
    materialize_publication,
    parquet_bytes,
    publish_verified,
    read_parquet_bytes,
    read_source_snapshot_payload,
    source_snapshot_parquet,
    store_input_payload,
)
from .source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from .source_policy import validate_source_snapshot_policy
from .state import (
    PublicationStateRecord,
    load_dependencies,
    load_publication_state,
    publication_state_locked,
    read_state_manifest,
)
from .station import (
    CENTER_ASSIGNMENT_VERSION,
    GRID_CONVERSION_VERSION,
    STATION_POLICY_VERSION,
    DispatchCenterReference,
    MasterSnapshot,
    RealtimeWindowSnapshot,
    StationProjection,
    StationRecord,
    build_station_projection,
)
from .station_stock import (
    STATION_STOCK_POLICY_VERSION,
    StationStockProjection,
    StationStockRecord,
    build_station_stock_projection,
)
from .versioning import PublicationCandidate, allocate_revision

BIKE_STATION_MASTER_SOURCE_ID = "bike_station_master"
BIKE_STATION_REALTIME_SOURCE_ID = "bike_station_realtime"
STATION_PUBLISHER_VERSION = "gold-station-publisher-v1"
STATION_STOCK_PUBLISHER_VERSION = "gold-station-stock-publisher-v1"

_STATION_SCHEMA = pa.schema(
    (
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("sta_nm", pa.string(), nullable=False),
        pa.field("sta_addr", pa.string(), nullable=False),
        pa.field("hold_cnt", pa.int32(), nullable=False),
        pa.field("sta_point_ewkb", pa.binary(), nullable=False),
        pa.field("sta_point_source_cd", pa.string(), nullable=False),
        pa.field("weather_grid_id", pa.string(), nullable=False),
        pa.field("dispatch_center_id", pa.string(), nullable=False),
        pa.field("master_base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("last_seen_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("is_active", pa.bool_(), nullable=False),
    )
)
_STATION_STOCK_SCHEMA = pa.schema(
    (
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("parking_bike_tot_cnt", pa.int32(), nullable=False),
    )
)
_STATION_ID = re.compile(r"ST-[0-9]+\Z")


@dataclass(frozen=True, slots=True)
class _TopologySnapshot:
    """station projection이 참조하는 exact Gold topology를 표현한다."""

    weather_grid_ids: tuple[str, ...]
    dispatch_centers: tuple[DispatchCenterReference, ...]


@dataclass(frozen=True, slots=True)
class _PriorStation:
    """현재 station state의 manifest·fingerprint·output을 결합한다."""

    state: PublicationStateRecord
    manifest: PublicationManifest
    fingerprint: InputFingerprint
    output_artifact: Artifact
    records: tuple[StationRecord, ...]


@dataclass(frozen=True, slots=True)
class _PriorStock:
    """현재 stock state의 sealed manifest·fingerprint·candidate를 결합한다."""

    state: PublicationStateRecord
    prepared: PreparedPublication
    candidate_input: InputArtifact
    records: tuple[StationStockRecord, ...]


@dataclass(frozen=True, slots=True)
class _StationInputs:
    """station fingerprint의 direct input artifact를 role별로 고정한다."""

    master: InputArtifact
    window_set: InputArtifact
    previous: InputArtifact | None
    relocation: InputArtifact | None

    @property
    def all(self) -> tuple[InputArtifact, ...]:
        """contract에 넣을 direct input artifact tuple을 반환한다."""
        optional = tuple(
            value for value in (self.previous, self.relocation) if value is not None
        )
        return (self.master, self.window_set, *optional)


@dataclass(slots=True)
class _LockedProjection:
    """locked validator가 재구성한 mutation 대상을 임시 보관한다."""

    station: StationProjection | None = None
    stock: StationStockProjection | None = None
    route_invalidating_station_ids: tuple[str, ...] = ()


def publish_station_realtime_release(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    master_artifact: SourceManifestArtifact,
    realtime_candidate: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    master_lookback: timedelta,
    realtime_lookback: timedelta,
    relocation_approval_payload: bytes | None = None,
    station_publisher_version: str = STATION_PUBLISHER_VERSION,
    stock_publisher_version: str = STATION_STOCK_PUBLISHER_VERSION,
) -> PublicationExecution:
    """authoritative realtime window를 station·stock 두 key로 원자 게시한다.

    station과 station_stock은 같은 realtime candidate identity를 각각의
    fingerprint에 남기고 한 transaction에서 함께 claim한다. 신규·재활성
    station은 #153의 weather·demand bootstrap이 없으므로 비활성으로 보존한다.
    """
    _require_catalog(source_catalog)
    _require_source_artifact(master_artifact, BIKE_STATION_MASTER_SOURCE_ID)
    _require_source_artifact(realtime_candidate, BIKE_STATION_REALTIME_SOURCE_ID)
    _require_positive_lookback(master_lookback, "master_lookback")
    _require_positive_lookback(realtime_lookback, "realtime_lookback")
    candidate_logical = realtime_candidate.manifest.logical_dttm
    if master_artifact.manifest.logical_dttm > candidate_logical:
        raise ContractViolation(
            "station master snapshot이 realtime candidate보다 미래입니다."
        )

    prior = _load_prior_station(connection, object_store)
    if prior is not None:
        _require_master_monotonic(object_store, prior, master_artifact)
    dependencies = load_dependencies(connection, ("dispatch_center", "weather_grid"))
    realtime_artifacts = source_catalog.recent_windows(
        BIKE_STATION_REALTIME_SOURCE_ID,
        candidate_logical,
        limit=3,
        lookback=realtime_lookback,
    )
    if not _same_source_artifact(realtime_artifacts[0], realtime_candidate):
        raise ContractViolation(
            "realtime candidate가 최신 authoritative correction이 아닙니다."
        )
    latest_master = source_catalog.latest_at_or_before(
        BIKE_STATION_MASTER_SOURCE_ID,
        candidate_logical,
        lookback=master_lookback,
    )
    if not _same_source_artifact(latest_master, master_artifact):
        raise ContractViolation(
            "station master가 candidate 이전 최신 authority가 아닙니다."
        )

    window_set = _build_window_set(realtime_artifacts, realtime_candidate)
    replay = _existing_realtime_replay(
        connection,
        object_store,
        prior=prior,
        dependencies=dependencies,
        master_artifact=master_artifact,
        realtime_candidate=realtime_candidate,
        window_set=window_set,
        relocation_approval_payload=relocation_approval_payload,
    )
    if replay is not None:
        station_prepared, stock_prepared = replay
        return _publish_station_pair(
            connection,
            object_store,
            station_prepared=station_prepared,
            stock_prepared=stock_prepared,
            prior=prior,
            source_catalog=source_catalog,
            expected_master=master_artifact,
            expected_windows=realtime_artifacts,
            master_lookback=master_lookback,
            realtime_lookback=realtime_lookback,
            mode="realtime",
        )
    current_change = _classify_current_candidate_station_change(
        connection,
        object_store,
        prior=prior,
        dependencies=dependencies,
        master_artifact=master_artifact,
        realtime_candidate=realtime_candidate,
        window_set=window_set,
        relocation_approval_payload=relocation_approval_payload,
    )
    if current_change == "lifecycle":
        return publish_station_lifecycle_correction(
            connection,
            object_store,
            source_catalog=source_catalog,
            object_base_uri=object_base_uri,
            realtime_lookback=realtime_lookback,
            relocation_approval_payload=relocation_approval_payload,
            publisher_version=station_publisher_version,
        )
    if current_change == "other":
        raise ContractViolation(
            "current realtime candidate의 station만 변경되었습니다. "
            "master·topology·approval 변경은 해당 station-only correction API로 분리하세요."
        )

    window_input = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        role="station_realtime_window_set",
        payload=window_set.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    master_input = _source_input(master_artifact, "bike_station_master_manifest")
    previous_input = _prior_projection_input(prior)
    provisional_inputs = _StationInputs(
        master=master_input,
        window_set=window_input,
        previous=previous_input,
        relocation=None,
    )
    approval = _parse_optional_approval(relocation_approval_payload)
    topology, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=provisional_inputs,
        direct_payloads=_direct_payloads(
            object_store,
            master_artifact=master_artifact,
            window_set=window_set,
            prior=prior,
            relocation_payload=None,
        ),
        relocation_approval=approval,
    )
    relocation_input = _materialize_relocation_input(
        object_store,
        object_base_uri=object_base_uri,
        payload=relocation_approval_payload,
        projection=projection,
    )
    station_inputs = _StationInputs(
        master=master_input,
        window_set=window_input,
        previous=previous_input,
        relocation=relocation_input,
    )
    direct_payloads = _direct_payloads(
        object_store,
        master_artifact=master_artifact,
        window_set=window_set,
        prior=prior,
        relocation_payload=relocation_approval_payload,
    )
    # relocation role이 추가되어도 projection은 같은 actual approval bytes로 재검증한다.
    _, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=station_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
        expected_topology=topology,
    )
    candidate_rows = _realtime_rows(
        object_store,
        realtime_candidate,
        role="bike_station_realtime_manifest",
    )
    stock_projection = build_station_stock_projection(
        candidate_rows,
        published_station_ids=tuple(record.sta_id for record in projection.records),
        candidate_logical_dttm=candidate_logical,
    )
    _require_nonempty_release(projection, stock_projection)

    station_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        dependencies=dependencies,
        input_artifacts=station_inputs.all,
        parameters=_station_parameters(),
        outputs=(
            OutputObject(
                role="station",
                payload=_station_records_to_parquet(projection.records),
                row_count=len(projection.records),
            ),
        ),
    )
    stock_input = _source_input(
        realtime_candidate,
        "bike_station_realtime_manifest",
    )
    stock_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_stock",
        input_artifacts=(stock_input,),
        parameters=(
            Parameter("station_stock_policy_version", STATION_STOCK_POLICY_VERSION),
        ),
        outputs=(
            OutputObject(
                role="station_stock",
                payload=_stock_records_to_parquet(stock_projection.records),
                row_count=len(stock_projection.records),
            ),
        ),
    )
    station_prepared = _prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station",
        logical_dttm=candidate_logical,
        publisher_version=station_publisher_version,
        row_count=len(projection.records),
        materials=station_materials,
    )
    stock_prepared = _prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station_stock",
        logical_dttm=candidate_logical,
        publisher_version=stock_publisher_version,
        row_count=len(stock_projection.records),
        materials=stock_materials,
    )
    validate_station_stock_release(
        station_prepared.input_fingerprint,
        stock_prepared.input_fingerprint,
        window_set,
    )
    return _publish_station_pair(
        connection,
        object_store,
        station_prepared=station_prepared,
        stock_prepared=stock_prepared,
        prior=prior,
        source_catalog=source_catalog,
        expected_master=master_artifact,
        expected_windows=realtime_artifacts,
        master_lookback=master_lookback,
        realtime_lookback=realtime_lookback,
        mode="realtime",
    )


def publish_station_master_correction(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    master_artifact: SourceManifestArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    realtime_lookback: timedelta,
    relocation_approval_payload: bytes | None = None,
    publisher_version: str = STATION_PUBLISHER_VERSION,
) -> PublicationExecution:
    """master-only correction을 station key만 claim해 게시한다.

    현재 station manifest의 realtime window-set을 그대로 재사용하므로
    ``last_seen_dttm``을 새 realtime window로 전진시키지 않고 station_stock을
    claim하거나 변경하지 않는다.
    """
    _require_catalog(source_catalog)
    _require_source_artifact(master_artifact, BIKE_STATION_MASTER_SOURCE_ID)
    _require_positive_lookback(realtime_lookback, "realtime_lookback")
    prior = _load_prior_station(connection, object_store)
    if prior is None:
        raise ContractViolation(
            "master-only correction은 기존 station state가 필요합니다."
        )
    _require_master_monotonic(object_store, prior, master_artifact)
    dependencies = load_dependencies(connection, ("dispatch_center", "weather_grid"))
    window_input = _input_by_role(prior.fingerprint, "station_realtime_window_set")
    window_payload = object_store.read_bytes(
        window_input.uri,
        window_input.byte_sha256,
        require_canonical_json=True,
    )
    window_set = parse_station_realtime_window_set(window_payload)
    realtime_artifacts = _catalog_artifacts_for_window_set(
        source_catalog,
        window_set,
        lookback=realtime_lookback,
    )
    latest_master = source_catalog.exact_window(
        BIKE_STATION_MASTER_SOURCE_ID,
        master_artifact.manifest.logical_dttm,
    )
    if not _same_source_artifact(latest_master, master_artifact):
        raise ContractViolation(
            "master-only input이 해당 logical window의 최신 correction이 아닙니다."
        )

    replay = _existing_master_replay(
        object_store,
        prior=prior,
        dependencies=dependencies,
        master_artifact=master_artifact,
        window_set=window_set,
        relocation_approval_payload=relocation_approval_payload,
    )
    if replay is not None:
        return _publish_station_single(
            connection,
            object_store,
            station_prepared=replay,
            prior=prior,
            source_catalog=source_catalog,
            expected_master=master_artifact,
            expected_windows=realtime_artifacts,
            realtime_lookback=realtime_lookback,
        )

    master_input = _source_input(master_artifact, "bike_station_master_manifest")
    provisional_inputs = _StationInputs(
        master=master_input,
        window_set=window_input,
        previous=_prior_projection_input(prior),
        relocation=None,
    )
    approval = _parse_optional_approval(relocation_approval_payload)
    direct_payloads = {
        master_input.uri: master_artifact.payload,
        window_input.uri: window_payload,
        prior.output_artifact.uri: object_store.read_bytes(
            prior.output_artifact.uri,
            prior.output_artifact.byte_sha256,
        ),
    }
    topology, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=provisional_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
    )
    relocation_input = _materialize_relocation_input(
        object_store,
        object_base_uri=object_base_uri,
        payload=relocation_approval_payload,
        projection=projection,
    )
    station_inputs = _StationInputs(
        master=master_input,
        window_set=window_input,
        previous=_prior_projection_input(prior),
        relocation=relocation_input,
    )
    if relocation_input is not None and relocation_approval_payload is not None:
        direct_payloads[relocation_input.uri] = relocation_approval_payload
    _, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=station_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
        expected_topology=topology,
    )
    _require_master_only_last_seen(prior.records, projection.records)
    station_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        dependencies=dependencies,
        input_artifacts=station_inputs.all,
        parameters=_station_parameters(),
        outputs=(
            OutputObject(
                role="station",
                payload=_station_records_to_parquet(projection.records),
                row_count=len(projection.records),
            ),
        ),
    )
    station_prepared = _prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station",
        logical_dttm=prior.state.logical_dttm,
        publisher_version=publisher_version,
        row_count=len(projection.records),
        materials=station_materials,
    )
    return _publish_station_single(
        connection,
        object_store,
        station_prepared=station_prepared,
        prior=prior,
        source_catalog=source_catalog,
        expected_master=master_artifact,
        expected_windows=realtime_artifacts,
        realtime_lookback=realtime_lookback,
    )


def publish_station_lifecycle_correction(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    realtime_lookback: timedelta,
    relocation_approval_payload: bytes | None = None,
    publisher_version: str = STATION_PUBLISHER_VERSION,
) -> PublicationExecution:
    """current stock candidate를 유지하며 과거 window correction만 station에 반영한다.

    현재 ``station_stock`` state의 actual realtime manifest가 window-set candidate와
    exact인 경우에만 station key를 correction claim한다. stock과 station의
    candidate·``last_seen_dttm``은 전진시키지 않고, 중간 1..2개 window의
    최신 correction으로 lifecycle streak만 재계산한다.
    """
    _require_catalog(source_catalog)
    _require_positive_lookback(realtime_lookback, "realtime_lookback")
    prior = _load_prior_station(connection, object_store)
    if prior is None:
        raise ContractViolation(
            "lifecycle correction은 기존 station state가 필요합니다."
        )
    stock = _load_prior_stock(connection, object_store)
    if stock is None:
        raise ContractViolation(
            "lifecycle correction은 현재 station_stock state가 필요합니다."
        )
    if stock.state.logical_dttm != prior.state.logical_dttm:
        raise ContractViolation(
            "station·stock current logical release가 서로 다릅니다."
        )

    current_candidate = source_catalog.exact_window(
        BIKE_STATION_REALTIME_SOURCE_ID,
        stock.state.logical_dttm,
    )
    if not _artifact_matches_source(stock.candidate_input, current_candidate):
        raise ContractViolation(
            "current stock candidate의 correction이 바뀌어 realtime two-key release가 필요합니다."
        )
    realtime_artifacts = source_catalog.recent_windows(
        BIKE_STATION_REALTIME_SOURCE_ID,
        stock.state.logical_dttm,
        limit=3,
        lookback=realtime_lookback,
    )
    if not _same_source_artifact(realtime_artifacts[0], current_candidate):
        raise ContractViolation(
            "lifecycle window-set candidate가 current stock input과 다릅니다."
        )
    window_set = _build_window_set(realtime_artifacts, current_candidate)
    prior_window_input = _input_by_role(
        prior.fingerprint,
        "station_realtime_window_set",
    )
    prior_window_payload = object_store.read_bytes(
        prior_window_input.uri,
        prior_window_input.byte_sha256,
        require_canonical_json=True,
    )
    master_input = _input_by_role(
        prior.fingerprint,
        "bike_station_master_manifest",
    )
    master_artifact = _source_artifact_from_input(
        object_store,
        master_input,
        BIKE_STATION_MASTER_SOURCE_ID,
    )
    dependencies = load_dependencies(connection, ("dispatch_center", "weather_grid"))

    if parse_station_realtime_window_set(prior_window_payload) == window_set:
        if not _same_optional_payload(
            object_store,
            prior.fingerprint,
            "station_relocation_approval",
            relocation_approval_payload,
        ):
            raise ContractViolation(
                "lifecycle replay의 relocation approval이 prior와 다릅니다."
            )
        if prior.fingerprint.dependencies != dependencies:
            raise ContractViolation(
                "lifecycle replay 중 topology dependency가 바뀌었습니다."
            )
        return _publish_station_lifecycle_single(
            connection,
            object_store,
            station_prepared=PreparedPublication(
                prior.manifest,
                prior.state.manifest_uri,
                prior.fingerprint,
            ),
            prior=prior,
            stock=stock,
            source_catalog=source_catalog,
            expected_master=master_artifact,
            expected_windows=realtime_artifacts,
            realtime_lookback=realtime_lookback,
        )

    window_input = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        role="station_realtime_window_set",
        payload=window_set.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    previous_input = _prior_projection_input(prior)
    provisional = _StationInputs(master_input, window_input, previous_input, None)
    approval = _parse_optional_approval(relocation_approval_payload)
    direct_payloads = {
        master_input.uri: master_artifact.payload,
        window_input.uri: window_set.canonical_bytes,
        prior.output_artifact.uri: object_store.read_bytes(
            prior.output_artifact.uri,
            prior.output_artifact.byte_sha256,
        ),
    }
    topology, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=provisional,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
    )
    relocation_input = _materialize_relocation_input(
        object_store,
        object_base_uri=object_base_uri,
        payload=relocation_approval_payload,
        projection=projection,
    )
    inputs = _StationInputs(
        master_input,
        window_input,
        previous_input,
        relocation_input,
    )
    if relocation_input is not None and relocation_approval_payload is not None:
        direct_payloads[relocation_input.uri] = relocation_approval_payload
    _, projection = _project_station_with_connection(
        connection,
        object_store,
        inputs=inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
        expected_topology=topology,
    )
    _require_master_only_last_seen(prior.records, projection.records)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        dependencies=dependencies,
        input_artifacts=inputs.all,
        parameters=_station_parameters(),
        outputs=(
            OutputObject(
                role="station",
                payload=_station_records_to_parquet(projection.records),
                row_count=len(projection.records),
            ),
        ),
    )
    prepared = _prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station",
        logical_dttm=prior.state.logical_dttm,
        publisher_version=publisher_version,
        row_count=len(projection.records),
        materials=materials,
    )
    return _publish_station_lifecycle_single(
        connection,
        object_store,
        station_prepared=prepared,
        prior=prior,
        stock=stock,
        source_catalog=source_catalog,
        expected_master=master_artifact,
        expected_windows=realtime_artifacts,
        realtime_lookback=realtime_lookback,
    )


def _publish_station_pair(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    station_prepared: PreparedPublication,
    stock_prepared: PreparedPublication,
    prior: _PriorStation | None,
    source_catalog: S3SourceSnapshotCatalog,
    expected_master: SourceManifestArtifact,
    expected_windows: tuple[SourceManifestArtifact, ...],
    master_lookback: timedelta,
    realtime_lookback: timedelta,
    mode: Literal["realtime"],
) -> PublicationExecution:
    """station·stock evidence를 검증하고 locked projection으로 함께 변경한다."""
    del mode
    holder = _LockedProjection()
    prior_stock = _load_prior_stock(connection, object_store)
    station_validator = _station_staging_validator(
        connection,
        object_store,
        expected_current_records=() if prior is None else prior.records,
    )
    stock_validator = _stock_staging_validator(
        connection,
        object_store,
        expected_current_records=() if prior_stock is None else prior_stock.records,
    )

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """lock 안에서 prior·source·topology·sealed output을 다시 검증한다."""
        evidence_by_key = _evidence_by_key(evidence, ("station", "station_stock"))
        previous_records = _validate_prior_locked(cursor, object_store, prior)
        _validate_prior_stock_locked(cursor, object_store, prior_stock)
        if prior is not None:
            _require_master_monotonic(object_store, prior, expected_master)
        _validate_latest_sources(
            source_catalog,
            expected_master=expected_master,
            expected_windows=expected_windows,
            mode="realtime",
            master_lookback=master_lookback,
            realtime_lookback=realtime_lookback,
        )
        station_projection = _locked_station_projection(
            cursor,
            object_store,
            evidence_by_key["station"],
        )
        stock_projection = _stock_projection_from_evidence(
            object_store,
            evidence_by_key["station_stock"],
            published_station_ids=tuple(
                record.sta_id for record in station_projection.records
            ),
        )
        window_set = _window_set_from_fingerprint(
            object_store,
            evidence_by_key["station"].input_fingerprint,
        )
        validate_station_stock_release(
            evidence_by_key["station"].input_fingerprint,
            evidence_by_key["station_stock"].input_fingerprint,
            window_set,
        )
        _validate_sealed_station_output(
            object_store,
            evidence_by_key["station"],
            station_projection,
        )
        _validate_sealed_stock_output(
            object_store,
            evidence_by_key["station_stock"],
            stock_projection,
        )
        holder.station = station_projection
        holder.stock = stock_projection
        holder.route_invalidating_station_ids = _route_invalidating_station_ids(
            previous_records,
            station_projection.records,
        )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """station upsert·stock 전체 교체를 publication claim과 묶는다."""
        _evidence_by_key(evidence, ("station", "station_stock"))
        if holder.station is None or holder.stock is None:
            raise ContractViolation("locked station release projection이 없습니다.")
        _delete_affected_proposed_routes(
            cursor,
            holder.route_invalidating_station_ids,
        )
        _upsert_station(cursor, holder.station.records)
        _replace_station_stock(cursor, holder.stock.records)

    return publish_verified(
        connection,
        (
            (station_prepared, station_validator),
            (stock_prepared, stock_validator),
        ),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _publish_station_single(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    station_prepared: PreparedPublication,
    prior: _PriorStation,
    source_catalog: S3SourceSnapshotCatalog,
    expected_master: SourceManifestArtifact,
    expected_windows: tuple[SourceManifestArtifact, ...],
    realtime_lookback: timedelta,
) -> PublicationExecution:
    """master-only station evidence를 stock 없이 claim·mutation한다."""
    holder = _LockedProjection()
    station_validator = _station_staging_validator(
        connection,
        object_store,
        expected_current_records=prior.records,
    )

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """lock 안에서 master correction의 prior·window·last-seen을 재검증한다."""
        [station_evidence] = _evidence_by_key(evidence, ("station",)).values()
        previous_records = _validate_prior_locked(cursor, object_store, prior)
        _require_master_monotonic(object_store, prior, expected_master)
        _validate_latest_sources(
            source_catalog,
            expected_master=expected_master,
            expected_windows=expected_windows,
            mode="master",
            master_lookback=None,
            realtime_lookback=realtime_lookback,
        )
        projection = _locked_station_projection(
            cursor,
            object_store,
            station_evidence,
        )
        _require_master_only_last_seen(prior.records, projection.records)
        _validate_sealed_station_output(
            object_store,
            station_evidence,
            projection,
        )
        holder.station = projection
        holder.route_invalidating_station_ids = _route_invalidating_station_ids(
            previous_records,
            projection.records,
        )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """master-only projection으로 station만 upsert한다."""
        _evidence_by_key(evidence, ("station",))
        if holder.station is None:
            raise ContractViolation("locked master correction projection이 없습니다.")
        _delete_affected_proposed_routes(
            cursor,
            holder.route_invalidating_station_ids,
        )
        _upsert_station(cursor, holder.station.records)

    return publish_verified(
        connection,
        ((station_prepared, station_validator),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _publish_station_lifecycle_single(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    station_prepared: PreparedPublication,
    prior: _PriorStation,
    stock: _PriorStock,
    source_catalog: S3SourceSnapshotCatalog,
    expected_master: SourceManifestArtifact,
    expected_windows: tuple[SourceManifestArtifact, ...],
    realtime_lookback: timedelta,
) -> PublicationExecution:
    """stock state를 추가 lock한 뒤 station lifecycle correction만 게시한다."""
    holder = _LockedProjection()
    station_validator = _station_staging_validator(
        connection,
        object_store,
        expected_current_records=prior.records,
    )

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """station 뒤 stock key를 lock해 current candidate를 고정한다."""
        [station_evidence] = _evidence_by_key(evidence, ("station",)).values()
        previous_records = _validate_prior_locked(cursor, object_store, prior)
        cursor.execute(
            """
            SELECT publication_key
              FROM gold_meta.publication_state
             WHERE publication_key = %s
             FOR SHARE
            """,
            ("station_stock",),
        )
        if cursor.fetchone() != ("station_stock",):
            raise ContractViolation("locked lifecycle station_stock state가 없습니다.")
        locked_stock = publication_state_locked(cursor, "station_stock")
        if locked_stock != stock.state:
            raise ContractViolation(
                "lifecycle 준비 후 station_stock state가 바뀌었습니다."
            )
        _validate_prior_stock_locked(cursor, object_store, stock)
        actual_stock = _prepared_from_state(object_store, locked_stock)
        actual_candidate = _input_by_role(
            actual_stock.input_fingerprint,
            "bike_station_realtime_manifest",
        )
        expected_candidate = _window_from_artifact(expected_windows[0])
        if (actual_candidate.uri, actual_candidate.byte_sha256) != (
            expected_candidate.uri,
            expected_candidate.byte_sha256,
        ):
            raise ContractViolation(
                "locked stock input이 lifecycle candidate와 다릅니다."
            )
        _validate_latest_sources(
            source_catalog,
            expected_master=expected_master,
            expected_windows=expected_windows,
            mode="lifecycle",
            master_lookback=None,
            realtime_lookback=realtime_lookback,
        )
        projection = _locked_station_projection(
            cursor,
            object_store,
            station_evidence,
        )
        _require_master_only_last_seen(prior.records, projection.records)
        _validate_sealed_station_output(
            object_store,
            station_evidence,
            projection,
        )
        holder.station = projection
        holder.route_invalidating_station_ids = _route_invalidating_station_ids(
            previous_records,
            projection.records,
        )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """corrected lifecycle station projection만 upsert한다."""
        _evidence_by_key(evidence, ("station",))
        if holder.station is None:
            raise ContractViolation("locked lifecycle projection이 없습니다.")
        _delete_affected_proposed_routes(
            cursor,
            holder.route_invalidating_station_ids,
        )
        _upsert_station(cursor, holder.station.records)

    return publish_verified(
        connection,
        ((station_prepared, station_validator),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _station_staging_validator(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    expected_current_records: tuple[StationRecord, ...],
    activation_ready_station_ids: tuple[str, ...] = (),
    validate_current_target: bool = True,
) -> Any:
    """actual station input·output을 DB geography로 재구성하는 validator를 만든다."""
    if type(validate_current_target) is not bool:
        raise ContractViolation("validate_current_target은 bool이어야 합니다.")

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """sealed station bytes를 direct·nested immutable input에서 재검증한다."""
        if publication.manifest.publication_key != "station":
            raise ContractViolation("station staging publication key가 다릅니다.")
        if validate_current_target:
            _validate_db_station_short(connection, expected_current_records)
        inputs = _station_inputs_from_fingerprint(publication.input_fingerprint)
        _, projection = _project_station_with_connection(
            connection,
            object_store,
            inputs=inputs,
            direct_payloads=payloads,
            relocation_approval=_approval_from_inputs(inputs, payloads),
            activation_ready_station_ids=activation_ready_station_ids,
        )
        validate_station_conditional_inputs(
            publication.input_fingerprint,
            previous_state_exists=inputs.previous is not None,
            relocation_applied=projection.relocation_applied,
        )
        _validate_prepared_station_output(publication, payloads, projection)
        return {
            "last_seen_dttm": tuple(
                record.last_seen_dttm for record in projection.records
            ),
            "master_base_dttm": tuple(
                record.master_base_dttm for record in projection.records
            ),
        }

    return validate_staging


def _validate_db_station_short(
    connection: Connection[Any],
    expected: tuple[StationRecord, ...],
) -> None:
    """evidence 검증 직전 DB station을 current immutable projection과 대조한다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "station drift read는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        actual = _db_station_records(cursor)
    if actual != expected:
        raise ContractViolation(
            "DB station target이 current immutable projection과 다릅니다."
        )


def _stock_staging_validator(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    expected_current_records: tuple[StationStockRecord, ...],
    validate_current_target: bool = True,
) -> Any:
    """actual realtime manifest·Silver와 stock output을 대조하는 validator를 만든다."""
    if type(validate_current_target) is not bool:
        raise ContractViolation("validate_current_target은 bool이어야 합니다.")

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """stock output의 candidate·schema·row 전체를 재검증한다."""
        if publication.manifest.publication_key != "station_stock":
            raise ContractViolation("stock staging publication key가 다릅니다.")
        if validate_current_target:
            _validate_db_stock_short(connection, expected_current_records)
        manifest_input = _input_by_role(
            publication.input_fingerprint,
            "bike_station_realtime_manifest",
        )
        snapshot = read_source_snapshot_payload(
            object_store,
            manifest_artifact=manifest_input,
            verified_payloads=payloads,
            expected_source_id=BIKE_STATION_REALTIME_SOURCE_ID,
        )
        validate_source_snapshot_policy(snapshot.manifest)
        if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
            raise ContractViolation(
                "station_stock은 confirmed EMPTY source를 게시할 수 없습니다."
            )
        output = _single_output(publication.manifest, "station_stock")
        actual_records = _stock_records_from_parquet(payloads[output.uri])
        expected = build_station_stock_projection(
            tuple(source_snapshot_parquet(snapshot).to_pylist()),
            published_station_ids=tuple(record.sta_id for record in actual_records),
            candidate_logical_dttm=snapshot.manifest.logical_dttm,
        )
        if expected.records != actual_records:
            raise ContractViolation(
                "stock output이 actual realtime projection과 다릅니다."
            )
        if not actual_records:
            raise ContractViolation("station_stock EMPTY는 SSOT에서 금지됩니다.")
        return {"base_dttm": tuple(record.base_dttm for record in actual_records)}

    return validate_staging


def _validate_db_stock_short(
    connection: Connection[Any],
    expected: tuple[StationStockRecord, ...],
) -> None:
    """evidence 검증 직전 DB stock을 current immutable projection과 대조한다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "stock drift read는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        cursor.execute(
            """
            SELECT sta_id, base_dttm, parking_bike_tot_cnt
              FROM station_stock
             ORDER BY sta_id
            """
        )
        actual = tuple(
            StationStockRecord(row[0], row[1], row[2]) for row in cursor.fetchall()
        )
    if actual != expected:
        raise ContractViolation(
            "DB station_stock target이 current immutable projection과 다릅니다."
        )


def _locked_station_projection(
    cursor: Cursor[tuple[Any, ...]],
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    *,
    activation_ready_station_ids: tuple[str, ...] = (),
) -> StationProjection:
    """topology lock 안 actual fingerprint에서 station projection을 다시 만든다."""
    inputs = _station_inputs_from_fingerprint(evidence.input_fingerprint)
    payloads = _read_direct_inputs(object_store, inputs)
    approval = _approval_from_inputs(inputs, payloads)
    topology = _load_topology_locked(cursor)
    projection = _project_station(
        object_store,
        inputs=inputs,
        direct_payloads=payloads,
        topology=topology,
        relocation_approval=approval,
        distance_meters=_postgis_distance(cursor),
        activation_ready_station_ids=activation_ready_station_ids,
    )
    validate_station_conditional_inputs(
        evidence.input_fingerprint,
        previous_state_exists=inputs.previous is not None,
        relocation_applied=projection.relocation_applied,
    )
    return projection


def _stock_projection_from_evidence(
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    *,
    published_station_ids: tuple[str, ...],
) -> StationStockProjection:
    """locked release의 stock fingerprint actual bytes로 projection을 재구성한다."""
    manifest_input = _input_by_role(
        evidence.input_fingerprint,
        "bike_station_realtime_manifest",
    )
    manifest_payload = object_store.read_bytes(
        manifest_input.uri,
        manifest_input.byte_sha256,
        require_canonical_json=True,
    )
    snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=manifest_input,
        verified_payloads={manifest_input.uri: manifest_payload},
        expected_source_id=BIKE_STATION_REALTIME_SOURCE_ID,
    )
    validate_source_snapshot_policy(snapshot.manifest)
    if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ContractViolation(
            "locked station_stock candidate가 SUCCEEDED가 아닙니다."
        )
    projection = build_station_stock_projection(
        tuple(source_snapshot_parquet(snapshot).to_pylist()),
        published_station_ids=published_station_ids,
        candidate_logical_dttm=snapshot.manifest.logical_dttm,
    )
    if not projection.records:
        raise ContractViolation("station_stock EMPTY는 SSOT에서 금지됩니다.")
    return projection


def _project_station_with_connection(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    inputs: _StationInputs,
    direct_payloads: Mapping[str, bytes],
    relocation_approval: StationRelocationApproval | None,
    expected_topology: _TopologySnapshot | None = None,
    activation_ready_station_ids: tuple[str, ...] = (),
) -> tuple[_TopologySnapshot, StationProjection]:
    """짧은 read transaction의 PostGIS 거리로 station projection을 만든다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "station projection은 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        topology = _load_topology_locked(cursor)
        if expected_topology is not None and topology != expected_topology:
            raise ContractViolation("station staging 중 Gold topology가 바뀌었습니다.")
        projection = _project_station(
            object_store,
            inputs=inputs,
            direct_payloads=direct_payloads,
            topology=topology,
            relocation_approval=relocation_approval,
            distance_meters=_postgis_distance(cursor),
            activation_ready_station_ids=activation_ready_station_ids,
        )
    return topology, projection


def _project_station(
    object_store: ImmutableObjectStore,
    *,
    inputs: _StationInputs,
    direct_payloads: Mapping[str, bytes],
    topology: _TopologySnapshot,
    relocation_approval: StationRelocationApproval | None,
    distance_meters: Any,
    activation_ready_station_ids: tuple[str, ...] = (),
) -> StationProjection:
    """actual direct·nested bytes를 typed station projection으로 변환한다."""
    master_snapshot = _master_snapshot(
        object_store,
        inputs.master,
        direct_payloads,
    )
    window_payload = _payload_by_artifact(direct_payloads, inputs.window_set)
    window_set = parse_station_realtime_window_set(window_payload)
    realtime_windows = _realtime_snapshots(object_store, window_set)
    previous_records = (
        None
        if inputs.previous is None
        else _station_records_from_parquet(
            _payload_by_artifact(direct_payloads, inputs.previous)
        )
    )
    projection = build_station_projection(
        master_snapshot=master_snapshot,
        realtime_windows=realtime_windows,
        previous_records=previous_records,
        weather_grid_ids=topology.weather_grid_ids,
        dispatch_centers=topology.dispatch_centers,
        activation_ready_station_ids=activation_ready_station_ids,
        relocation_approval=relocation_approval,
        distance_meters=distance_meters,
    )
    return projection


def _master_snapshot(
    object_store: ImmutableObjectStore,
    artifact: InputArtifact,
    payloads: Mapping[str, bytes],
) -> MasterSnapshot:
    """actual master manifest·Silver를 typed complete snapshot으로 변환한다."""
    snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=artifact,
        verified_payloads=payloads,
        expected_source_id=BIKE_STATION_MASTER_SOURCE_ID,
    )
    validate_source_snapshot_policy(snapshot.manifest)
    if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ContractViolation(
            "bike station master config는 EMPTY authority를 허용하지 않습니다."
        )
    rows = tuple(source_snapshot_parquet(snapshot).to_pylist())
    return MasterSnapshot(snapshot.manifest.logical_dttm, rows)


def _realtime_snapshots(
    object_store: ImmutableObjectStore,
    window_set: StationRealtimeWindowSet,
) -> tuple[RealtimeWindowSnapshot, ...]:
    """window-set의 각 manifest·Silver exact bytes를 snapshot tuple로 연다."""
    snapshots: list[RealtimeWindowSnapshot] = []
    for window in window_set.windows:
        manifest_payload = object_store.read_bytes(
            window.uri,
            window.byte_sha256,
            require_canonical_json=True,
        )
        artifact = InputArtifact(
            byte_sha256=window.byte_sha256,
            role="bike_station_realtime_manifest",
            uri=window.uri,
        )
        snapshot = read_source_snapshot_payload(
            object_store,
            manifest_artifact=artifact,
            verified_payloads={window.uri: manifest_payload},
            expected_source_id=BIKE_STATION_REALTIME_SOURCE_ID,
            expected_logical_dttm=window.logical_dttm,
        )
        validate_source_snapshot_policy(snapshot.manifest)
        if snapshot.manifest.revision_no != window.revision_no:
            raise ContractViolation(
                "window-set revision이 actual source manifest와 다릅니다."
            )
        if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
            raise ContractViolation(
                "bike station realtime config는 EMPTY window를 lifecycle 근거로 허용하지 않습니다."
            )
        rows = tuple(source_snapshot_parquet(snapshot).to_pylist())
        snapshots.append(
            RealtimeWindowSnapshot(
                logical_dttm=snapshot.manifest.logical_dttm,
                revision_no=snapshot.manifest.revision_no,
                rows=rows,
            )
        )
    return tuple(snapshots)


def _realtime_rows(
    object_store: ImmutableObjectStore,
    artifact: SourceManifestArtifact,
    *,
    role: str,
) -> tuple[Mapping[str, Any], ...]:
    """source artifact의 candidate Silver row를 actual bytes에서 반환한다."""
    input_artifact = _source_input(artifact, role)
    snapshot = read_source_snapshot_payload(
        object_store,
        manifest_artifact=input_artifact,
        verified_payloads={artifact.uri: artifact.payload},
        expected_source_id=BIKE_STATION_REALTIME_SOURCE_ID,
    )
    validate_source_snapshot_policy(snapshot.manifest)
    if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ContractViolation(
            "station realtime release candidate는 SUCCEEDED여야 합니다."
        )
    return tuple(source_snapshot_parquet(snapshot).to_pylist())


def _load_topology_locked(cursor: Cursor[tuple[Any, ...]]) -> _TopologySnapshot:
    """topology lock이 적용된 transaction에서 grid·center를 읽는다."""
    cursor.execute("SELECT weather_grid_id FROM weather_grid ORDER BY weather_grid_id")
    grid_ids = tuple(row[0] for row in cursor.fetchall())
    cursor.execute(
        """
        SELECT dispatch_center_id,
               ST_X(dispatch_center_point),
               ST_Y(dispatch_center_point),
               is_active
          FROM dispatch_center
         ORDER BY dispatch_center_id
        """
    )
    centers = tuple(
        DispatchCenterReference(
            dispatch_center_id=row[0],
            longitude=float(row[1]),
            latitude=float(row[2]),
            is_active=row[3],
        )
        for row in cursor.fetchall()
    )
    return _TopologySnapshot(grid_ids, centers)


def _postgis_distance(cursor: Cursor[tuple[Any, ...]]) -> Any:
    """PostGIS geography ST_Distance를 사용하는 callback을 반환한다."""

    def distance(
        longitude_a: float,
        latitude_a: float,
        longitude_b: float,
        latitude_b: float,
    ) -> float:
        """Point 두 개의 spheroid geography 거리를 meter로 반환한다."""
        cursor.execute(
            """
            SELECT ST_Distance(
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography,
                ST_SetSRID(ST_MakePoint(%s, %s), 4326)::geography
            )
            """,
            (longitude_a, latitude_a, longitude_b, latitude_b),
        )
        row = cursor.fetchone()
        if row is None:
            raise ContractViolation("PostGIS ST_Distance 결과가 없습니다.")
        return float(row[0])

    def batch(
        pairs: tuple[tuple[tuple[float, float], tuple[float, float]], ...],
    ) -> tuple[float, ...]:
        """Point 쌍 여러 개의 거리를 한 번의 round trip으로 반환한다.

        `distance`와 완전히 같은 geography 식을 쓰므로 값이 달라지지 않는다.
        정류소 하나당 배차센터 12개를 개별 쿼리로 재는 구조가
        prepare_serving_plan 175초 중 151초를 RDS 왕복 대기로 소모했다
        (2026-08-22 실측, 106,626 round trip). `unnest ... WITH ORDINALITY`로
        입력 순서를 보장해 호출부가 그대로 index로 대응시킬 수 있게 한다.
        """
        if not pairs:
            return ()
        cursor.execute(
            """
            SELECT ST_Distance(
                ST_SetSRID(ST_MakePoint(t.lon_a, t.lat_a), 4326)::geography,
                ST_SetSRID(ST_MakePoint(t.lon_b, t.lat_b), 4326)::geography
            )
            FROM unnest(
                %s::float8[], %s::float8[], %s::float8[], %s::float8[]
            ) WITH ORDINALITY AS t(lon_a, lat_a, lon_b, lat_b, ord)
            ORDER BY t.ord
            """,
            (
                [candidate[0] for candidate, _ in pairs],
                [candidate[1] for candidate, _ in pairs],
                [reference[0] for _, reference in pairs],
                [reference[1] for _, reference in pairs],
            ),
        )
        rows = cursor.fetchall()
        if len(rows) != len(pairs):
            raise ContractViolation(
                "PostGIS ST_Distance batch 결과 수가 입력과 다릅니다: "
                f"expected={len(pairs)} actual={len(rows)}"
            )
        return tuple(float(row[0]) for row in rows)

    distance.batch = batch  # type: ignore[attr-defined]
    return distance


def _load_prior_station(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
) -> _PriorStation | None:
    """현재 station state의 actual manifest·fingerprint·projection을 읽는다."""
    state = load_publication_state(connection, "station")
    if state is None:
        return None
    manifest = read_state_manifest(object_store, state)
    fingerprint_payload = object_store.read_bytes(
        manifest.input_fingerprint_uri,
        manifest.input_fingerprint_sha256,
        require_canonical_json=True,
    )
    fingerprint = parse_input_fingerprint(fingerprint_payload, "station")
    output = _single_output(manifest, "station")
    output_payload = object_store.read_bytes(output.uri, output.byte_sha256)
    records = _station_records_from_parquet(output_payload)
    if len(records) != state.published_row_cnt:
        raise ContractViolation("station state row count가 prior output과 다릅니다.")
    return _PriorStation(state, manifest, fingerprint, output, records)


def _load_prior_stock(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
) -> _PriorStock | None:
    """현재 stock state의 actual prepared·candidate input을 읽는다."""
    state = load_publication_state(connection, "station_stock")
    if state is None:
        return None
    prepared = _prepared_from_state(object_store, state)
    candidate = _input_by_role(
        prepared.input_fingerprint,
        "bike_station_realtime_manifest",
    )
    output = _single_output(prepared.manifest, "station_stock")
    records = _stock_records_from_parquet(
        object_store.read_bytes(output.uri, output.byte_sha256)
    )
    if len(records) != state.published_row_cnt:
        raise ContractViolation(
            "station_stock state row count가 prior output과 다릅니다."
        )
    return _PriorStock(state, prepared, candidate, records)


def _validate_prior_locked(
    cursor: Cursor[tuple[Any, ...]],
    object_store: ImmutableObjectStore,
    prior: _PriorStation | None,
) -> tuple[StationRecord, ...]:
    """lock 안 state·manifest·DB station을 검증하고 실제 이전 projection을 반환한다."""
    locked_state = publication_state_locked(cursor, "station")
    db_records = _db_station_records(cursor)
    if prior is None:
        if locked_state is not None:
            raise ContractViolation("station 준비 후 prior state가 생겼습니다.")
        if db_records:
            raise ContractViolation("station state 없이 target row가 있는 drift입니다.")
        return db_records
    if locked_state != prior.state:
        raise ContractViolation("station 준비 후 prior state가 바뀌었습니다.")
    locked_manifest = read_state_manifest(object_store, locked_state)
    if locked_manifest != prior.manifest:
        raise ContractViolation("locked station manifest가 준비한 prior와 다릅니다.")
    actual_prior = object_store.read_bytes(
        prior.output_artifact.uri,
        prior.output_artifact.byte_sha256,
    )
    if _station_records_from_parquet(actual_prior) != prior.records:
        raise ContractViolation("prior station projection actual bytes가 바뀌었습니다.")
    if db_records != prior.records:
        raise ContractViolation(
            "DB station target이 prior immutable projection과 다릅니다."
        )
    return db_records


def _validate_prior_stock_locked(
    cursor: Cursor[tuple[Any, ...]],
    object_store: ImmutableObjectStore,
    prior: _PriorStock | None,
) -> None:
    """lock 안 stock state·output·DB target이 exact prior인지 확인한다."""
    locked_state = publication_state_locked(cursor, "station_stock")
    cursor.execute(
        """
        SELECT sta_id, base_dttm, parking_bike_tot_cnt
          FROM station_stock
         ORDER BY sta_id
        """
    )
    db_records = tuple(
        StationStockRecord(row[0], row[1], row[2]) for row in cursor.fetchall()
    )
    if prior is None:
        if locked_state is not None or db_records:
            raise ContractViolation(
                "station_stock state 없이 target/state drift가 있습니다."
            )
        return
    if locked_state != prior.state:
        raise ContractViolation("station_stock 준비 후 prior state가 바뀌었습니다.")
    output = _single_output(prior.prepared.manifest, "station_stock")
    actual = _stock_records_from_parquet(
        object_store.read_bytes(output.uri, output.byte_sha256)
    )
    if actual != prior.records or db_records != prior.records:
        raise ContractViolation(
            "DB station_stock이 prior immutable projection과 다릅니다."
        )


def _db_station_records(cursor: Cursor[tuple[Any, ...]]) -> tuple[StationRecord, ...]:
    """DB station business column을 immutable output과 비교할 typed row로 읽는다."""
    cursor.execute(
        """
        SELECT sta_id,
               sta_nm,
               sta_addr,
               hold_cnt,
               ST_X(sta_point),
               ST_Y(sta_point),
               sta_point_source_cd,
               weather_grid_id,
               dispatch_center_id,
               master_base_dttm,
               last_seen_dttm,
               is_active
          FROM station
         ORDER BY sta_id
        """
    )
    return tuple(
        StationRecord(
            sta_id=row[0],
            sta_nm=row[1],
            sta_addr=row[2],
            hold_cnt=row[3],
            longitude=float(row[4]),
            latitude=float(row[5]),
            sta_point_source_cd=row[6],
            weather_grid_id=row[7],
            dispatch_center_id=row[8],
            master_base_dttm=row[9],
            last_seen_dttm=row[10],
            is_active=row[11],
        )
        for row in cursor.fetchall()
    )


def _existing_realtime_replay(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    prior: _PriorStation | None,
    dependencies: tuple[Any, ...],
    master_artifact: SourceManifestArtifact,
    realtime_candidate: SourceManifestArtifact,
    window_set: StationRealtimeWindowSet,
    relocation_approval_payload: bytes | None,
) -> tuple[PreparedPublication, PreparedPublication] | None:
    """현재 두 state가 같은 release input이면 sealed prepared 쌍을 반환한다."""
    if (
        prior is None
        or prior.state.logical_dttm != realtime_candidate.manifest.logical_dttm
    ):
        return None
    stock_state = load_publication_state(connection, "station_stock")
    if stock_state is None or stock_state.logical_dttm != prior.state.logical_dttm:
        return None
    station_prepared = _prepared_from_state(object_store, prior.state)
    stock_prepared = _prepared_from_state(object_store, stock_state)
    station_inputs = station_prepared.input_fingerprint
    if station_inputs.dependencies != dependencies:
        return None
    if not _artifact_matches_source(
        _input_by_role(station_inputs, "bike_station_master_manifest"),
        master_artifact,
    ):
        return None
    current_window_input = _input_by_role(
        station_inputs,
        "station_realtime_window_set",
    )
    current_window_payload = object_store.read_bytes(
        current_window_input.uri,
        current_window_input.byte_sha256,
        require_canonical_json=True,
    )
    if parse_station_realtime_window_set(current_window_payload) != window_set:
        return None
    stock_input = _input_by_role(
        stock_prepared.input_fingerprint,
        "bike_station_realtime_manifest",
    )
    if not _artifact_matches_source(stock_input, realtime_candidate):
        return None
    if not _same_optional_payload(
        object_store,
        station_inputs,
        "station_relocation_approval",
        relocation_approval_payload,
    ):
        return None
    return station_prepared, stock_prepared


def _existing_master_replay(
    object_store: ImmutableObjectStore,
    *,
    prior: _PriorStation,
    dependencies: tuple[Any, ...],
    master_artifact: SourceManifestArtifact,
    window_set: StationRealtimeWindowSet,
    relocation_approval_payload: bytes | None,
) -> PreparedPublication | None:
    """현재 station이 같은 master-only input이면 sealed prepared를 반환한다."""
    prepared = PreparedPublication(
        manifest=prior.manifest,
        manifest_uri=prior.state.manifest_uri,
        input_fingerprint=prior.fingerprint,
    )
    if prepared.input_fingerprint.dependencies != dependencies:
        return None
    if not _artifact_matches_source(
        _input_by_role(prepared.input_fingerprint, "bike_station_master_manifest"),
        master_artifact,
    ):
        return None
    window_input = _input_by_role(
        prepared.input_fingerprint,
        "station_realtime_window_set",
    )
    payload = object_store.read_bytes(
        window_input.uri,
        window_input.byte_sha256,
        require_canonical_json=True,
    )
    if parse_station_realtime_window_set(payload) != window_set:
        return None
    if not _same_optional_payload(
        object_store,
        prepared.input_fingerprint,
        "station_relocation_approval",
        relocation_approval_payload,
    ):
        return None
    return prepared


def _classify_current_candidate_station_change(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    prior: _PriorStation | None,
    dependencies: tuple[Any, ...],
    master_artifact: SourceManifestArtifact,
    realtime_candidate: SourceManifestArtifact,
    window_set: StationRealtimeWindowSet,
    relocation_approval_payload: bytes | None,
) -> Literal["none", "lifecycle", "other"]:
    """current candidate station-only 변경을 안전한 mode로 분류한다."""
    if (
        prior is None
        or prior.state.logical_dttm != realtime_candidate.manifest.logical_dttm
    ):
        return "none"
    stock_state = load_publication_state(connection, "station_stock")
    if stock_state is None or stock_state.logical_dttm != prior.state.logical_dttm:
        return "none"
    stock_prepared = _prepared_from_state(object_store, stock_state)
    stock_input = _input_by_role(
        stock_prepared.input_fingerprint,
        "bike_station_realtime_manifest",
    )
    if not _artifact_matches_source(stock_input, realtime_candidate):
        return "none"
    prior_window = _input_by_role(
        prior.fingerprint,
        "station_realtime_window_set",
    )
    prior_window_payload = object_store.read_bytes(
        prior_window.uri,
        prior_window.byte_sha256,
        require_canonical_json=True,
    )
    window_changed = (
        parse_station_realtime_window_set(prior_window_payload) != window_set
    )
    master_same = _artifact_matches_source(
        _input_by_role(prior.fingerprint, "bike_station_master_manifest"),
        master_artifact,
    )
    approval_same = _same_optional_payload(
        object_store,
        prior.fingerprint,
        "station_relocation_approval",
        relocation_approval_payload,
    )
    if (
        window_changed
        and master_same
        and approval_same
        and prior.fingerprint.dependencies == dependencies
    ):
        return "lifecycle"
    return "other"


def _prepared_from_state(
    object_store: ImmutableObjectStore,
    state: PublicationStateRecord,
) -> PreparedPublication:
    """state의 actual manifest·fingerprint를 replay용 prepared로 복원한다."""
    manifest = read_state_manifest(object_store, state)
    payload = object_store.read_bytes(
        manifest.input_fingerprint_uri,
        manifest.input_fingerprint_sha256,
        require_canonical_json=True,
    )
    fingerprint = parse_input_fingerprint(payload, state.publication_key)
    return PreparedPublication(manifest, state.manifest_uri, fingerprint)


def _same_optional_payload(
    object_store: ImmutableObjectStore,
    fingerprint: InputFingerprint,
    role: str,
    payload: bytes | None,
) -> bool:
    """optional input이 caller payload과 모두 없거나 exact인지 반환한다."""
    artifacts = tuple(item for item in fingerprint.input_artifacts if item.role == role)
    if payload is None:
        return not artifacts
    if len(artifacts) != 1:
        return False
    actual = object_store.read_bytes(
        artifacts[0].uri,
        artifacts[0].byte_sha256,
        require_canonical_json=True,
    )
    return actual == payload


def _validate_latest_sources(
    source_catalog: S3SourceSnapshotCatalog,
    *,
    expected_master: SourceManifestArtifact,
    expected_windows: tuple[SourceManifestArtifact, ...],
    mode: Literal["realtime", "master", "lifecycle"],
    master_lookback: timedelta | None,
    realtime_lookback: timedelta,
) -> None:
    """DB lock 후 source authority correction identity를 bounded 재조회한다."""
    if mode == "realtime":
        if master_lookback is None:
            raise ContractViolation(
                "realtime locked 검증에 master_lookback이 필요합니다."
            )
        candidate = expected_windows[0]
        master = source_catalog.latest_at_or_before(
            BIKE_STATION_MASTER_SOURCE_ID,
            candidate.manifest.logical_dttm,
            lookback=master_lookback,
        )
        windows = source_catalog.recent_windows(
            BIKE_STATION_REALTIME_SOURCE_ID,
            candidate.manifest.logical_dttm,
            limit=3,
            lookback=realtime_lookback,
        )
        if not _same_source_artifact(master, expected_master):
            raise ContractViolation("locked station master authority가 바뀌었습니다.")
        if not _same_source_artifact_tuple(windows, expected_windows):
            raise ContractViolation(
                "locked station realtime window/correction이 바뀌었습니다."
            )
        return
    master = source_catalog.exact_window(
        BIKE_STATION_MASTER_SOURCE_ID,
        expected_master.manifest.logical_dttm,
    )
    if not _same_source_artifact(master, expected_master):
        raise ContractViolation(
            "locked station master authority correction이 바뀌었습니다."
        )
    window_set = _build_window_set(expected_windows, expected_windows[0])
    actual = _catalog_artifacts_for_window_set(
        source_catalog,
        window_set,
        lookback=realtime_lookback,
    )
    if not _same_source_artifact_tuple(actual, expected_windows):
        raise ContractViolation("locked station realtime correction이 바뀌었습니다.")


def _prepare(
    connection: Connection[Any],
    *,
    object_base_uri: str,
    publication_key: Literal["station", "station_stock"],
    logical_dttm: datetime,
    publisher_version: str,
    row_count: int,
    materials: Any,
) -> PreparedPublication:
    """publication content에 맞는 revision을 배정해 prepared를 만든다."""
    revision = allocate_revision(
        connection,
        PublicationCandidate(
            publication_key=publication_key,
            logical_dttm=logical_dttm,
            artifact_set_sha256=materials.artifact_set.sha256,
            input_fingerprint_sha256=materials.input_fingerprint.sha256,
            published_row_cnt=row_count,
        ),
    )
    return build_prepared_publication(
        base_uri=object_base_uri,
        publication_key=publication_key,
        logical_dttm=logical_dttm,
        publisher_version=publisher_version,
        revision_no=revision,
        target_row_counts={publication_key: row_count},
        materials=materials,
    )


def _station_parameters() -> tuple[Parameter, ...]:
    """station SSOT의 exact algorithm parameter를 반환한다."""
    return (
        Parameter("center_assignment_version", CENTER_ASSIGNMENT_VERSION),
        Parameter("grid_conversion_version", GRID_CONVERSION_VERSION),
        Parameter("station_policy_version", STATION_POLICY_VERSION),
    )


def _build_window_set(
    artifacts: tuple[SourceManifestArtifact, ...],
    candidate: SourceManifestArtifact,
) -> StationRealtimeWindowSet:
    """catalog artifact tuple을 canonical realtime window-set으로 변환한다."""
    if not artifacts:
        raise ContractViolation(
            "station realtime window artifact가 하나 이상 필요합니다."
        )
    windows = tuple(_window_from_artifact(artifact) for artifact in artifacts)
    expected = _window_from_artifact(candidate)
    return build_station_realtime_window_set(windows, expected_candidate=expected)


def _window_from_artifact(artifact: SourceManifestArtifact) -> StationRealtimeWindow:
    """source artifact identity를 station window identity로 변환한다."""
    _require_source_artifact(artifact, BIKE_STATION_REALTIME_SOURCE_ID)
    return StationRealtimeWindow(
        byte_sha256=artifact.byte_sha256,
        logical_dttm=artifact.manifest.logical_dttm,
        revision_no=artifact.manifest.revision_no,
        uri=artifact.uri,
    )


def _catalog_artifacts_for_window_set(
    source_catalog: S3SourceSnapshotCatalog,
    window_set: StationRealtimeWindowSet,
    *,
    lookback: timedelta,
) -> tuple[SourceManifestArtifact, ...]:
    """window-set의 최대 3개 identity를 bounded exact catalog read로 재확인한다."""
    _require_positive_lookback(lookback, "realtime_lookback")
    newest = window_set.windows[0].logical_dttm
    if newest - window_set.windows[-1].logical_dttm > lookback:
        raise ContractViolation(
            "station realtime window-set이 caller lookback 범위를 벗어났습니다."
        )
    artifacts: list[SourceManifestArtifact] = []
    for window in window_set.windows:
        artifact = source_catalog.exact_window(
            BIKE_STATION_REALTIME_SOURCE_ID,
            window.logical_dttm,
        )
        if _window_from_artifact(artifact) != window:
            raise ContractViolation(
                "station prior window의 최신 correction이 바뀌었습니다."
            )
        artifacts.append(artifact)
    return tuple(artifacts)


def _source_input(artifact: SourceManifestArtifact, role: str) -> InputArtifact:
    """actual source manifest identity를 fingerprint artifact로 만든다."""
    return InputArtifact(
        byte_sha256=artifact.byte_sha256,
        role=role,
        uri=artifact.uri,
    )


def _source_artifact_from_input(
    object_store: ImmutableObjectStore,
    artifact: InputArtifact,
    source_id: str,
) -> SourceManifestArtifact:
    """fingerprint input의 actual canonical manifest를 source artifact로 복원한다."""
    payload = object_store.read_bytes(
        artifact.uri,
        artifact.byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_source_snapshot_manifest(payload)
    result = SourceManifestArtifact(
        manifest=manifest,
        uri=artifact.uri,
        byte_sha256=artifact.byte_sha256,
        payload=payload,
    )
    _require_source_artifact(result, source_id)
    return result


def _prior_projection_input(prior: _PriorStation | None) -> InputArtifact | None:
    """prior station output을 조건부 fingerprint input으로 반환한다."""
    if prior is None:
        return None
    return InputArtifact(
        byte_sha256=prior.output_artifact.byte_sha256,
        role="station_previous_projection",
        uri=prior.output_artifact.uri,
    )


def _direct_payloads(
    object_store: ImmutableObjectStore,
    *,
    master_artifact: SourceManifestArtifact,
    window_set: StationRealtimeWindowSet,
    prior: _PriorStation | None,
    relocation_payload: bytes | None,
) -> dict[str, bytes]:
    """준비 단계의 actual direct input payload mapping을 만든다."""
    payloads = {
        master_artifact.uri: master_artifact.payload,
    }
    if prior is not None:
        payloads[prior.output_artifact.uri] = object_store.read_bytes(
            prior.output_artifact.uri,
            prior.output_artifact.byte_sha256,
        )
    # URI는 content-addressed helper가 만들므로 payload만으로는 알 수 없다.
    payloads["__station_window_set_payload__"] = window_set.canonical_bytes
    if relocation_payload is not None:
        payloads["__station_relocation_payload__"] = relocation_payload
    return payloads


def _station_inputs_from_fingerprint(
    fingerprint: InputFingerprint,
) -> _StationInputs:
    """validated station fingerprint를 role별 typed input으로 풀어낸다."""
    return _StationInputs(
        master=_input_by_role(fingerprint, "bike_station_master_manifest"),
        window_set=_input_by_role(fingerprint, "station_realtime_window_set"),
        previous=_optional_input_by_role(
            fingerprint,
            "station_previous_projection",
        ),
        relocation=_optional_input_by_role(
            fingerprint,
            "station_relocation_approval",
        ),
    )


def _input_by_role(fingerprint: InputFingerprint, role: str) -> InputArtifact:
    """fingerprint에서 exact role 하나를 반환한다."""
    matches = tuple(item for item in fingerprint.input_artifacts if item.role == role)
    if len(matches) != 1:
        raise ContractViolation(f"{role} input artifact가 정확히 하나가 아닙니다.")
    return matches[0]


def _optional_input_by_role(
    fingerprint: InputFingerprint,
    role: str,
) -> InputArtifact | None:
    """fingerprint에서 optional role 0..1개를 반환한다."""
    matches = tuple(item for item in fingerprint.input_artifacts if item.role == role)
    if len(matches) > 1:
        raise ContractViolation(f"{role} optional input artifact가 중복됩니다.")
    return None if not matches else matches[0]


def _payload_by_artifact(
    payloads: Mapping[str, bytes],
    artifact: InputArtifact,
) -> bytes:
    """verified mapping에서 artifact URI의 exact payload를 반환한다."""
    try:
        payload = payloads[artifact.uri]
    except KeyError as exc:
        marker = {
            "station_realtime_window_set": "__station_window_set_payload__",
            "station_relocation_approval": "__station_relocation_payload__",
        }.get(artifact.role)
        if marker is None or marker not in payloads:
            raise ContractViolation(
                f"{artifact.role} actual payload가 없습니다."
            ) from exc
        payload = payloads[marker]
    if type(payload) is not bytes:
        raise ContractViolation(f"{artifact.role} payload는 bytes여야 합니다.")
    from core.gold_publication import sha256_hex

    if sha256_hex(payload) != artifact.byte_sha256:
        raise ContractViolation(f"{artifact.role} payload checksum이 다릅니다.")
    return payload


def _read_direct_inputs(
    object_store: ImmutableObjectStore,
    inputs: _StationInputs,
) -> dict[str, bytes]:
    """station fingerprint의 direct immutable input을 exact-read한다."""
    result: dict[str, bytes] = {}
    for artifact in inputs.all:
        result[artifact.uri] = object_store.read_bytes(
            artifact.uri,
            artifact.byte_sha256,
            require_canonical_json=artifact.role
            in {
                "bike_station_master_manifest",
                "station_realtime_window_set",
                "station_relocation_approval",
            },
        )
    return result


def _approval_from_inputs(
    inputs: _StationInputs,
    payloads: Mapping[str, bytes],
) -> StationRelocationApproval | None:
    """relocation input이 있으면 actual canonical bytes를 typed 문서로 읽는다."""
    if inputs.relocation is None:
        return None
    return parse_station_relocation_approval(
        _payload_by_artifact(payloads, inputs.relocation)
    )


def _parse_optional_approval(
    payload: bytes | None,
) -> StationRelocationApproval | None:
    """caller의 optional relocation canonical bytes를 typed 문서로 읽는다."""
    if payload is None:
        return None
    return parse_station_relocation_approval(payload)


def _materialize_relocation_input(
    object_store: ImmutableObjectStore,
    *,
    object_base_uri: str,
    payload: bytes | None,
    projection: StationProjection,
) -> InputArtifact | None:
    """실제 relocation을 반영했을 때만 approval input을 immutable write한다."""
    if projection.relocation_applied:
        if payload is None:
            raise ContractViolation(
                "relocation을 반영했지만 approval bytes가 없습니다."
            )
        return store_input_payload(
            object_store,
            base_uri=object_base_uri,
            publication_key="station",
            role="station_relocation_approval",
            payload=payload,
            suffix="json",
            require_canonical_json=True,
        )
    if payload is not None:
        raise ContractViolation(
            "station에 반영하지 않은 relocation approval이 있습니다."
        )
    return None


def _validate_prepared_station_output(
    publication: PreparedPublication,
    payloads: Mapping[str, bytes],
    projection: StationProjection,
) -> None:
    """prepared station output actual bytes와 재구성 projection을 대조한다."""
    output = _single_output(publication.manifest, "station")
    records = _station_records_from_parquet(payloads[output.uri])
    if records != projection.records:
        raise ContractViolation(
            "station output이 actual source·prior·topology projection과 다릅니다."
        )


def _validate_sealed_station_output(
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    projection: StationProjection,
) -> None:
    """sealed station output object를 lock 안 projection과 대조한다."""
    output = _single_output(evidence.manifest, "station")
    payload = object_store.read_bytes(output.uri, output.byte_sha256)
    if _station_records_from_parquet(payload) != projection.records:
        raise ContractViolation("locked station projection이 sealed output과 다릅니다.")


def _validate_sealed_stock_output(
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    projection: StationStockProjection,
) -> None:
    """sealed station_stock output object를 lock 안 projection과 대조한다."""
    output = _single_output(evidence.manifest, "station_stock")
    payload = object_store.read_bytes(output.uri, output.byte_sha256)
    if _stock_records_from_parquet(payload) != projection.records:
        raise ContractViolation(
            "locked station_stock projection이 sealed output과 다릅니다."
        )


def _single_output(manifest: PublicationManifest, role: str) -> Artifact:
    """manifest에서 exact output role 하나를 반환한다."""
    matches = tuple(
        artifact for artifact in manifest.artifacts if artifact.role == role
    )
    if len(matches) != 1 or len(manifest.artifacts) != 1:
        raise ContractViolation(f"{role} output artifact가 정확히 하나가 아닙니다.")
    return matches[0]


def _station_records_to_parquet(records: tuple[StationRecord, ...]) -> bytes:
    """station records를 fixed-schema deterministic Parquet bytes로 만든다."""
    _validate_station_ids(tuple(record.sta_id for record in records))
    table = pa.Table.from_pylist(
        [
            {
                "sta_id": record.sta_id,
                "sta_nm": record.sta_nm,
                "sta_addr": record.sta_addr,
                "hold_cnt": record.hold_cnt,
                "sta_point_ewkb": bytes.fromhex(record.point_ewkb),
                "sta_point_source_cd": record.sta_point_source_cd,
                "weather_grid_id": record.weather_grid_id,
                "dispatch_center_id": record.dispatch_center_id,
                "master_base_dttm": record.master_base_dttm,
                "last_seen_dttm": record.last_seen_dttm,
                "is_active": record.is_active,
            }
            for record in records
        ],
        schema=_STATION_SCHEMA,
    )
    return parquet_bytes(table)


def _station_records_from_parquet(payload: bytes) -> tuple[StationRecord, ...]:
    """fixed-schema station Parquet을 typed records로 재검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _STATION_SCHEMA:
        raise ContractViolation(
            "station output Parquet schema가 exact 계약과 다릅니다."
        )
    records: list[StationRecord] = []
    for row in table.to_pylist():
        longitude, latitude = _point_from_ewkb(row.pop("sta_point_ewkb"))
        records.append(
            StationRecord(
                longitude=longitude,
                latitude=latitude,
                **row,
            )
        )
    result = tuple(records)
    _validate_station_ids(tuple(record.sta_id for record in result))
    if result != tuple(sorted(result, key=lambda item: item.sta_id.encode("utf-8"))):
        raise ContractViolation(
            "station output Parquet row 순서가 sta_id UTF-8 순이 아닙니다."
        )
    return result


def _stock_records_to_parquet(records: tuple[StationStockRecord, ...]) -> bytes:
    """station_stock records를 fixed-schema deterministic Parquet bytes로 만든다."""
    _validate_station_ids(tuple(record.sta_id for record in records))
    table = pa.Table.from_pylist(
        [
            {
                "sta_id": record.sta_id,
                "base_dttm": record.base_dttm,
                "parking_bike_tot_cnt": record.parking_bike_tot_cnt,
            }
            for record in records
        ],
        schema=_STATION_STOCK_SCHEMA,
    )
    return parquet_bytes(table)


def _stock_records_from_parquet(payload: bytes) -> tuple[StationStockRecord, ...]:
    """fixed-schema station_stock Parquet을 typed records로 재검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _STATION_STOCK_SCHEMA:
        raise ContractViolation(
            "station_stock output Parquet schema가 exact 계약과 다릅니다."
        )
    records = tuple(StationStockRecord(**row) for row in table.to_pylist())
    _validate_station_ids(tuple(record.sta_id for record in records))
    if records != tuple(sorted(records, key=lambda item: item.sta_id.encode("utf-8"))):
        raise ContractViolation(
            "station_stock output row 순서가 sta_id UTF-8 순이 아닙니다."
        )
    return records


def _point_from_ewkb(value: Any) -> tuple[float, float]:
    """big-endian SRID 4326 Point EWKB bytes를 longitude·latitude로 파싱한다."""
    if type(value) is not bytes or len(value) != 25:
        raise ContractViolation("station Point EWKB가 25-byte XDR Point가 아닙니다.")
    byte_order, geometry_type, srid, longitude, latitude = struct.unpack(
        ">BIIdd", value
    )
    if byte_order != 0 or geometry_type != 0x20000001 or srid != 4326:
        raise ContractViolation("station Point EWKB type·SRID·byte order가 다릅니다.")
    return longitude, latitude


def _route_invalidating_station_ids(
    previous: tuple[StationRecord, ...],
    incoming: tuple[StationRecord, ...],
) -> tuple[str, ...]:
    """비활성화·센터·Point 변화로 proposed route를 무효화하는 ID를 반환한다."""
    previous_by_id = {record.sta_id: record for record in previous}
    incoming_by_id = {record.sta_id: record for record in incoming}
    affected: list[str] = []
    for station_id, prior_record in previous_by_id.items():
        incoming_record = incoming_by_id.get(station_id)
        if incoming_record is None:
            affected.append(station_id)
            continue
        if (
            (prior_record.is_active and not incoming_record.is_active)
            or prior_record.dispatch_center_id != incoming_record.dispatch_center_id
            or (prior_record.longitude, prior_record.latitude)
            != (incoming_record.longitude, incoming_record.latitude)
        ):
            affected.append(station_id)
    return tuple(sorted(affected, key=lambda value: value.encode("utf-8")))


def _delete_affected_proposed_routes(
    cursor: Cursor[tuple[Any, ...]],
    station_ids: tuple[str, ...],
) -> None:
    """영향 station stop을 가진 proposed header만 삭제해 stop을 cascade 정리한다."""
    _validate_station_ids(station_ids)
    if not station_ids:
        return
    cursor.execute(
        """
        DELETE FROM rebalance_route AS route
         WHERE route.route_status_cd = 'proposed'
           AND EXISTS (
               SELECT 1
                 FROM rebalance_route_stop AS stop
                WHERE stop.route_id = route.route_id
                  AND stop.sta_id = ANY(%s::TEXT[])
           )
        """,
        (list(station_ids),),
    )


def _upsert_station(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[StationRecord, ...],
) -> None:
    """station 전체 projection을 FK 이력 행 삭제 없이 upsert한다."""
    _validate_station_ids(tuple(record.sta_id for record in records))
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
        )
        VALUES (
            %s, %s, %s, %s,
            ST_SetSRID(ST_MakePoint(%s, %s), 4326),
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (sta_id) DO UPDATE
        SET sta_nm = EXCLUDED.sta_nm,
            sta_addr = EXCLUDED.sta_addr,
            hold_cnt = EXCLUDED.hold_cnt,
            sta_point = EXCLUDED.sta_point,
            sta_point_source_cd = EXCLUDED.sta_point_source_cd,
            weather_grid_id = EXCLUDED.weather_grid_id,
            dispatch_center_id = EXCLUDED.dispatch_center_id,
            master_base_dttm = EXCLUDED.master_base_dttm,
            last_seen_dttm = EXCLUDED.last_seen_dttm,
            is_active = EXCLUDED.is_active
        """,
        tuple(
            (
                record.sta_id,
                record.sta_nm,
                record.sta_addr,
                record.hold_cnt,
                record.longitude,
                record.latitude,
                record.sta_point_source_cd,
                record.weather_grid_id,
                record.dispatch_center_id,
                record.master_base_dttm,
                record.last_seen_dttm,
                record.is_active,
            )
            for record in records
        ),
    )


def _replace_station_stock(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[StationStockRecord, ...],
) -> None:
    """station_stock을 created_dttm 보존 upsert 후 absent-key 삭제로 reconcile한다."""
    _validate_station_ids(tuple(record.sta_id for record in records))
    cursor.executemany(
        """
        INSERT INTO station_stock (
            sta_id,
            base_dttm,
            parking_bike_tot_cnt
        ) VALUES (%s, %s, %s)
        ON CONFLICT (sta_id) DO UPDATE
        SET base_dttm = EXCLUDED.base_dttm,
            parking_bike_tot_cnt = EXCLUDED.parking_bike_tot_cnt
        """,
        tuple(
            (record.sta_id, record.base_dttm, record.parking_bike_tot_cnt)
            for record in records
        ),
    )
    cursor.execute(
        "DELETE FROM station_stock WHERE NOT (sta_id = ANY(%s::TEXT[]))",
        ([record.sta_id for record in records],),
    )


def _require_master_only_last_seen(
    previous: tuple[StationRecord, ...],
    incoming: tuple[StationRecord, ...],
) -> None:
    """master-only correction이 station identity·last-seen을 전진시키지 않음을 검증한다."""
    previous_values = {record.sta_id: record.last_seen_dttm for record in previous}
    incoming_values = {record.sta_id: record.last_seen_dttm for record in incoming}
    if incoming_values != previous_values:
        raise ContractViolation(
            "master-only correction이 station identity 또는 last_seen_dttm을 바꾸었습니다."
        )


def _require_nonempty_release(
    station: StationProjection,
    stock: StationStockProjection,
) -> None:
    """SSOT가 EMPTY를 금지한 station·stock release를 검증한다."""
    if not station.records:
        raise ContractViolation("station EMPTY publication은 SSOT에서 금지됩니다.")
    if not stock.records:
        raise ContractViolation(
            "station_stock EMPTY publication은 SSOT에서 금지됩니다."
        )


def _validate_station_ids(values: tuple[str, ...]) -> None:
    """target DDL의 exact ``ST-[0-9]+`` station ID를 검증한다."""
    if any(
        type(value) is not str or _STATION_ID.fullmatch(value) is None
        for value in values
    ):
        raise ContractViolation(
            "station ID가 target DDL의 ^ST-[0-9]+$ 계약과 다릅니다."
        )
    if len(values) != len(set(values)):
        raise ContractViolation("station ID가 중복됩니다.")


def _evidence_by_key(
    evidence: tuple[VerifiedPublicationEvidence, ...],
    expected_keys: tuple[str, ...],
) -> dict[str, VerifiedPublicationEvidence]:
    """evidence tuple의 publication key 집합을 exact로 검증한다."""
    result = {item.manifest.publication_key: item for item in evidence}
    if len(result) != len(evidence) or set(result) != set(expected_keys):
        raise ContractViolation(
            f"station release evidence key가 다릅니다: expected={expected_keys}"
        )
    return result


def _window_set_from_fingerprint(
    object_store: ImmutableObjectStore,
    fingerprint: InputFingerprint,
) -> StationRealtimeWindowSet:
    """station fingerprint의 actual canonical window-set을 읽는다."""
    artifact = _input_by_role(fingerprint, "station_realtime_window_set")
    payload = object_store.read_bytes(
        artifact.uri,
        artifact.byte_sha256,
        require_canonical_json=True,
    )
    return parse_station_realtime_window_set(payload)


def _same_source_artifact(
    left: SourceManifestArtifact,
    right: SourceManifestArtifact,
) -> bool:
    """source manifest URI·SHA identity가 같은지 반환한다."""
    return (left.uri, left.byte_sha256) == (right.uri, right.byte_sha256)


def _same_source_artifact_tuple(
    left: tuple[SourceManifestArtifact, ...],
    right: tuple[SourceManifestArtifact, ...],
) -> bool:
    """source artifact tuple의 순서와 URI·SHA identity가 같은지 반환한다."""
    return len(left) == len(right) and all(
        _same_source_artifact(left_item, right_item)
        for left_item, right_item in zip(left, right, strict=True)
    )


def _artifact_matches_source(
    input_artifact: InputArtifact,
    source_artifact: SourceManifestArtifact,
) -> bool:
    """fingerprint artifact가 source manifest URI·SHA와 같은지 반환한다."""
    return (input_artifact.uri, input_artifact.byte_sha256) == (
        source_artifact.uri,
        source_artifact.byte_sha256,
    )


def _require_source_artifact(
    artifact: SourceManifestArtifact,
    source_id: str,
) -> None:
    """source artifact의 exact type·source·checked-in policy를 검증한다."""
    if type(artifact) is not SourceManifestArtifact:
        raise ContractViolation("station source artifact type이 잘못됐습니다.")
    if artifact.manifest.source_id != source_id:
        raise ContractViolation(
            f"station source artifact가 {source_id} authority가 아닙니다."
        )
    validate_source_snapshot_policy(artifact.manifest)


def _require_master_monotonic(
    object_store: ImmutableObjectStore,
    prior: _PriorStation,
    incoming: SourceManifestArtifact,
) -> None:
    """master-only/realtime release가 prior의 master authority를 되돌리지 않게 한다."""
    prior_input = _input_by_role(
        prior.fingerprint,
        "bike_station_master_manifest",
    )
    prior_payload = object_store.read_bytes(
        prior_input.uri,
        prior_input.byte_sha256,
        require_canonical_json=True,
    )
    prior_manifest: SourceSnapshotManifest = parse_source_snapshot_manifest(
        prior_payload
    )
    validate_source_snapshot_policy(prior_manifest)
    previous_version = (
        prior_manifest.logical_dttm,
        prior_manifest.revision_no,
    )
    incoming_version = (
        incoming.manifest.logical_dttm,
        incoming.manifest.revision_no,
    )
    if incoming_version < previous_version:
        raise ContractViolation(
            "station master authority를 prior projection보다 과거로 되돌릴 수 없습니다."
        )
    if incoming_version == previous_version and not _artifact_matches_source(
        prior_input,
        incoming,
    ):
        raise ContractViolation(
            "station master 같은 source version의 identity가 prior와 다릅니다."
        )


def _require_catalog(value: S3SourceSnapshotCatalog) -> None:
    """source catalog이 concrete bounded authority reader인지 검증한다."""
    if type(value) is not S3SourceSnapshotCatalog:
        raise ContractViolation("station source_catalog type이 잘못됐습니다.")


def _require_positive_lookback(value: timedelta, name: str) -> None:
    """caller가 명시한 bounded catalog lookback을 검증한다."""
    if type(value) is not timedelta or value <= timedelta(0):
        raise ContractViolation(f"{name}은 양의 timedelta여야 합니다.")
