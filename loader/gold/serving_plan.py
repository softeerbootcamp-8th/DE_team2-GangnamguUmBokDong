"""Station serving projection을 추론 전 준비하고 네 Gold key를 원자 게시한다."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, cast
from urllib.parse import urlsplit

from core.gold_publication import (
    ContractViolation,
    Dependency,
    ImmutableObjectStore,
    InputArtifact,
    Parameter,
    PreparedPublication,
    PublicationOutcome,
    VerifiedPublicationEvidence,
    build_id_set,
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_id_set,
    parse_input_fingerprint,
    parse_publication_manifest,
    parse_station_realtime_window_set,
    parse_utc_dttm,
    sha256_hex,
    validate_id_set_parameter,
    validate_sha256_hex,
    validate_station_stock_release,
)
from core.inference_catalog import InferenceRevisionCatalog
from core.inference_snapshot import (
    InferenceSnapshotManifest,
    InferenceSnapshotStatus,
    ServingPlanRef,
    ServingReleaseRef,
)
from core.model_snapshot import (
    IdSetArtifactRef,
    build_id_set_artifact_ref,
    parse_model_snapshot_manifest,
)
from core.source_snapshot import parse_source_snapshot_manifest
from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus

from . import demand as demand_publisher
from . import station_release, weather_forecast
from .common import (
    OutputObject,
    PublicationExecution,
    build_prepared_publication,
    materialize_publication,
    publish_verified,
)
from .demand import DEMAND_PUBLISHER_VERSION, DemandProjection
from .source_catalog import S3SourceSnapshotCatalog, SourceManifestArtifact
from .state import (
    PublicationStateRecord,
    load_dependencies,
    load_publication_state,
    publication_state_locked,
)
from .station import StationProjection, StationRecord
from .station_stock import StationStockProjection, StationStockRecord
from .versioning import PublicationCandidate, allocate_revision
from .weather_forecast import WeatherForecastProjection

LEGACY_SERVING_PLAN_SCHEMA_VERSION = "gold-serving-plan-v2"
"""Shared identity가 없지만 finalize 호환을 유지하는 legacy plan version이다."""

SERVING_PLAN_SCHEMA_VERSION = "gold-serving-plan-v3"
"""Inference 전 orchestration plan의 exact schema version이다."""

MAX_INFERENCE_INELIGIBLE_RATIO = 0.01
"""활성·모델지원 후보 중 입력 품질 문제로 제외할 수 있는 최대 비율이다."""

_PLAN_V2_KEYS = frozenset(
    {
        "activation_ready_sta_ids",
        "expected_sta_ids",
        "inference_eligible_sta_ids",
        "logical_dttm",
        "object_base_uri",
        "prepared_publications",
        "prior_states",
        "rental_support_sta_ids",
        "return_support_sta_ids",
        "schema_version",
        "source_lookbacks",
        "station_dependency",
    }
)
_PLAN_V3_KEYS = frozenset(
    (*_PLAN_V2_KEYS, "serving_release", "station_master_enriched")
)
_PREPARED_REF_KEYS = frozenset({"byte_sha256", "publication_key", "uri"})
_ID_SET_REF_KEYS = frozenset({"byte_sha256", "id_count", "schema_version", "uri"})
_INPUT_ARTIFACT_KEYS = frozenset({"byte_sha256", "role", "uri"})
_SERVING_RELEASE_REF_KEYS = frozenset(
    {"byte_sha256", "effective_contract_version", "release_version", "uri"}
)
_DEPENDENCY_KEYS = frozenset(
    {
        "artifact_set_sha256",
        "input_fingerprint_sha256",
        "logical_dttm",
        "manifest_uri",
        "publication_key",
        "revision_no",
    }
)
_STATE_KEYS = frozenset((*_DEPENDENCY_KEYS, "published_row_cnt"))
_LOOKBACK_KEYS = frozenset(
    {
        "master_seconds",
        "realtime_seconds",
        "short_term_seconds",
        "ultra_short_seconds",
    }
)
_PLAN_PUBLICATION_KEYS = ("station", "station_stock", "weather_forecast")
_FINAL_PUBLICATION_KEYS = (
    "station",
    "station_demand_forecast",
    "station_stock",
    "weather_forecast",
)
_STATION_MASTER_ENRICHED_ROLE = "station_master_enriched"


@dataclass(frozen=True, slots=True)
class PreparedManifestRef:
    """Plan이 pin한 prepared Gold manifest의 exact identity다."""

    publication_key: str
    uri: str
    byte_sha256: str

    def __post_init__(self) -> None:
        """Publication key·URI·SHA를 plan 허용 집합에 고정한다."""
        if self.publication_key not in _PLAN_PUBLICATION_KEYS:
            raise ContractViolation(
                f"serving plan prepared key가 잘못됐습니다: {self.publication_key}"
            )
        _nonblank(self.uri, "prepared manifest URI")
        validate_sha256_hex(self.byte_sha256)


@dataclass(frozen=True, slots=True)
class SourceLookbacks:
    """Plan과 final lock 재검증이 공유하는 source catalog lookback이다."""

    master: timedelta
    realtime: timedelta
    short_term: timedelta
    ultra_short: timedelta

    def __post_init__(self) -> None:
        """각 lookback을 양의 whole-second timedelta로 제한한다."""
        for name, value in (
            ("master", self.master),
            ("realtime", self.realtime),
            ("short_term", self.short_term),
            ("ultra_short", self.ultra_short),
        ):
            if (
                type(value) is not timedelta
                or value <= timedelta(0)
                or value.microseconds != 0
            ):
                raise ContractViolation(
                    f"{name} lookback은 양의 whole-second timedelta여야 합니다."
                )


@dataclass(frozen=True, slots=True)
class ServingPlan:
    """Inference와 final commit 사이에 고정하는 내부 serving release plan이다."""

    logical_dttm: datetime
    object_base_uri: str
    station_dependency: Dependency
    activation_ready_sta_ids: IdSetArtifactRef
    expected_sta_ids: IdSetArtifactRef
    inference_eligible_sta_ids: IdSetArtifactRef
    rental_support_sta_ids: IdSetArtifactRef
    return_support_sta_ids: IdSetArtifactRef
    prepared_publications: tuple[PreparedManifestRef, ...]
    prior_states: tuple[PublicationStateRecord, ...]
    source_lookbacks: SourceLookbacks
    serving_release: ServingReleaseRef | None = None
    station_master_enriched: InputArtifact | None = None
    schema_version: str = SERVING_PLAN_SCHEMA_VERSION

    def __post_init__(self) -> None:
        """Plan field와 canonical key 순서를 검증한다."""
        if self.schema_version not in (
            LEGACY_SERVING_PLAN_SCHEMA_VERSION,
            SERVING_PLAN_SCHEMA_VERSION,
        ):
            raise ContractViolation("serving plan schema_version이 다릅니다.")
        if self.schema_version == LEGACY_SERVING_PLAN_SCHEMA_VERSION:
            if (
                self.serving_release is not None
                or self.station_master_enriched is not None
            ):
                raise ContractViolation("v2 serving plan에는 shared identity가 없어야 합니다.")
        else:
            if type(self.serving_release) is not ServingReleaseRef:
                raise ContractViolation(
                    "v3 serving plan에는 exact serving release가 필요합니다."
                )
            if type(self.station_master_enriched) is not InputArtifact:
                raise ContractViolation(
                    "v3 serving plan에는 exact station_master_enriched가 필요합니다."
                )
        logical = _utc(self.logical_dttm, "serving plan logical_dttm")
        object.__setattr__(self, "logical_dttm", logical)
        object_base_uri = _require_s3_base_uri(self.object_base_uri)
        if self.serving_release is not None:
            _require_same_s3_bucket(
                self.serving_release.uri,
                object_base_uri,
                "serving release",
            )
        if self.station_master_enriched is not None:
            _require_enriched_master_ref(
                self.station_master_enriched,
                object_base_uri,
            )
        if (
            type(self.station_dependency) is not Dependency
            or self.station_dependency.publication_key != "station"
            or self.station_dependency.logical_dttm != logical
        ):
            raise ContractViolation(
                "serving plan station dependency가 같은 anchor의 station이 아닙니다."
            )
        for name, value in (
            ("activation_ready_sta_ids", self.activation_ready_sta_ids),
            ("expected_sta_ids", self.expected_sta_ids),
            ("inference_eligible_sta_ids", self.inference_eligible_sta_ids),
            ("rental_support_sta_ids", self.rental_support_sta_ids),
            ("return_support_sta_ids", self.return_support_sta_ids),
        ):
            if type(value) is not IdSetArtifactRef:
                raise ContractViolation(f"{name}은 exact ID set ref여야 합니다.")
        if type(self.prepared_publications) is not tuple or any(
            type(item) is not PreparedManifestRef for item in self.prepared_publications
        ):
            raise ContractViolation("prepared_publications 타입이 잘못됐습니다.")
        prepared_keys = tuple(
            item.publication_key for item in self.prepared_publications
        )
        if prepared_keys != _PLAN_PUBLICATION_KEYS:
            raise ContractViolation(
                "serving plan prepared publication key 순서가 다릅니다."
            )
        if type(self.prior_states) is not tuple or any(
            type(item) is not PublicationStateRecord for item in self.prior_states
        ):
            raise ContractViolation("prior_states 타입이 잘못됐습니다.")
        prior_keys = tuple(item.publication_key for item in self.prior_states)
        expected_prior_order = tuple(
            key for key in _PLAN_PUBLICATION_KEYS if key in set(prior_keys)
        )
        if prior_keys != expected_prior_order or len(prior_keys) != len(
            set(prior_keys)
        ):
            raise ContractViolation(
                "serving plan prior state 순서·중복이 잘못됐습니다."
            )
        if type(self.source_lookbacks) is not SourceLookbacks:
            raise ContractViolation("source_lookbacks 타입이 잘못됐습니다.")

    @property
    def canonical_bytes(self) -> bytes:
        """Plan을 exact-key canonical JSON bytes로 반환한다."""
        return canonical_json_bytes(_plan_document(self))

    @property
    def sha256(self) -> str:
        """Canonical plan bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class ServingPlanArtifact:
    """저장된 plan URI·SHA와 inference 입력 편의값을 반환한다."""

    plan: ServingPlan
    uri: str
    byte_sha256: str

    def __post_init__(self) -> None:
        """Plan bytes와 반환 identity가 정확히 결합됐는지 검증한다."""
        if type(self.plan) is not ServingPlan:
            raise ContractViolation("plan은 ServingPlan이어야 합니다.")
        if self.byte_sha256 != self.plan.sha256:
            raise ContractViolation("serving plan 반환 SHA가 bytes와 다릅니다.")
        ServingPlanRef(byte_sha256=self.byte_sha256, uri=self.uri)

    @property
    def station_dependency(self) -> Dependency:
        """Inference manifest에 넣을 incoming station dependency를 반환한다."""
        return self.plan.station_dependency

    @property
    def expected_sta_ids(self) -> IdSetArtifactRef:
        """Inference가 exact-read할 기대 station ID set reference를 반환한다."""
        return self.plan.expected_sta_ids

    @property
    def serving_plan_ref(self) -> ServingPlanRef:
        """Inference manifest에 기록할 content-addressed plan reference를 반환한다."""
        return ServingPlanRef(byte_sha256=self.byte_sha256, uri=self.uri)


@dataclass(slots=True)
class _LockedRelease:
    """Final lock에서 재구성한 네 target projection을 mutation까지 보관한다."""

    station: StationProjection | None = None
    stock: StationStockProjection | None = None
    demand: DemandProjection | None = None
    weather: WeatherForecastProjection | None = None
    route_invalidating_station_ids: tuple[str, ...] = ()


def prepare_serving_plan(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    master_artifact: SourceManifestArtifact,
    realtime_candidate: SourceManifestArtifact,
    short_term_artifact: SourceManifestArtifact,
    ultra_short_artifact: SourceManifestArtifact,
    rental_support_sta_ids: IdSetArtifactRef,
    return_support_sta_ids: IdSetArtifactRef,
    serving_release: ServingReleaseRef,
    station_master_enriched: InputArtifact,
    source_catalog: S3SourceSnapshotCatalog,
    object_base_uri: str,
    source_lookbacks: SourceLookbacks,
    relocation_approval_payload: bytes | None = None,
    station_publisher_version: str = station_release.STATION_PUBLISHER_VERSION,
    stock_publisher_version: str = station_release.STATION_STOCK_PUBLISHER_VERSION,
    weather_publisher_version: str = weather_forecast.WEATHER_FORECAST_PUBLISHER_VERSION,
    inference_eligible_sta_ids: tuple[str, ...] | None = None,
) -> ServingPlanArtifact:
    """추론 전에 station·stock·weather와 activation 기대 집합을 immutable 준비한다.

    준비 중에는 Gold target/state를 변경하지 않는다. Prepared manifest와 plan은
    content-addressed object일 뿐이며 final transaction이 publication_state를 claim하기
    전에는 serving authority가 아니다.
    """
    _require_idle(connection)
    _require_catalog(source_catalog)
    _require_s3_base_uri(object_base_uri)
    anchor = _utc(realtime_candidate.manifest.logical_dttm, "realtime anchor")
    station_release._require_source_artifact(
        master_artifact,
        station_release.BIKE_STATION_MASTER_SOURCE_ID,
    )
    station_release._require_source_artifact(
        realtime_candidate,
        station_release.BIKE_STATION_REALTIME_SOURCE_ID,
    )
    weather_forecast._require_source_artifact(
        short_term_artifact,
        weather_forecast.SHORT_TERM_SOURCE_ID,
        anchor,
    )
    weather_forecast._require_source_artifact(
        ultra_short_artifact,
        weather_forecast.ULTRA_SHORT_SOURCE_ID,
        anchor,
    )
    if master_artifact.manifest.logical_dttm > anchor:
        raise ContractViolation("station master가 realtime anchor보다 미래입니다.")
    _validate_latest_plan_sources(
        source_catalog,
        master_artifact=master_artifact,
        realtime_candidate=realtime_candidate,
        short_term_artifact=short_term_artifact,
        ultra_short_artifact=ultra_short_artifact,
        anchor=anchor,
        lookbacks=source_lookbacks,
    )
    rental_ids = _read_station_id_set_ref(
        object_store,
        rental_support_sta_ids,
        "rental support",
    )
    return_ids = _read_station_id_set_ref(
        object_store,
        return_support_sta_ids,
        "return support",
    )
    if inference_eligible_sta_ids is None:
        inference_eligible_sta_ids = tuple(
            sorted(
                set(rental_ids) | set(return_ids),
                key=lambda value: value.encode("utf-8"),
            )
        )
    if type(inference_eligible_sta_ids) is not tuple:
        raise ContractViolation("inference eligible station ID는 tuple이어야 합니다.")
    eligible_ids = build_id_set(inference_eligible_sta_ids).ids
    if eligible_ids != inference_eligible_sta_ids:
        raise ContractViolation(
            "inference eligible station ID는 중복 없는 UTF-8 순이어야 합니다."
        )

    prior_station = station_release._load_prior_station(connection, object_store)
    prior_stock = station_release._load_prior_stock(connection, object_store)
    if prior_station is not None:
        station_release._require_master_monotonic(
            object_store,
            prior_station,
            master_artifact,
        )
    station_release._validate_db_station_short(
        connection,
        () if prior_station is None else prior_station.records,
    )
    station_release._validate_db_stock_short(
        connection,
        () if prior_stock is None else prior_stock.records,
    )
    initial_states = _load_plan_prior_states(connection)
    if _state_for(initial_states, "station") != (
        None if prior_station is None else prior_station.state
    ):
        raise ContractViolation(
            "station prior state가 immutable projection과 다릅니다."
        )
    if _state_for(initial_states, "station_stock") != (
        None if prior_stock is None else prior_stock.state
    ):
        raise ContractViolation("stock prior state가 immutable projection과 다릅니다.")

    dependencies = load_dependencies(connection, ("dispatch_center", "weather_grid"))
    weather_grid_dependency = next(
        item for item in dependencies if item.publication_key == "weather_grid"
    )
    realtime_artifacts = source_catalog.recent_windows(
        station_release.BIKE_STATION_REALTIME_SOURCE_ID,
        anchor,
        limit=3,
        lookback=source_lookbacks.realtime,
    )
    if not station_release._same_source_artifact(
        realtime_artifacts[0],
        realtime_candidate,
    ):
        raise ContractViolation("realtime candidate가 최신 correction이 아닙니다.")
    window_set = station_release._build_window_set(
        realtime_artifacts,
        realtime_candidate,
    )
    window_input = station_release.store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        role="station_realtime_window_set",
        payload=window_set.canonical_bytes,
        suffix="json",
        require_canonical_json=True,
    )
    master_input = station_release._source_input(
        master_artifact,
        "bike_station_master_manifest",
    )
    previous_input = station_release._prior_projection_input(prior_station)
    approval = station_release._parse_optional_approval(relocation_approval_payload)
    provisional_inputs = station_release._StationInputs(
        master=master_input,
        window_set=window_input,
        previous=previous_input,
        relocation=None,
    )
    direct_payloads = station_release._direct_payloads(
        object_store,
        master_artifact=master_artifact,
        window_set=window_set,
        prior=prior_station,
        relocation_payload=None,
    )
    topology, provisional = station_release._project_station_with_connection(
        connection,
        object_store,
        inputs=provisional_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
    )
    relocation_input = station_release._materialize_relocation_input(
        object_store,
        object_base_uri=object_base_uri,
        payload=relocation_approval_payload,
        projection=provisional,
    )
    station_inputs = station_release._StationInputs(
        master=master_input,
        window_set=window_input,
        previous=previous_input,
        relocation=relocation_input,
    )
    direct_payloads = station_release._direct_payloads(
        object_store,
        master_artifact=master_artifact,
        window_set=window_set,
        prior=prior_station,
        relocation_payload=relocation_approval_payload,
    )
    _, provisional = station_release._project_station_with_connection(
        connection,
        object_store,
        inputs=station_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
        expected_topology=topology,
    )
    candidate_rows = station_release._realtime_rows(
        object_store,
        realtime_candidate,
        role="bike_station_realtime_manifest",
    )
    stock_projection = station_release.build_station_stock_projection(
        candidate_rows,
        published_station_ids=tuple(record.sta_id for record in provisional.records),
        candidate_logical_dttm=anchor,
    )
    current_stock_ids = tuple(record.sta_id for record in stock_projection.records)

    short_input = InputArtifact(
        byte_sha256=short_term_artifact.byte_sha256,
        role="short_term_manifest",
        uri=short_term_artifact.uri,
    )
    ultra_input = InputArtifact(
        byte_sha256=ultra_short_artifact.byte_sha256,
        role="ultra_short_manifest",
        uri=ultra_short_artifact.uri,
    )
    always_active_grids = _active_grid_ids(provisional.records)
    _weather_projection_from_artifacts(
        object_store,
        short_term_artifact=short_term_artifact,
        ultra_short_artifact=ultra_short_artifact,
        short_input=short_input,
        ultra_input=ultra_input,
        active_grids=always_active_grids,
        anchor=anchor,
    )
    activation_ready = _activation_ready_station_ids(
        object_store,
        provisional=provisional,
        short_term_artifact=short_term_artifact,
        ultra_short_artifact=ultra_short_artifact,
        short_input=short_input,
        ultra_input=ultra_input,
        anchor=anchor,
        current_stock_ids=current_stock_ids,
    )
    _, station_projection = station_release._project_station_with_connection(
        connection,
        object_store,
        inputs=station_inputs,
        direct_payloads=direct_payloads,
        relocation_approval=approval,
        expected_topology=topology,
        activation_ready_station_ids=activation_ready,
    )
    active_ids = _active_station_ids(station_projection.records)
    expected_ids = _inference_expected_station_ids(
        active_ids,
        rental_ids,
        return_ids,
        eligible_ids,
    )
    final_active_grids = _active_grid_ids(station_projection.records)
    weather_projection = _weather_projection_from_artifacts(
        object_store,
        short_term_artifact=short_term_artifact,
        ultra_short_artifact=ultra_short_artifact,
        short_input=short_input,
        ultra_input=ultra_input,
        active_grids=final_active_grids,
        anchor=anchor,
    )
    if tuple(record.sta_id for record in station_projection.records) != tuple(
        record.sta_id for record in provisional.records
    ):
        raise ContractViolation("activation gate가 station identity 집합을 바꿨습니다.")
    station_release._require_nonempty_release(station_projection, stock_projection)

    station_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station",
        dependencies=dependencies,
        input_artifacts=station_inputs.all,
        parameters=station_release._station_parameters(),
        outputs=(
            OutputObject(
                role="station",
                payload=station_release._station_records_to_parquet(
                    station_projection.records
                ),
                row_count=len(station_projection.records),
            ),
        ),
    )
    stock_input = station_release._source_input(
        realtime_candidate,
        "bike_station_realtime_manifest",
    )
    stock_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_stock",
        input_artifacts=(stock_input,),
        parameters=(
            Parameter(
                "station_stock_policy_version",
                station_release.STATION_STOCK_POLICY_VERSION,
            ),
        ),
        outputs=(
            OutputObject(
                role="station_stock",
                payload=station_release._stock_records_to_parquet(
                    stock_projection.records
                ),
                row_count=len(stock_projection.records),
            ),
        ),
    )
    station_prepared = station_release._prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station",
        logical_dttm=anchor,
        publisher_version=station_publisher_version,
        row_count=len(station_projection.records),
        materials=station_materials,
    )
    stock_prepared = station_release._prepare(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station_stock",
        logical_dttm=anchor,
        publisher_version=stock_publisher_version,
        row_count=len(stock_projection.records),
        materials=stock_materials,
    )
    replayed_station_pair = station_release._existing_realtime_replay(
        connection,
        object_store,
        prior=prior_station,
        dependencies=dependencies,
        master_artifact=master_artifact,
        realtime_candidate=realtime_candidate,
        window_set=window_set,
        relocation_approval_payload=relocation_approval_payload,
    )
    if (
        replayed_station_pair is not None
        and prior_station is not None
        and prior_stock is not None
        and station_projection.records == prior_station.records
        and stock_projection.records == prior_stock.records
    ):
        station_prepared, stock_prepared = replayed_station_pair
    validate_station_stock_release(
        station_prepared.input_fingerprint,
        stock_prepared.input_fingerprint,
        window_set,
    )
    station_dependency = _dependency_from_prepared(station_prepared)
    weather_outputs = _weather_outputs(weather_projection)
    weather_materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="weather_forecast",
        dependencies=(station_dependency, weather_grid_dependency),
        input_artifacts=(short_input, ultra_input),
        parameters=(
            Parameter(
                "forecast_hour_count",
                str(weather_forecast.FORECAST_HOUR_COUNT),
            ),
            Parameter("resolver_version", weather_forecast.RESOLVER_VERSION),
        ),
        outputs=weather_outputs,
    )
    weather_prepared = _prepare_publication(
        connection,
        object_base_uri=object_base_uri,
        publication_key="weather_forecast",
        logical_dttm=anchor,
        publisher_version=weather_publisher_version,
        row_count=len(weather_projection.records),
        materials=weather_materials,
        conditional_empty=not weather_projection.records,
    )
    prepared = (station_prepared, stock_prepared, weather_prepared)
    if _load_plan_prior_states(connection) != initial_states:
        raise ContractViolation(
            "serving plan 준비 중 station·stock·weather publication state가 바뀌었습니다."
        )
    for item in prepared:
        _store_prepared_manifest(object_store, item)

    activation_ref = _store_id_set(
        object_store,
        object_base_uri=object_base_uri,
        name="activation-ready-sta-ids",
        values=activation_ready,
    )
    expected_ref = _store_id_set(
        object_store,
        object_base_uri=object_base_uri,
        name="expected-sta-ids",
        values=expected_ids,
    )
    eligible_ref = _store_id_set(
        object_store,
        object_base_uri=object_base_uri,
        name="inference-eligible-sta-ids",
        values=eligible_ids,
    )
    plan = ServingPlan(
        logical_dttm=anchor,
        object_base_uri=object_base_uri,
        station_dependency=station_dependency,
        activation_ready_sta_ids=activation_ref,
        expected_sta_ids=expected_ref,
        inference_eligible_sta_ids=eligible_ref,
        rental_support_sta_ids=rental_support_sta_ids,
        return_support_sta_ids=return_support_sta_ids,
        prepared_publications=tuple(
            PreparedManifestRef(
                publication_key=item.manifest.publication_key,
                uri=item.manifest_uri,
                byte_sha256=item.manifest.sha256,
            )
            for item in prepared
        ),
        prior_states=initial_states,
        source_lookbacks=source_lookbacks,
        serving_release=serving_release,
        station_master_enriched=station_master_enriched,
    )
    plan_uri = (
        f"{object_base_uri.rstrip('/')}/serving-plan/plans/sha256={plan.sha256}.json"
    )
    object_store.put_once(
        plan_uri,
        plan.canonical_bytes,
        expected_sha256=plan.sha256,
        require_canonical_json=True,
    )
    readback = object_store.read_bytes(
        plan_uri,
        plan.sha256,
        require_canonical_json=True,
    )
    if readback != plan.canonical_bytes:
        raise ContractViolation(
            "serving plan readback bytes가 준비한 bytes와 다릅니다."
        )
    return ServingPlanArtifact(plan, plan_uri, plan.sha256)


def publish_serving_plan(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    plan_uri: str,
    plan_sha256: str,
    inference_manifest_uri: str,
    inference_manifest_sha256: str,
    inference_catalog: InferenceRevisionCatalog,
    source_catalog: S3SourceSnapshotCatalog,
    demand_publisher_version: str = DEMAND_PUBLISHER_VERSION,
) -> PublicationExecution:
    """Plan과 inference actual bytes를 읽어 네 Gold projection을 한 번에 게시한다."""
    _require_idle(connection)
    _require_catalog(source_catalog)
    plan = read_serving_plan(
        object_store,
        plan_uri=plan_uri,
        plan_sha256=plan_sha256,
    )
    prepared_by_key = {
        reference.publication_key: _read_prepared_manifest(object_store, reference)
        for reference in plan.prepared_publications
    }
    station_prepared = prepared_by_key["station"]
    stock_prepared = prepared_by_key["station_stock"]
    weather_prepared = prepared_by_key["weather_forecast"]
    if _dependency_from_prepared(station_prepared) != plan.station_dependency:
        raise ContractViolation(
            "plan station dependency가 prepared manifest와 다릅니다."
        )
    if any(
        item.manifest.logical_dttm != plan.logical_dttm
        for item in prepared_by_key.values()
    ):
        raise ContractViolation("plan prepared publication anchor가 섞였습니다.")

    activation_ready = _read_station_id_set_ref(
        object_store,
        plan.activation_ready_sta_ids,
        "activation ready",
    )
    expected_ids = _read_station_id_set_ref(
        object_store,
        plan.expected_sta_ids,
        "inference expected",
    )
    inference_eligible = _read_station_id_set_ref(
        object_store,
        plan.inference_eligible_sta_ids,
        "inference eligible",
    )
    rental_support = _read_station_id_set_ref(
        object_store,
        plan.rental_support_sta_ids,
        "rental support",
    )
    return_support = _read_station_id_set_ref(
        object_store,
        plan.return_support_sta_ids,
        "return support",
    )
    snapshot = demand_publisher._read_inference_snapshot(
        object_store,
        inference_manifest_uri=inference_manifest_uri,
        inference_manifest_sha256=inference_manifest_sha256,
    )
    plan_ref = ServingPlanRef(byte_sha256=plan_sha256, uri=plan_uri)
    if snapshot.manifest.serving_plan != plan_ref:
        raise ContractViolation(
            "inference manifest serving_plan ref가 current plan URI·SHA와 다릅니다."
        )
    if (
        snapshot.manifest.logical_dttm != plan.logical_dttm
        or snapshot.manifest.station_dependency != plan.station_dependency
        or snapshot.manifest.expected_sta_ids != plan.expected_sta_ids
        or snapshot.expected_sta_ids != expected_ids
    ):
        raise ContractViolation(
            "inference manifest가 plan anchor·station dependency·expected ID와 다릅니다."
        )
    _validate_shared_identity_binding(plan, snapshot.manifest)
    _validate_inference_catalog_latest(
        inference_catalog,
        logical_dttm=plan.logical_dttm,
        revision_no=snapshot.manifest.revision_no,
        manifest_uri=inference_manifest_uri,
        manifest_sha256=inference_manifest_sha256,
    )
    _validate_inference_support_binding(
        object_store,
        snapshot,
        rental_support_ref=plan.rental_support_sta_ids,
        return_support_ref=plan.return_support_sta_ids,
    )
    if (
        snapshot.rental_support_sta_ids != rental_support
        or snapshot.return_support_sta_ids != return_support
    ):
        raise ContractViolation(
            "inference actual model support가 serving plan과 다릅니다."
        )
    station_output = station_release._single_output(
        station_prepared.manifest,
        "station",
    )
    planned_station_records = station_release._station_records_from_parquet(
        object_store.read_bytes(
            station_output.uri,
            station_output.byte_sha256,
        )
    )
    planned_active_ids = _active_station_ids(planned_station_records)
    if expected_ids != _inference_expected_station_ids(
        planned_active_ids,
        rental_support,
        return_support,
        inference_eligible,
    ):
        raise ContractViolation(
            "plan expected ID가 incoming active∩rental∩return∩eligible과 다릅니다."
        )
    demand_projection = demand_publisher._projection_from_snapshot(
        snapshot,
        active_sta_ids=planned_active_ids,
        expected_sta_ids=expected_ids,
    )
    if demand_projection.expected_sta_ids != expected_ids:
        raise ContractViolation("inference projection 기대 집합이 plan과 다릅니다.")
    demand_prepared = _prepare_demand_publication(
        connection,
        object_store,
        snapshot=snapshot,
        projection=demand_projection,
        object_base_uri=plan.object_base_uri,
        publisher_version=demand_publisher_version,
    )

    prior_station = station_release._load_prior_station(connection, object_store)
    prior_stock = station_release._load_prior_stock(connection, object_store)
    prior_demand = load_publication_state(connection, "station_demand_forecast")

    station_validator = station_release._station_staging_validator(
        connection,
        object_store,
        expected_current_records=() if prior_station is None else prior_station.records,
        activation_ready_station_ids=activation_ready,
        validate_current_target=False,
    )
    stock_validator = station_release._stock_staging_validator(
        connection,
        object_store,
        expected_current_records=() if prior_stock is None else prior_stock.records,
        validate_current_target=False,
    )
    weather_validator = _weather_staging_validator(
        object_store,
        weather_prepared,
        active_grids=_active_grid_ids(planned_station_records),
        anchor=plan.logical_dttm,
    )
    demand_validator = _demand_staging_validator(
        object_store,
        snapshot=snapshot,
        projection=demand_projection,
        active_sta_ids=planned_active_ids,
    )
    holder = _LockedRelease()

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """모든 lock 안에서 prior·source·topology·네 sealed output을 재검증한다."""
        evidence_by_key = _evidence_by_key(evidence, _FINAL_PUBLICATION_KEYS)
        _validate_inference_catalog_latest(
            inference_catalog,
            logical_dttm=plan.logical_dttm,
            revision_no=snapshot.manifest.revision_no,
            manifest_uri=inference_manifest_uri,
            manifest_sha256=inference_manifest_sha256,
        )
        _validate_locked_prior_states(
            cursor,
            plan,
            prior_demand,
            evidence_by_key,
        )
        station_is_replay = publication_state_locked(cursor, "station") == (
            _state_from_evidence(evidence_by_key["station"])
        )
        previous_records = (
            station_release._db_station_records(cursor)
            if station_is_replay
            else station_release._validate_prior_locked(
                cursor,
                object_store,
                prior_station,
            )
        )
        stock_is_replay = publication_state_locked(cursor, "station_stock") == (
            _state_from_evidence(evidence_by_key["station_stock"])
        )
        if not stock_is_replay:
            station_release._validate_prior_stock_locked(
                cursor,
                object_store,
                prior_stock,
            )
        master_artifact, realtime_artifacts = _station_sources_from_prepared(
            object_store,
            station_prepared,
        )
        if prior_station is not None:
            station_release._require_master_monotonic(
                object_store,
                prior_station,
                master_artifact,
            )
        station_release._validate_latest_sources(
            source_catalog,
            expected_master=master_artifact,
            expected_windows=realtime_artifacts,
            mode="realtime",
            master_lookback=plan.source_lookbacks.master,
            realtime_lookback=plan.source_lookbacks.realtime,
        )
        station_projection = station_release._locked_station_projection(
            cursor,
            object_store,
            evidence_by_key["station"],
            activation_ready_station_ids=activation_ready,
        )
        stock_projection = station_release._stock_projection_from_evidence(
            object_store,
            evidence_by_key["station_stock"],
            published_station_ids=tuple(
                record.sta_id for record in station_projection.records
            ),
        )
        if not set(activation_ready).issubset(
            {record.sta_id for record in stock_projection.records}
        ):
            raise ContractViolation(
                "activation-ready station의 locked current stock이 완전하지 않습니다."
            )
        window_set = station_release._window_set_from_fingerprint(
            object_store,
            evidence_by_key["station"].input_fingerprint,
        )
        validate_station_stock_release(
            evidence_by_key["station"].input_fingerprint,
            evidence_by_key["station_stock"].input_fingerprint,
            window_set,
        )
        station_release._validate_sealed_station_output(
            object_store,
            evidence_by_key["station"],
            station_projection,
        )
        station_release._validate_sealed_stock_output(
            object_store,
            evidence_by_key["station_stock"],
            stock_projection,
        )
        locked_active_ids = _active_station_ids(station_projection.records)
        locked_expected_ids = _inference_expected_station_ids(
            locked_active_ids,
            rental_support,
            return_support,
            inference_eligible,
        )
        if locked_expected_ids != expected_ids:
            raise ContractViolation(
                "locked incoming station topology의 expected inference ID가 바뀌었습니다."
            )
        demand_projection_locked = demand_publisher._projection_from_snapshot(
            snapshot,
            active_sta_ids=locked_active_ids,
            expected_sta_ids=locked_expected_ids,
        )
        validate_id_set_parameter(
            "station_demand_forecast",
            evidence_by_key["station_demand_forecast"].input_fingerprint,
            build_id_set(locked_expected_ids),
        )
        _validate_sealed_demand_output(
            object_store,
            evidence_by_key["station_demand_forecast"],
            demand_projection_locked,
        )
        short_artifact, ultra_artifact = _weather_sources_from_prepared(
            object_store,
            weather_prepared,
        )
        weather_projection_locked = _weather_projection_from_artifacts(
            object_store,
            short_term_artifact=short_artifact,
            ultra_short_artifact=ultra_artifact,
            short_input=_input_by_role(
                evidence_by_key["weather_forecast"].input_fingerprint,
                "short_term_manifest",
            ),
            ultra_input=_input_by_role(
                evidence_by_key["weather_forecast"].input_fingerprint,
                "ultra_short_manifest",
            ),
            active_grids=_active_grid_ids(station_projection.records),
            anchor=plan.logical_dttm,
        )
        _validate_sealed_weather_output(
            object_store,
            evidence_by_key["weather_forecast"],
            weather_projection_locked,
        )
        holder.station = station_projection
        holder.stock = stock_projection
        holder.demand = demand_projection_locked
        holder.weather = weather_projection_locked
        holder.route_invalidating_station_ids = (
            station_release._route_invalidating_station_ids(
                previous_records,
                station_projection.records,
            )
        )

    def validate_conditional_empty(
        _cursor: Cursor[tuple[Any, ...]],
        evidence: VerifiedPublicationEvidence,
    ) -> bool:
        """Lock에서 재구성한 incoming topology로 demand·weather EMPTY를 증명한다."""
        if evidence.manifest.publication_key == "station_demand_forecast":
            return holder.demand is not None and not holder.demand.records
        if evidence.manifest.publication_key == "weather_forecast":
            return holder.weather is not None and not holder.weather.records
        raise ContractViolation(
            "serving plan conditional EMPTY callback에 예상 밖 key가 전달됐습니다."
        )

    def validate_replay_targets_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """혼합 실행의 replay target을 같은 DB lock 아래 sealed projection과 대조한다."""
        if any(
            item is None
            for item in (holder.station, holder.stock, holder.demand, holder.weather)
        ):
            raise ContractViolation("locked serving projection이 완전하지 않습니다.")
        assert holder.station is not None
        assert holder.stock is not None
        assert holder.demand is not None
        assert holder.weather is not None
        _validate_target_records_locked(
            cursor,
            publication_keys=tuple(item.manifest.publication_key for item in evidence),
            station_records=holder.station.records,
            stock_records=holder.stock.records,
            demand_records=holder.demand.records,
            weather_records=holder.weather.records,
        )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Station FK parent 뒤 stock·demand·weather를 한 transaction에서 reconcile한다."""
        published_keys = {item.manifest.publication_key for item in evidence}
        if not published_keys or not published_keys.issubset(_FINAL_PUBLICATION_KEYS):
            raise ContractViolation(
                "serving release published key subset이 잘못됐습니다."
            )
        if any(
            item is None
            for item in (holder.station, holder.stock, holder.demand, holder.weather)
        ):
            raise ContractViolation("locked serving projection이 완전하지 않습니다.")
        assert holder.station is not None
        assert holder.stock is not None
        assert holder.demand is not None
        assert holder.weather is not None
        if "station" in published_keys:
            station_release._delete_affected_proposed_routes(
                cursor,
                holder.route_invalidating_station_ids,
            )
            station_release._upsert_station(cursor, holder.station.records)
        if "station_stock" in published_keys:
            station_release._replace_station_stock(cursor, holder.stock.records)
        if "station_demand_forecast" in published_keys:
            demand_publisher._reconcile_demand_records(cursor, holder.demand.records)
        if "weather_forecast" in published_keys:
            weather_forecast._upsert_weather_forecast_records(
                cursor,
                holder.weather.records,
            )
            weather_forecast._delete_absent_weather_forecast_records(
                cursor,
                holder.weather.records,
            )

    execution = publish_verified(
        connection,
        (
            (station_prepared, station_validator),
            (stock_prepared, stock_validator),
            (demand_prepared, demand_validator),
            (weather_prepared, weather_validator),
        ),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
        validate_conditional_empty=validate_conditional_empty,
        allow_mixed_replay=True,
        validate_replay_targets_locked=validate_replay_targets_locked,
    )
    if (
        execution.result.outcome is PublicationOutcome.PUBLISHED
        and execution.result.publication_keys != _FINAL_PUBLICATION_KEYS
    ):
        raise ContractViolation("serving release published key 집합이 다릅니다.")
    return execution


def _validate_shared_identity_binding(
    plan: ServingPlan,
    manifest: InferenceSnapshotManifest,
) -> None:
    """v3 plan의 release와 enriched-master identity를 inference에 결합한다.

    Legacy v2 plan은 이미 생성된 inference manifest의 finalize/replay 호환을 위해
    기존 검증만 유지한다. 신규 inference reader는 v2를 fail-closed하므로 이 예외가
    새로운 unpinned 계산을 허용하지는 않는다.
    """
    if plan.schema_version == LEGACY_SERVING_PLAN_SCHEMA_VERSION:
        return
    if manifest.serving_release != plan.serving_release:
        raise ContractViolation(
            "inference manifest serving release가 v3 serving plan ref와 다릅니다."
        )
    if manifest.status is InferenceSnapshotStatus.EMPTY:
        return
    assert plan.station_master_enriched is not None
    matches = tuple(
        item for item in manifest.inputs if item.role == _STATION_MASTER_ENRICHED_ROLE
    )
    if len(matches) != 1:
        raise ContractViolation(
            "SUCCEEDED inference manifest에 station_master_enriched input이 "
            "정확히 하나가 아닙니다."
        )
    if matches[0].byte_sha256 != plan.station_master_enriched.byte_sha256:
        raise ContractViolation(
            "inference station_master_enriched bytes가 v3 serving plan과 다릅니다."
        )


def read_serving_plan(
    object_store: ImmutableObjectStore,
    *,
    plan_uri: str,
    plan_sha256: str,
) -> ServingPlan:
    """URI·SHA로 canonical plan actual bytes를 exact-read하고 typed plan으로 파싱한다."""
    validate_sha256_hex(plan_sha256)
    payload = object_store.read_bytes(
        plan_uri,
        plan_sha256,
        require_canonical_json=True,
    )
    if sha256_hex(payload) != plan_sha256:
        raise ContractViolation("serving plan actual bytes SHA가 argument와 다릅니다.")
    plan = parse_serving_plan(payload)
    if plan.sha256 != plan_sha256:
        raise ContractViolation("serving plan canonical SHA가 argument와 다릅니다.")
    return plan


def parse_serving_plan(payload: bytes) -> ServingPlan:
    """Exact-key canonical JSON bytes를 ServingPlan으로 파싱한다."""
    parsed = parse_canonical_json(payload)
    if type(parsed) is not dict:
        raise ContractViolation("serving plan은 JSON object여야 합니다.")
    version = _string(parsed.get("schema_version"), "schema_version")
    if version == LEGACY_SERVING_PLAN_SCHEMA_VERSION:
        document = _exact_object(parsed, _PLAN_V2_KEYS, "serving plan")
    elif version == SERVING_PLAN_SCHEMA_VERSION:
        document = _exact_object(parsed, _PLAN_V3_KEYS, "serving plan")
    else:
        raise ContractViolation("serving plan schema_version이 다릅니다.")
    prepared_values = _array(
        document["prepared_publications"],
        "prepared_publications",
    )
    prior_values = _array(document["prior_states"], "prior_states")
    return ServingPlan(
        schema_version=version,
        logical_dttm=parse_utc_dttm(_string(document["logical_dttm"], "logical_dttm")),
        object_base_uri=_string(document["object_base_uri"], "object_base_uri"),
        station_dependency=_parse_dependency(document["station_dependency"]),
        activation_ready_sta_ids=_parse_id_set_ref(
            document["activation_ready_sta_ids"],
            "activation_ready_sta_ids",
        ),
        expected_sta_ids=_parse_id_set_ref(
            document["expected_sta_ids"],
            "expected_sta_ids",
        ),
        inference_eligible_sta_ids=_parse_id_set_ref(
            document["inference_eligible_sta_ids"],
            "inference_eligible_sta_ids",
        ),
        rental_support_sta_ids=_parse_id_set_ref(
            document["rental_support_sta_ids"],
            "rental_support_sta_ids",
        ),
        return_support_sta_ids=_parse_id_set_ref(
            document["return_support_sta_ids"],
            "return_support_sta_ids",
        ),
        prepared_publications=tuple(
            _parse_prepared_ref(value) for value in prepared_values
        ),
        prior_states=tuple(_parse_state(value) for value in prior_values),
        source_lookbacks=_parse_lookbacks(document["source_lookbacks"]),
        serving_release=(
            None
            if version == LEGACY_SERVING_PLAN_SCHEMA_VERSION
            else _parse_serving_release_ref(document["serving_release"])
        ),
        station_master_enriched=(
            None
            if version == LEGACY_SERVING_PLAN_SCHEMA_VERSION
            else _parse_input_artifact(
                document["station_master_enriched"],
                _STATION_MASTER_ENRICHED_ROLE,
            )
        ),
    )


def _prepare_demand_publication(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    snapshot: demand_publisher.DemandInferenceSnapshot,
    projection: DemandProjection,
    object_base_uri: str,
    publisher_version: str,
) -> PreparedPublication:
    """Verified inference snapshot으로 demand prepared identity를 한 번 계산한다."""
    outputs = (
        ()
        if not projection.records
        else (
            OutputObject(
                role="station_demand_forecast",
                payload=demand_publisher.demand_records_to_parquet(
                    projection.records,
                    expected_sta_ids=projection.expected_sta_ids,
                ),
                row_count=len(projection.records),
            ),
        )
    )
    expected = build_id_set(projection.expected_sta_ids)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_demand_forecast",
        dependencies=(snapshot.manifest.station_dependency,),
        input_artifacts=(
            snapshot.inference_input,
            snapshot.rental_model_input,
            snapshot.return_model_input,
        ),
        parameters=(
            Parameter("expected_sta_id_sha256", expected.sha256),
            Parameter("horizon_count", str(demand_publisher.HORIZON_COUNT)),
            Parameter("rounding_mode", demand_publisher.ROUNDING_MODE),
        ),
        outputs=outputs,
    )
    return _prepare_publication(
        connection,
        object_base_uri=object_base_uri,
        publication_key="station_demand_forecast",
        logical_dttm=snapshot.manifest.logical_dttm,
        publisher_version=publisher_version,
        row_count=len(projection.records),
        materials=materials,
        conditional_empty=not projection.records,
    )


def _prepare_publication(
    connection: Connection[Any],
    *,
    object_base_uri: str,
    publication_key: str,
    logical_dttm: datetime,
    publisher_version: str,
    row_count: int,
    materials: Any,
    conditional_empty: bool,
) -> PreparedPublication:
    """고정 materials에 현재 state 기준 correction revision을 한 번 배정한다."""
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
        conditional_empty_candidate=conditional_empty,
    )


def _weather_staging_validator(
    object_store: ImmutableObjectStore,
    prepared: PreparedPublication,
    *,
    active_grids: tuple[str, ...],
    anchor: datetime,
) -> Any:
    """Incoming station grid를 사용하는 weather staging validator를 만든다."""
    short_input = _input_by_role(prepared.input_fingerprint, "short_term_manifest")
    ultra_input = _input_by_role(prepared.input_fingerprint, "ultra_short_manifest")

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """Actual two-source bytes와 incoming grid로 weather output을 재구성한다."""
        if publication.manifest.publication_key != "weather_forecast":
            raise ContractViolation("weather prepared publication key가 다릅니다.")
        projection = weather_forecast._projection_from_verified_payloads(
            object_store,
            short_input=short_input,
            ultra_input=ultra_input,
            payloads=payloads,
            active_grids=active_grids,
            anchor=anchor,
        )
        _validate_weather_artifact(publication, payloads, projection)
        return {"base_dttm": tuple(record.base_dttm for record in projection.records)}

    return validate_staging


def _demand_staging_validator(
    object_store: ImmutableObjectStore,
    *,
    snapshot: demand_publisher.DemandInferenceSnapshot,
    projection: DemandProjection,
    active_sta_ids: tuple[str, ...],
) -> Any:
    """Incoming station ID를 사용하는 demand staging validator를 만든다."""

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """Actual inference/model bytes로 demand output을 다시 검증한다."""
        verified = demand_publisher._read_inference_snapshot_from_verified_inputs(
            object_store,
            inference_input=snapshot.inference_input,
            rental_model_input=snapshot.rental_model_input,
            return_model_input=snapshot.return_model_input,
            payloads=payloads,
        )
        expected = demand_publisher._projection_from_snapshot(
            verified,
            active_sta_ids=active_sta_ids,
            expected_sta_ids=projection.expected_sta_ids,
        )
        if expected != projection:
            raise ContractViolation("demand staging projection이 plan과 다릅니다.")
        demand_publisher._validate_demand_artifact(
            publication,
            payloads,
            expected,
        )
        return {"base_dttm": tuple(record.base_dttm for record in expected.records)}

    return validate_staging


def _validate_weather_artifact(
    publication: PreparedPublication,
    payloads: Mapping[str, bytes],
    projection: WeatherForecastProjection,
) -> None:
    """Prepared weather output 또는 EMPTY를 exact projection과 대조한다."""
    artifacts = publication.manifest.artifacts
    if not projection.records:
        if artifacts:
            raise ContractViolation("weather EMPTY에 output artifact가 있습니다.")
        return
    if len(artifacts) != 1 or artifacts[0].role != "weather_forecast":
        raise ContractViolation("weather output artifact가 정확히 하나가 아닙니다.")
    if (
        weather_forecast._records_from_parquet(payloads[artifacts[0].uri])
        != projection.records
    ):
        raise ContractViolation("weather output Parquet이 plan projection과 다릅니다.")


def _validate_sealed_demand_output(
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    projection: DemandProjection,
) -> None:
    """Locked demand projection과 sealed output actual bytes를 대조한다."""
    artifacts = evidence.manifest.artifacts
    if not projection.records:
        if artifacts:
            raise ContractViolation("locked demand EMPTY에 output artifact가 있습니다.")
        return
    if len(artifacts) != 1 or artifacts[0].role != "station_demand_forecast":
        raise ContractViolation("locked demand output artifact가 잘못됐습니다.")
    payload = object_store.read_bytes(
        artifacts[0].uri,
        artifacts[0].byte_sha256,
    )
    actual = demand_publisher.demand_records_from_parquet(
        payload,
        expected_base_dttm=projection.base_dttm,
        expected_sta_ids=projection.expected_sta_ids,
    )
    if actual != projection.records:
        raise ContractViolation(
            "locked demand output이 inference projection과 다릅니다."
        )


def _validate_sealed_weather_output(
    object_store: ImmutableObjectStore,
    evidence: VerifiedPublicationEvidence,
    projection: WeatherForecastProjection,
) -> None:
    """Locked weather projection과 sealed output actual bytes를 대조한다."""
    artifacts = evidence.manifest.artifacts
    if not projection.records:
        if artifacts:
            raise ContractViolation(
                "locked weather EMPTY에 output artifact가 있습니다."
            )
        return
    if len(artifacts) != 1 or artifacts[0].role != "weather_forecast":
        raise ContractViolation("locked weather output artifact가 잘못됐습니다.")
    payload = object_store.read_bytes(
        artifacts[0].uri,
        artifacts[0].byte_sha256,
    )
    if weather_forecast._records_from_parquet(payload) != projection.records:
        raise ContractViolation(
            "locked weather output이 resolver projection과 다릅니다."
        )


def _validate_target_records_locked(
    cursor: Cursor[tuple[Any, ...]],
    *,
    publication_keys: tuple[str, ...],
    station_records: tuple[StationRecord, ...],
    stock_records: tuple[StationStockRecord, ...],
    demand_records: tuple[demand_publisher.DemandForecastRecord, ...],
    weather_records: tuple[weather_forecast.WeatherForecastRecord, ...],
) -> None:
    """선택된 replay key의 DB target을 동일 lock snapshot에서 exact 대조한다."""
    if len(publication_keys) != len(set(publication_keys)) or not set(
        publication_keys
    ).issubset(_FINAL_PUBLICATION_KEYS):
        raise ContractViolation("replay target key subset이 잘못됐습니다.")
    keys = set(publication_keys)
    if (
        "station" in keys
        and station_release._db_station_records(cursor) != station_records
    ):
        raise ContractViolation("replay station target이 sealed projection과 다릅니다.")
    if "station_stock" in keys:
        cursor.execute(
            """
            SELECT sta_id, base_dttm, parking_bike_tot_cnt
              FROM station_stock
             ORDER BY sta_id COLLATE "C"
            """
        )
        actual_stock = tuple(StationStockRecord(*row) for row in cursor.fetchall())
        if actual_stock != stock_records:
            raise ContractViolation(
                "replay station_stock target이 sealed projection과 다릅니다."
            )
    if "station_demand_forecast" in keys:
        cursor.execute(
            """
            SELECT base_dttm,
                   sta_id,
                   predicted_dttm,
                   predicted_rent_cnt,
                   predicted_rtn_cnt
              FROM station_demand_forecast
             ORDER BY sta_id COLLATE "C", predicted_dttm
            """
        )
        actual_demand = tuple(
            demand_publisher.DemandForecastRecord(*row) for row in cursor.fetchall()
        )
        if actual_demand != demand_records:
            raise ContractViolation(
                "replay demand target이 sealed projection과 다릅니다."
            )
    if "weather_forecast" in keys:
        cursor.execute(
            """
            SELECT weather_grid_id,
                   forecast_dttm,
                   source_product_cd,
                   base_dttm,
                   sky_condition_cd,
                   precipitation_type_cd,
                   temperature,
                   precipitation_prob,
                   precipitation_amount,
                   humidity,
                   wind_speed
              FROM weather_forecast
             ORDER BY weather_grid_id, forecast_dttm
            """
        )
        actual_weather = tuple(
            weather_forecast.WeatherForecastRecord(*row) for row in cursor.fetchall()
        )
        if actual_weather != weather_records:
            raise ContractViolation(
                "replay weather target이 sealed projection과 다릅니다."
            )


def _weather_projection_from_artifacts(
    object_store: ImmutableObjectStore,
    *,
    short_term_artifact: SourceManifestArtifact,
    ultra_short_artifact: SourceManifestArtifact,
    short_input: InputArtifact,
    ultra_input: InputArtifact,
    active_grids: tuple[str, ...],
    anchor: datetime,
) -> WeatherForecastProjection:
    """두 actual source manifest와 explicit incoming grid로 weather를 resolve한다."""
    return weather_forecast._projection_from_source_artifacts(
        object_store,
        short_artifact=short_term_artifact,
        ultra_artifact=ultra_short_artifact,
        short_input=short_input,
        ultra_input=ultra_input,
        active_grids=active_grids,
        anchor=anchor,
    )


def _activation_ready_station_ids(
    object_store: ImmutableObjectStore,
    *,
    provisional: StationProjection,
    short_term_artifact: SourceManifestArtifact,
    ultra_short_artifact: SourceManifestArtifact,
    short_input: InputArtifact,
    ultra_input: InputArtifact,
    anchor: datetime,
    current_stock_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """13시간 weather와 valid current stock이 모두 있는 station ID를 반환한다."""
    if type(current_stock_ids) is not tuple:
        raise ContractViolation("current stock ID는 tuple이어야 합니다.")
    if build_id_set(current_stock_ids).ids != current_stock_ids:
        raise ContractViolation("current stock ID는 중복 없는 UTF-8 순이어야 합니다.")
    stock_ids = set(current_stock_ids)
    covered_grids = set(_active_grid_ids(provisional.records))
    for grid_id in sorted(
        {
            record.weather_grid_id
            for record in provisional.records
            if not record.is_active
        },
        key=lambda value: value.encode("utf-8"),
    ):
        try:
            _weather_projection_from_artifacts(
                object_store,
                short_term_artifact=short_term_artifact,
                ultra_short_artifact=ultra_short_artifact,
                short_input=short_input,
                ultra_input=ultra_input,
                active_grids=(grid_id,),
                anchor=anchor,
            )
        except ContractViolation as exc:
            if "coverage가 완전하지 않습니다" not in str(exc):
                raise
        else:
            covered_grids.add(grid_id)
    return tuple(
        record.sta_id
        for record in provisional.records
        if (record.sta_id in stock_ids and record.weather_grid_id in covered_grids)
    )


def _weather_outputs(
    projection: WeatherForecastProjection,
) -> tuple[OutputObject, ...]:
    """Weather projection을 conditional EMPTY 또는 exact output tuple로 바꾼다."""
    if not projection.records:
        return ()
    return (
        OutputObject(
            role="weather_forecast",
            payload=weather_forecast._records_to_parquet(projection.records),
            row_count=len(projection.records),
        ),
    )


def _active_station_ids(records: tuple[StationRecord, ...]) -> tuple[str, ...]:
    """Station projection에서 active ID를 existing canonical 순서로 반환한다."""
    return tuple(record.sta_id for record in records if record.is_active)


def _inference_expected_station_ids(
    active_ids: tuple[str, ...],
    rental_ids: tuple[str, ...],
    return_ids: tuple[str, ...],
    eligible_ids: tuple[str, ...],
) -> tuple[str, ...]:
    """알려진 입력 결측을 격리하고 안전한 exact inference 집합을 반환한다."""
    model_ready_ids = set(active_ids) & set(rental_ids) & set(return_ids)
    ineligible_ids = model_ready_ids.difference(eligible_ids)
    ineligible_ratio = (
        len(ineligible_ids) / len(model_ready_ids) if model_ready_ids else 0.0
    )
    if ineligible_ratio > MAX_INFERENCE_INELIGIBLE_RATIO:
        raise ContractViolation(
            "추론 입력 품질 제외율이 기준을 초과했습니다: "
            f"excluded={len(ineligible_ids)} candidate={len(model_ready_ids)} "
            f"ratio={ineligible_ratio:.3%} "
            f"limit={MAX_INFERENCE_INELIGIBLE_RATIO:.3%}"
        )
    return tuple(
        sorted(
            model_ready_ids & set(eligible_ids),
            key=lambda value: value.encode("utf-8"),
        )
    )


def _active_grid_ids(records: tuple[StationRecord, ...]) -> tuple[str, ...]:
    """Station projection에서 active distinct grid를 UTF-8 순으로 반환한다."""
    return tuple(
        sorted(
            {record.weather_grid_id for record in records if record.is_active},
            key=lambda value: value.encode("utf-8"),
        )
    )


def _validate_latest_plan_sources(
    source_catalog: S3SourceSnapshotCatalog,
    *,
    master_artifact: SourceManifestArtifact,
    realtime_candidate: SourceManifestArtifact,
    short_term_artifact: SourceManifestArtifact,
    ultra_short_artifact: SourceManifestArtifact,
    anchor: datetime,
    lookbacks: SourceLookbacks,
) -> None:
    """Plan 시작에 네 source가 anchor 이전 최신 correction인지 검증한다."""
    master = source_catalog.latest_at_or_before(
        station_release.BIKE_STATION_MASTER_SOURCE_ID,
        anchor,
        lookback=lookbacks.master,
    )
    if not station_release._same_source_artifact(master, master_artifact):
        raise ContractViolation(
            "station master가 anchor 이전 최신 authority가 아닙니다."
        )
    realtime = source_catalog.recent_windows(
        station_release.BIKE_STATION_REALTIME_SOURCE_ID,
        anchor,
        limit=3,
        lookback=lookbacks.realtime,
    )
    if not realtime or not station_release._same_source_artifact(
        realtime[0],
        realtime_candidate,
    ):
        raise ContractViolation("realtime candidate가 최신 authority가 아닙니다.")
    weather_forecast._require_latest_source(
        source_catalog,
        short_term_artifact,
        weather_forecast.SHORT_TERM_SOURCE_ID,
        anchor,
        lookbacks.short_term,
    )
    weather_forecast._require_latest_source(
        source_catalog,
        ultra_short_artifact,
        weather_forecast.ULTRA_SHORT_SOURCE_ID,
        anchor,
        lookbacks.ultra_short,
    )


def _station_sources_from_prepared(
    object_store: ImmutableObjectStore,
    prepared: PreparedPublication,
) -> tuple[SourceManifestArtifact, tuple[SourceManifestArtifact, ...]]:
    """Station fingerprint actual bytes에서 master와 exact realtime window를 복원한다."""
    master = station_release._source_artifact_from_input(
        object_store,
        station_release._input_by_role(
            prepared.input_fingerprint,
            "bike_station_master_manifest",
        ),
        station_release.BIKE_STATION_MASTER_SOURCE_ID,
    )
    window_input = station_release._input_by_role(
        prepared.input_fingerprint,
        "station_realtime_window_set",
    )
    window_payload = object_store.read_bytes(
        window_input.uri,
        window_input.byte_sha256,
        require_canonical_json=True,
    )
    window_set = parse_station_realtime_window_set(window_payload)
    windows = tuple(
        station_release._source_artifact_from_input(
            object_store,
            InputArtifact(
                byte_sha256=window.byte_sha256,
                role="bike_station_realtime_manifest",
                uri=window.uri,
            ),
            station_release.BIKE_STATION_REALTIME_SOURCE_ID,
        )
        for window in window_set.windows
    )
    return master, windows


def _weather_sources_from_prepared(
    object_store: ImmutableObjectStore,
    prepared: PreparedPublication,
) -> tuple[SourceManifestArtifact, SourceManifestArtifact]:
    """Weather fingerprint actual manifest bytes를 source artifact로 복원한다."""
    return tuple(
        _source_artifact_from_input(
            object_store, _input_by_role(prepared.input_fingerprint, role)
        )
        for role in ("short_term_manifest", "ultra_short_manifest")
    )  # type: ignore[return-value]


def _source_artifact_from_input(
    object_store: ImmutableObjectStore,
    artifact: InputArtifact,
) -> SourceManifestArtifact:
    """Input URI·SHA actual source manifest를 catalog 비교 artifact로 만든다."""
    payload = object_store.read_bytes(
        artifact.uri,
        artifact.byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_source_snapshot_manifest(payload)
    return SourceManifestArtifact(
        manifest=manifest,
        uri=artifact.uri,
        byte_sha256=artifact.byte_sha256,
        payload=payload,
    )


def _validate_inference_support_binding(
    object_store: ImmutableObjectStore,
    snapshot: demand_publisher.DemandInferenceSnapshot,
    *,
    rental_support_ref: IdSetArtifactRef,
    return_support_ref: IdSetArtifactRef,
) -> None:
    """Inference가 pin한 두 model manifest의 support ref를 plan과 대조한다."""
    rental_payload = object_store.read_bytes(
        snapshot.manifest.rental_model_manifest.uri,
        snapshot.manifest.rental_model_manifest.byte_sha256,
        require_canonical_json=True,
    )
    return_payload = object_store.read_bytes(
        snapshot.manifest.return_model_manifest.uri,
        snapshot.manifest.return_model_manifest.byte_sha256,
        require_canonical_json=True,
    )
    rental_manifest = parse_model_snapshot_manifest(rental_payload)
    return_manifest = parse_model_snapshot_manifest(return_payload)
    if (
        rental_manifest.support_sta_ids != rental_support_ref
        or return_manifest.support_sta_ids != return_support_ref
    ):
        raise ContractViolation(
            "inference model manifest support ref가 serving plan과 다릅니다."
        )


def _validate_inference_catalog_latest(
    catalog: InferenceRevisionCatalog,
    *,
    logical_dttm: datetime,
    revision_no: int,
    manifest_uri: str,
    manifest_sha256: str,
) -> None:
    """Catalog의 same-logical latest를 exact inference identity에 결합한다."""
    snapshot_method = getattr(catalog, "snapshot", None)
    if not callable(snapshot_method):
        raise ContractViolation("inference catalog에 snapshot 경계가 없습니다.")
    snapshot = snapshot_method(logical_dttm)
    records = getattr(snapshot, "records", None)
    logical = _utc(logical_dttm, "inference catalog logical_dttm")
    if type(records) is not tuple or not records:
        raise ContractViolation("inference catalog에 요청 logical revision이 없습니다.")
    latest = records[-1]
    if (
        _utc(
            getattr(latest, "logical_dttm", None),
            "inference catalog record logical_dttm",
        )
        != logical
        or getattr(latest, "revision_no", None) != revision_no
        or getattr(latest, "manifest_uri", None) != manifest_uri
        or getattr(latest, "manifest_byte_sha256", None) != manifest_sha256
    ):
        raise ContractViolation(
            "inference manifest가 catalog의 same-logical latest revision이 아닙니다."
        )


def _store_id_set(
    object_store: ImmutableObjectStore,
    *,
    object_base_uri: str,
    name: str,
    values: tuple[str, ...],
) -> IdSetArtifactRef:
    """Canonical Gold ID set을 plan namespace에 content-addressed로 저장한다."""
    id_set = build_id_set(values)
    uri = (
        f"{object_base_uri.rstrip('/')}/serving-plan/inputs/{name}/"
        f"sha256={id_set.sha256}.json"
    )
    object_store.put_once(
        uri,
        id_set.canonical_bytes,
        expected_sha256=id_set.sha256,
        require_canonical_json=True,
    )
    object_store.read_bytes(uri, id_set.sha256, require_canonical_json=True)
    return build_id_set_artifact_ref(id_set, uri)


def _read_station_id_set_ref(
    object_store: ImmutableObjectStore,
    reference: IdSetArtifactRef,
    name: str,
) -> tuple[str, ...]:
    """ID set ref actual canonical bytes와 ST-* ID를 함께 검증한다."""
    payload = object_store.read_bytes(
        reference.uri,
        reference.byte_sha256,
        require_canonical_json=True,
    )
    id_set = parse_id_set(payload)
    if (
        id_set.schema_version != reference.schema_version
        or id_set.sha256 != reference.byte_sha256
        or len(id_set.ids) != reference.id_count
    ):
        raise ContractViolation(f"{name} ID set actual bytes가 ref와 다릅니다.")
    values = demand_publisher._station_id_set(id_set.ids, name)
    if values != id_set.ids:
        raise ContractViolation(f"{name} ID set이 canonical 순서가 아닙니다.")
    return values


def _store_prepared_manifest(
    object_store: ImmutableObjectStore,
    prepared: PreparedPublication,
) -> None:
    """Prepared manifest를 non-authoritative immutable object로 쓰고 readback한다."""
    payload = prepared.manifest.canonical_bytes
    object_store.put_once(
        prepared.manifest_uri,
        payload,
        expected_sha256=prepared.manifest.sha256,
        require_canonical_json=True,
    )
    actual = object_store.read_bytes(
        prepared.manifest_uri,
        prepared.manifest.sha256,
        require_canonical_json=True,
    )
    if actual != payload:
        raise ContractViolation("prepared manifest readback이 exact bytes와 다릅니다.")


def _read_prepared_manifest(
    object_store: ImmutableObjectStore,
    reference: PreparedManifestRef,
) -> PreparedPublication:
    """Plan ref가 pin한 manifest·fingerprint actual bytes를 PreparedPublication으로 연다."""
    payload = object_store.read_bytes(
        reference.uri,
        reference.byte_sha256,
        require_canonical_json=True,
    )
    manifest = parse_publication_manifest(payload)
    if (
        manifest.sha256 != reference.byte_sha256
        or manifest.publication_key != reference.publication_key
    ):
        raise ContractViolation("prepared manifest actual bytes가 plan ref와 다릅니다.")
    fingerprint_payload = object_store.read_bytes(
        manifest.input_fingerprint_uri,
        manifest.input_fingerprint_sha256,
        require_canonical_json=True,
    )
    fingerprint = parse_input_fingerprint(
        fingerprint_payload,
        manifest.publication_key,
    )
    return PreparedPublication(manifest, reference.uri, fingerprint)


def _dependency_from_prepared(prepared: PreparedPublication) -> Dependency:
    """Prepared publication을 같은 transaction dependency 6-tuple로 바꾼다."""
    manifest = prepared.manifest
    return Dependency(
        artifact_set_sha256=manifest.artifact_set_sha256,
        input_fingerprint_sha256=manifest.input_fingerprint_sha256,
        logical_dttm=manifest.logical_dttm,
        manifest_uri=prepared.manifest_uri,
        publication_key=manifest.publication_key,
        revision_no=manifest.revision_no,
    )


def _load_plan_prior_states(
    connection: Connection[Any],
) -> tuple[PublicationStateRecord, ...]:
    """Plan이 준비하는 세 key의 현재 state를 canonical key 순서로 읽는다."""
    values = tuple(
        load_publication_state(connection, key) for key in _PLAN_PUBLICATION_KEYS
    )
    return tuple(value for value in values if value is not None)


def _state_for(
    states: tuple[PublicationStateRecord, ...],
    publication_key: str,
) -> PublicationStateRecord | None:
    """State tuple에서 key 하나를 0..1개로 반환한다."""
    matches = tuple(item for item in states if item.publication_key == publication_key)
    if len(matches) > 1:
        raise ContractViolation("serving plan prior state가 중복됐습니다.")
    return None if not matches else matches[0]


def _require_plan_prior_state(
    plan: ServingPlan,
    publication_key: str,
    actual: PublicationStateRecord | None,
) -> None:
    """Final 시작의 publication state가 plan 준비 시점과 같은지 검증한다."""
    expected = _state_for(plan.prior_states, publication_key)
    if expected != actual:
        raise ContractViolation(
            f"serving plan 준비 후 {publication_key} state가 바뀌었습니다."
        )


def _validate_locked_prior_states(
    cursor: Cursor[tuple[Any, ...]],
    plan: ServingPlan,
    prior_demand: PublicationStateRecord | None,
    evidence_by_key: Mapping[str, VerifiedPublicationEvidence],
) -> None:
    """Replay current 또는 준비 시 prior인지 publication lock 안에서 검증한다."""
    for key in _PLAN_PUBLICATION_KEYS:
        actual = publication_state_locked(cursor, key)
        if actual != _state_from_evidence(evidence_by_key[key]):
            _require_plan_prior_state(plan, key, actual)
    demand_key = "station_demand_forecast"
    demand_actual = publication_state_locked(cursor, demand_key)
    if (
        demand_actual != _state_from_evidence(evidence_by_key[demand_key])
        and demand_actual != prior_demand
    ):
        raise ContractViolation(
            "demand 준비 후 station_demand_forecast state가 바뀌었습니다."
        )


def _state_from_evidence(
    evidence: VerifiedPublicationEvidence,
) -> PublicationStateRecord:
    """Verified evidence를 DB publication state 비교 tuple로 바꾼다."""
    manifest = evidence.manifest
    return PublicationStateRecord(
        publication_key=manifest.publication_key,
        logical_dttm=manifest.logical_dttm,
        revision_no=manifest.revision_no,
        manifest_uri=evidence.manifest_uri,
        artifact_set_sha256=manifest.artifact_set_sha256,
        input_fingerprint_sha256=manifest.input_fingerprint_sha256,
        published_row_cnt=manifest.published_row_cnt,
    )


def _evidence_by_key(
    evidence: tuple[VerifiedPublicationEvidence, ...],
    expected_keys: tuple[str, ...],
) -> dict[str, VerifiedPublicationEvidence]:
    """Evidence key 집합을 exact로 검증해 mapping으로 반환한다."""
    result = {item.manifest.publication_key: item for item in evidence}
    if len(result) != len(evidence) or tuple(sorted(result)) != expected_keys:
        raise ContractViolation(
            f"serving release evidence key가 다릅니다: expected={expected_keys}"
        )
    return result


def _input_by_role(fingerprint: Any, role: str) -> InputArtifact:
    """Fingerprint에서 exact input role 하나를 반환한다."""
    matches = tuple(item for item in fingerprint.input_artifacts if item.role == role)
    if len(matches) != 1:
        raise ContractViolation(f"{role} input artifact가 정확히 하나가 아닙니다.")
    return matches[0]


def _plan_document(plan: ServingPlan) -> dict[str, Any]:
    """ServingPlan을 canonical serializer용 exact-key document로 바꾼다."""
    document = {
        "activation_ready_sta_ids": _id_set_ref_document(plan.activation_ready_sta_ids),
        "expected_sta_ids": _id_set_ref_document(plan.expected_sta_ids),
        "inference_eligible_sta_ids": _id_set_ref_document(
            plan.inference_eligible_sta_ids
        ),
        "logical_dttm": format_utc_dttm(plan.logical_dttm),
        "object_base_uri": plan.object_base_uri,
        "prepared_publications": [
            {
                "byte_sha256": item.byte_sha256,
                "publication_key": item.publication_key,
                "uri": item.uri,
            }
            for item in plan.prepared_publications
        ],
        "prior_states": [_state_document(item) for item in plan.prior_states],
        "rental_support_sta_ids": _id_set_ref_document(plan.rental_support_sta_ids),
        "return_support_sta_ids": _id_set_ref_document(plan.return_support_sta_ids),
        "schema_version": plan.schema_version,
        "source_lookbacks": {
            "master_seconds": _timedelta_seconds(plan.source_lookbacks.master),
            "realtime_seconds": _timedelta_seconds(plan.source_lookbacks.realtime),
            "short_term_seconds": _timedelta_seconds(plan.source_lookbacks.short_term),
            "ultra_short_seconds": _timedelta_seconds(
                plan.source_lookbacks.ultra_short
            ),
        },
        "station_dependency": _dependency_document(plan.station_dependency),
    }
    if plan.schema_version == SERVING_PLAN_SCHEMA_VERSION:
        assert plan.serving_release is not None
        assert plan.station_master_enriched is not None
        document["serving_release"] = _serving_release_ref_document(
            plan.serving_release
        )
        document["station_master_enriched"] = _input_artifact_document(
            plan.station_master_enriched
        )
    return document


def _serving_release_ref_document(reference: ServingReleaseRef) -> dict[str, Any]:
    """Serving release ref를 canonical plan document로 바꾼다."""
    return {
        "byte_sha256": reference.byte_sha256,
        "effective_contract_version": reference.effective_contract_version,
        "release_version": reference.release_version,
        "uri": reference.uri,
    }


def _input_artifact_document(reference: InputArtifact) -> dict[str, Any]:
    """Exact non-authority S3 input ref를 canonical plan document로 바꾼다."""
    return {
        "byte_sha256": reference.byte_sha256,
        "role": reference.role,
        "uri": reference.uri,
    }


def _id_set_ref_document(reference: IdSetArtifactRef) -> dict[str, Any]:
    """ID set ref를 canonical plan document로 바꾼다."""
    return {
        "byte_sha256": reference.byte_sha256,
        "id_count": reference.id_count,
        "schema_version": reference.schema_version,
        "uri": reference.uri,
    }


def _dependency_document(dependency: Dependency) -> dict[str, Any]:
    """Dependency를 canonical plan document로 바꾼다."""
    return {
        "artifact_set_sha256": dependency.artifact_set_sha256,
        "input_fingerprint_sha256": dependency.input_fingerprint_sha256,
        "logical_dttm": format_utc_dttm(dependency.logical_dttm),
        "manifest_uri": dependency.manifest_uri,
        "publication_key": dependency.publication_key,
        "revision_no": dependency.revision_no,
    }


def _state_document(state: PublicationStateRecord) -> dict[str, Any]:
    """Publication state를 canonical plan document로 바꾼다."""
    return {
        **_dependency_document(state.dependency),
        "published_row_cnt": state.published_row_cnt,
    }


def _parse_prepared_ref(value: Any) -> PreparedManifestRef:
    """Prepared publication reference exact object를 파싱한다."""
    document = _exact_object(value, _PREPARED_REF_KEYS, "prepared manifest ref")
    return PreparedManifestRef(
        publication_key=_string(document["publication_key"], "publication_key"),
        uri=_string(document["uri"], "prepared URI"),
        byte_sha256=_string(document["byte_sha256"], "prepared SHA"),
    )


def _parse_id_set_ref(value: Any, name: str) -> IdSetArtifactRef:
    """ID set reference exact object를 파싱한다."""
    document = _exact_object(value, _ID_SET_REF_KEYS, name)
    return IdSetArtifactRef(
        byte_sha256=_string(document["byte_sha256"], f"{name}.byte_sha256"),
        id_count=_nonnegative_int(document["id_count"], f"{name}.id_count"),
        schema_version=_string(
            document["schema_version"],
            f"{name}.schema_version",
        ),
        uri=_string(document["uri"], f"{name}.uri"),
    )


def _parse_serving_release_ref(value: Any) -> ServingReleaseRef:
    """Serving release reference exact object를 파싱한다."""
    document = _exact_object(
        value,
        _SERVING_RELEASE_REF_KEYS,
        "serving release ref",
    )
    return ServingReleaseRef(
        byte_sha256=_string(
            document["byte_sha256"],
            "serving release byte_sha256",
        ),
        effective_contract_version=_string(
            document["effective_contract_version"],
            "serving release effective_contract_version",
        ),
        release_version=_string(
            document["release_version"],
            "serving release release_version",
        ),
        uri=_string(document["uri"], "serving release uri"),
    )


def _parse_input_artifact(value: Any, expected_role: str) -> InputArtifact:
    """Plan의 exact S3 input artifact를 role까지 검증해 파싱한다."""
    document = _exact_object(value, _INPUT_ARTIFACT_KEYS, expected_role)
    artifact = InputArtifact(
        byte_sha256=_string(
            document["byte_sha256"],
            f"{expected_role}.byte_sha256",
        ),
        role=_string(document["role"], f"{expected_role}.role"),
        uri=_string(document["uri"], f"{expected_role}.uri"),
    )
    if artifact.role != expected_role:
        raise ContractViolation(f"{expected_role} role이 잘못됐습니다.")
    return artifact


def _parse_dependency(value: Any) -> Dependency:
    """Dependency exact object를 파싱한다."""
    document = _exact_object(value, _DEPENDENCY_KEYS, "station dependency")
    return Dependency(
        artifact_set_sha256=_string(
            document["artifact_set_sha256"],
            "dependency artifact_set_sha256",
        ),
        input_fingerprint_sha256=_string(
            document["input_fingerprint_sha256"],
            "dependency input_fingerprint_sha256",
        ),
        logical_dttm=parse_utc_dttm(
            _string(document["logical_dttm"], "dependency logical_dttm")
        ),
        manifest_uri=_string(document["manifest_uri"], "dependency manifest_uri"),
        publication_key=_string(
            document["publication_key"],
            "dependency publication_key",
        ),
        revision_no=_nonnegative_int(
            document["revision_no"],
            "dependency revision_no",
        ),
    )


def _parse_state(value: Any) -> PublicationStateRecord:
    """Publication state exact object를 파싱한다."""
    document = _exact_object(value, _STATE_KEYS, "prior state")
    dependency = _parse_dependency({key: document[key] for key in _DEPENDENCY_KEYS})
    return PublicationStateRecord(
        publication_key=dependency.publication_key,
        logical_dttm=dependency.logical_dttm,
        revision_no=dependency.revision_no,
        manifest_uri=dependency.manifest_uri,
        artifact_set_sha256=dependency.artifact_set_sha256,
        input_fingerprint_sha256=dependency.input_fingerprint_sha256,
        published_row_cnt=_nonnegative_int(
            document["published_row_cnt"],
            "prior state published_row_cnt",
        ),
    )


def _parse_lookbacks(value: Any) -> SourceLookbacks:
    """Whole-second lookback exact object를 파싱한다."""
    document = _exact_object(value, _LOOKBACK_KEYS, "source lookbacks")
    return SourceLookbacks(
        master=timedelta(
            seconds=_positive_int(document["master_seconds"], "master_seconds")
        ),
        realtime=timedelta(
            seconds=_positive_int(
                document["realtime_seconds"],
                "realtime_seconds",
            )
        ),
        short_term=timedelta(
            seconds=_positive_int(
                document["short_term_seconds"],
                "short_term_seconds",
            )
        ),
        ultra_short=timedelta(
            seconds=_positive_int(
                document["ultra_short_seconds"],
                "ultra_short_seconds",
            )
        ),
    )


def _exact_object(value: Any, keys: frozenset[str], name: str) -> dict[str, Any]:
    """JSON value가 exact key 집합을 가진 object인지 검증한다."""
    if type(value) is not dict:
        raise ContractViolation(f"{name}은 JSON object여야 합니다.")
    document = cast(dict[str, Any], value)
    if frozenset(document) != keys:
        raise ContractViolation(
            f"{name} key가 정확하지 않습니다: "
            f"missing={sorted(keys - frozenset(document))}, "
            f"extra={sorted(frozenset(document) - keys)}"
        )
    return document


def _array(value: Any, name: str) -> list[Any]:
    """JSON value가 array인지 검증한다."""
    if type(value) is not list:
        raise ContractViolation(f"{name}은 JSON array여야 합니다.")
    return cast(list[Any], value)


def _string(value: Any, name: str) -> str:
    """JSON value가 문자열인지 검증한다."""
    if type(value) is not str:
        raise ContractViolation(f"{name}은 문자열이어야 합니다.")
    return value


def _nonblank(value: Any, name: str) -> str:
    """값이 nonblank 문자열인지 검증한다."""
    text = _string(value, name)
    if not text.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return text


def _nonnegative_int(value: Any, name: str) -> int:
    """JSON value가 bool이 아닌 0 이상 integer인지 검증한다."""
    if type(value) is not int or value < 0:
        raise ContractViolation(f"{name}은 0 이상 integer여야 합니다.")
    return value


def _positive_int(value: Any, name: str) -> int:
    """JSON value가 bool이 아닌 양의 integer인지 검증한다."""
    result = _nonnegative_int(value, name)
    if result == 0:
        raise ContractViolation(f"{name}은 양의 integer여야 합니다.")
    return result


def _timedelta_seconds(value: timedelta) -> int:
    """Whole-second positive timedelta를 canonical integer seconds로 바꾼다."""
    seconds = value.days * 86400 + value.seconds
    if value.microseconds or seconds <= 0:
        raise ContractViolation("lookback은 양의 whole-second timedelta여야 합니다.")
    return seconds


def _utc(value: Any, name: str) -> datetime:
    """Timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation(f"{name}은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)


def _require_s3_base_uri(value: Any) -> str:
    """Object base가 object key를 가진 query 없는 S3 URI인지 검증한다."""
    text = _nonblank(value, "object_base_uri")
    if not text.startswith("s3://") or "?" in text or "#" in text:
        raise ContractViolation("object_base_uri는 s3:// URI여야 합니다.")
    bucket, separator, key = text.removeprefix("s3://").partition("/")
    if not bucket or not separator or not key or key.startswith("/"):
        raise ContractViolation("object_base_uri는 bucket과 key가 필요합니다.")
    return text


def _require_same_s3_bucket(uri: str, base_uri: str, name: str) -> None:
    """Exact ref와 plan object base가 같은 S3 bucket인지 검증한다."""
    reference = urlsplit(uri)
    base = urlsplit(base_uri)
    if (
        reference.scheme != "s3"
        or reference.netloc != base.netloc
        or not reference.path.lstrip("/")
        or reference.query
        or reference.fragment
    ):
        raise ContractViolation(f"{name} URI가 serving plan bucket과 다릅니다.")


def _require_enriched_master_ref(
    reference: InputArtifact,
    object_base_uri: str,
) -> None:
    """Enriched master ref를 same-bucket Silver Parquet identity로 제한한다."""
    if reference.role != _STATION_MASTER_ENRICHED_ROLE:
        raise ContractViolation("station_master_enriched role이 잘못됐습니다.")
    _require_same_s3_bucket(
        reference.uri,
        object_base_uri,
        "station_master_enriched",
    )
    key = urlsplit(reference.uri).path.lstrip("/")
    if not key.startswith("silver/station_master_enriched/") or not key.endswith(
        ".parquet"
    ):
        raise ContractViolation("station_master_enriched URI 경로가 잘못됐습니다.")


def _require_catalog(value: Any) -> None:
    """Source catalog exact concrete type을 검증한다."""
    if type(value) is not S3SourceSnapshotCatalog:
        raise ContractViolation("source_catalog type이 잘못됐습니다.")


def _require_idle(connection: Connection[Any]) -> None:
    """Plan과 final API가 열린 transaction 밖에서 시작하는지 검증한다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "serving plan API는 열린 transaction을 받을 수 없습니다."
        )
