"""ML inference rows를 완전한 12시간 Gold 수요 projection으로 변환한다."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

import pyarrow as pa
from core.gold_publication import (
    ContractViolation,
    ImmutableObjectStore,
    InputArtifact,
    Parameter,
    PreparedPublication,
    VerifiedPublicationEvidence,
    build_id_set,
    parse_id_set,
    validate_id_set_parameter,
)
from core.inference_snapshot import (
    InferenceSnapshotManifest,
    InferenceSnapshotStatus,
    ModelManifestRef,
    inference_output_input_artifact,
    parse_inference_output_parquet,
    parse_inference_snapshot_manifest,
    validate_model_manifest_binding,
)
from core.model_snapshot import (
    IdSetArtifactRef,
    ModelSnapshotManifest,
    model_manifest_input_artifact,
    parse_model_snapshot_manifest,
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
)
from .state import load_dependencies
from .versioning import PublicationCandidate, allocate_revision

HORIZON_COUNT = 12
POSTGRES_INTEGER_MAX = 2_147_483_647
DEMAND_PUBLISHER_VERSION = "gold-demand-publisher-v1"
ROUNDING_MODE = "roundTiesToEven"
_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_DEMAND_FORECAST_SCHEMA = pa.schema(
    (
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("predicted_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("predicted_rent_cnt", pa.int32(), nullable=False),
        pa.field("predicted_rtn_cnt", pa.int32(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True)
class DemandInferenceSnapshot:
    """검증된 inference authority와 transitive model·ID·예측 bytes를 묶는다."""

    manifest: InferenceSnapshotManifest
    inference_input: InputArtifact
    rental_model_input: InputArtifact
    return_model_input: InputArtifact
    rental_support_sta_ids: tuple[str, ...]
    return_support_sta_ids: tuple[str, ...]
    expected_sta_ids: tuple[str, ...]
    predictions: tuple[DemandPredictionRecord, ...]

    def __post_init__(self) -> None:
        """Snapshot field가 exact typed contract와 canonical station 집합인지 검증한다."""
        if type(self.manifest) is not InferenceSnapshotManifest:
            raise ContractViolation(
                "demand inference manifest type이 올바르지 않습니다."
            )
        for value, role in (
            (self.inference_input, "inference_output"),
            (self.rental_model_input, "rental_model_manifest"),
            (self.return_model_input, "return_model_manifest"),
        ):
            if type(value) is not InputArtifact or value.role != role:
                raise ContractViolation(
                    f"demand inference input role이 잘못됐습니다: expected={role}"
                )
        for values, name in (
            (self.rental_support_sta_ids, "rental support"),
            (self.return_support_sta_ids, "return support"),
            (self.expected_sta_ids, "inference expected"),
        ):
            if values != _station_id_set(values, name):
                raise ContractViolation(f"{name} ID가 canonical 순서가 아닙니다.")
        if type(self.predictions) is not tuple or any(
            type(record) is not DemandPredictionRecord for record in self.predictions
        ):
            raise ContractViolation(
                "inference predictions는 DemandPredictionRecord tuple이어야 합니다."
            )


@dataclass(frozen=True, slots=True)
class DemandPredictionRecord:
    """Gold 변환 직전의 typed inference mean 예측 행을 표현한다."""

    base_dttm: datetime
    station_id: str
    horizon: int
    target_dttm: datetime
    rental_pred_mean: float
    return_pred_mean: float

    def __post_init__(self) -> None:
        """source의 base·horizon·구간 시작시각·float64 계약을 검증한다."""
        base = _utc_dttm(self.base_dttm, "inference base_dttm")
        target = _utc_dttm(self.target_dttm, "inference target_dttm")
        object.__setattr__(self, "base_dttm", base)
        object.__setattr__(self, "target_dttm", target)
        _station_id(self.station_id)
        if type(self.horizon) is not int or not 1 <= self.horizon <= HORIZON_COUNT:
            raise ContractViolation("demand horizon은 정확히 1..12여야 합니다.")
        expected_target = _add_hours(
            base,
            self.horizon - 1,
            "inference target",
        )
        if target != expected_target:
            raise ContractViolation(
                "inference target은 base+(horizon-1)시간이어야 합니다."
            )
        _prediction_mean(self.rental_pred_mean, "rental_pred_mean")
        _prediction_mean(self.return_pred_mean, "return_pred_mean")


@dataclass(frozen=True, slots=True)
class DemandForecastRecord:
    """Gold station_demand_forecast 전체 교체 행을 표현한다."""

    base_dttm: datetime
    sta_id: str
    predicted_dttm: datetime
    predicted_rent_cnt: int
    predicted_rtn_cnt: int

    def __post_init__(self) -> None:
        """target DDL의 key·시간·PostgreSQL INTEGER 계약을 검증한다."""
        base = _utc_dttm(self.base_dttm, "demand base_dttm")
        predicted = _utc_dttm(self.predicted_dttm, "predicted_dttm")
        object.__setattr__(self, "base_dttm", base)
        object.__setattr__(self, "predicted_dttm", predicted)
        _station_id(self.sta_id)
        if predicted <= base:
            raise ContractViolation("predicted_dttm은 base_dttm 후여야 합니다.")
        _postgres_nonnegative_integer(self.predicted_rent_cnt, "predicted_rent_cnt")
        _postgres_nonnegative_integer(self.predicted_rtn_cnt, "predicted_rtn_cnt")


@dataclass(frozen=True, slots=True)
class DemandProjection:
    """active·두 모델 공통 지원 station의 완전 12시간 projection을 표현한다."""

    base_dttm: datetime
    expected_sta_ids: tuple[str, ...]
    records: tuple[DemandForecastRecord, ...]

    def __post_init__(self) -> None:
        """expected key 집합·공통 base·canonical row 순서를 검증한다."""
        base = _utc_dttm(self.base_dttm, "projection base_dttm")
        object.__setattr__(self, "base_dttm", base)
        if type(self.expected_sta_ids) is not tuple:
            raise ContractViolation("expected station ID는 tuple이어야 합니다.")
        expected_ids = _station_id_set(self.expected_sta_ids, "expected station")
        if self.expected_sta_ids != expected_ids:
            raise ContractViolation(
                "expected station ID는 중복 없이 UTF-8 순이어야 합니다."
            )
        if type(self.records) is not tuple or any(
            type(record) is not DemandForecastRecord for record in self.records
        ):
            raise ContractViolation(
                "demand records는 DemandForecastRecord tuple이어야 합니다."
            )
        for record in self.records:
            if record.base_dttm != base:
                raise ContractViolation(
                    "demand projection에 서로 다른 base_dttm이 섞였습니다."
                )
        ordered = tuple(
            sorted(
                self.records,
                key=lambda record: (
                    record.sta_id.encode("utf-8"),
                    record.predicted_dttm,
                ),
            )
        )
        if self.records != ordered:
            raise ContractViolation(
                "demand records는 (sta_id,predicted_dttm) 순이어야 합니다."
            )
        actual_keys = tuple(
            (record.sta_id, record.predicted_dttm) for record in self.records
        )
        if len(actual_keys) != len(set(actual_keys)):
            raise ContractViolation("demand projection에 중복 target key가 있습니다.")
        expected_keys = {
            (station_id, _add_hours(base, horizon, "demand predicted_dttm"))
            for station_id in expected_ids
            for horizon in range(1, HORIZON_COUNT + 1)
        }
        if set(actual_keys) != expected_keys:
            missing = len(expected_keys - set(actual_keys))
            extra = len(set(actual_keys) - expected_keys)
            raise ContractViolation(
                "demand projection이 station×horizon 1..12로 완전하지 않습니다: "
                f"missing={missing}, extra={extra}"
            )


def build_demand_projection(
    predictions: tuple[DemandPredictionRecord, ...],
    *,
    base_dttm: datetime,
    active_station_ids: tuple[str, ...],
    rental_model_station_ids: tuple[str, ...],
    return_model_station_ids: tuple[str, ...],
) -> DemandProjection:
    """typed inference를 active·두 모델 교집합의 완전한 Gold 행으로 만든다."""
    base = _utc_dttm(base_dttm, "projection base_dttm")
    if type(predictions) is not tuple or any(
        type(record) is not DemandPredictionRecord for record in predictions
    ):
        raise ContractViolation(
            "predictions는 DemandPredictionRecord tuple이어야 합니다."
        )
    active = _station_id_set(active_station_ids, "active station")
    rental = _station_id_set(rental_model_station_ids, "rental model station")
    returned = _station_id_set(return_model_station_ids, "return model station")
    expected_ids = tuple(
        sorted(
            set(active) & set(rental) & set(returned),
            key=lambda value: value.encode("utf-8"),
        )
    )
    indexed: dict[tuple[str, int], DemandPredictionRecord] = {}
    for prediction in predictions:
        if prediction.base_dttm != base:
            raise ContractViolation(
                "inference rows에 서로 다른 base_dttm이 섞였거나 anchor와 다릅니다."
            )
        key = (prediction.station_id, prediction.horizon)
        if key in indexed:
            raise ContractViolation("inference에 중복 station·horizon이 있습니다.")
        indexed[key] = prediction
    expected_keys = {
        (station_id, horizon)
        for station_id in expected_ids
        for horizon in range(1, HORIZON_COUNT + 1)
    }
    actual_keys = set(indexed)
    if actual_keys != expected_keys:
        missing = len(expected_keys - actual_keys)
        extra = len(actual_keys - expected_keys)
        raise ContractViolation(
            "inference가 active∩rental∩return×horizon 1..12와 다릅니다: "
            f"missing={missing}, extra={extra}"
        )
    records = tuple(
        DemandForecastRecord(
            base_dttm=base,
            sta_id=prediction.station_id,
            predicted_dttm=_add_hours(
                base,
                prediction.horizon,
                "demand predicted_dttm",
            ),
            predicted_rent_cnt=_round_prediction(
                prediction.rental_pred_mean, "rental_pred_mean"
            ),
            predicted_rtn_cnt=_round_prediction(
                prediction.return_pred_mean, "return_pred_mean"
            ),
        )
        for prediction in sorted(
            predictions,
            key=lambda record: (
                record.station_id.encode("utf-8"),
                record.horizon,
            ),
        )
    )
    return DemandProjection(base, expected_ids, records)


def demand_records_to_parquet(
    records: tuple[DemandForecastRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
) -> bytes:
    """authoritative 기대 집합의 nonempty demand를 고정 schema Parquet으로 만든다."""
    _validate_record_snapshot(records, expected_sta_ids=expected_sta_ids)
    table = pa.Table.from_pylist(
        [
            {
                "base_dttm": record.base_dttm,
                "sta_id": record.sta_id,
                "predicted_dttm": record.predicted_dttm,
                "predicted_rent_cnt": record.predicted_rent_cnt,
                "predicted_rtn_cnt": record.predicted_rtn_cnt,
            }
            for record in records
        ],
        schema=_DEMAND_FORECAST_SCHEMA,
    )
    return parquet_bytes(table)


def demand_records_from_parquet(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[DemandForecastRecord, ...]:
    """고정 schema demand Parquet을 기대 anchor·집합과 함께 다시 검증한다."""
    table = read_parquet_bytes(payload)
    if table.schema != _DEMAND_FORECAST_SCHEMA:
        raise ContractViolation("demand output Parquet schema가 exact 계약과 다릅니다.")
    records = tuple(DemandForecastRecord(**row) for row in table.to_pylist())
    _validate_record_snapshot(
        records,
        expected_sta_ids=expected_sta_ids,
        expected_base_dttm=expected_base_dttm,
    )
    return records


def publish_station_demand_forecast(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    inference_manifest_uri: str,
    inference_manifest_sha256: str,
    object_base_uri: str,
    publisher_version: str = DEMAND_PUBLISHER_VERSION,
) -> PublicationExecution:
    """완전한 inference snapshot을 최신 Gold demand projection으로 원자 게시한다.

    Inference authority manifest bytes와 manifest가 pin한 output, 두 model manifest,
    세 ID set을 exact URI·SHA로 읽는다. Gold dependency와 기대 station 집합은
    topology shared lock 안에서 다시 증명한 뒤 전체 projection과 state를 같은
    transaction에서 reconcile한다.
    """
    snapshot = _read_inference_snapshot(
        object_store,
        inference_manifest_uri=inference_manifest_uri,
        inference_manifest_sha256=inference_manifest_sha256,
    )
    dependencies = load_dependencies(connection, ("station",))
    if dependencies != (snapshot.manifest.station_dependency,):
        raise ContractViolation(
            "inference station dependency가 현재 Gold station state와 다릅니다."
        )
    active_sta_ids = _load_active_station_ids(connection)
    projection = _projection_from_snapshot(snapshot, active_sta_ids=active_sta_ids)
    outputs = (
        ()
        if not projection.records
        else (
            OutputObject(
                role="station_demand_forecast",
                payload=demand_records_to_parquet(
                    projection.records,
                    expected_sta_ids=projection.expected_sta_ids,
                ),
                row_count=len(projection.records),
            ),
        )
    )
    expected_ids = build_id_set(projection.expected_sta_ids)
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="station_demand_forecast",
        dependencies=dependencies,
        input_artifacts=(
            snapshot.inference_input,
            snapshot.rental_model_input,
            snapshot.return_model_input,
        ),
        parameters=(
            Parameter("expected_sta_id_sha256", expected_ids.sha256),
            Parameter("horizon_count", str(HORIZON_COUNT)),
            Parameter("rounding_mode", ROUNDING_MODE),
        ),
        outputs=outputs,
    )
    revision_no = allocate_revision(
        connection,
        PublicationCandidate(
            publication_key="station_demand_forecast",
            logical_dttm=snapshot.manifest.logical_dttm,
            artifact_set_sha256=materials.artifact_set.sha256,
            input_fingerprint_sha256=materials.input_fingerprint.sha256,
            published_row_cnt=len(projection.records),
        ),
    )
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="station_demand_forecast",
        logical_dttm=snapshot.manifest.logical_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={"station_demand_forecast": len(projection.records)},
        materials=materials,
        conditional_empty_candidate=not projection.records,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """Verifier actual manifest bytes와 transitive bytes로 projection을 재구성한다."""
        if publication.manifest.publication_key != "station_demand_forecast":
            raise ContractViolation("demand prepared publication key가 다릅니다.")
        verified = _read_inference_snapshot_from_verified_inputs(
            object_store,
            inference_input=snapshot.inference_input,
            rental_model_input=snapshot.rental_model_input,
            return_model_input=snapshot.return_model_input,
            payloads=payloads,
        )
        expected = _projection_from_snapshot(
            verified,
            active_sta_ids=active_sta_ids,
        )
        _validate_demand_artifact(publication, payloads, expected)
        return {"base_dttm": tuple(record.base_dttm for record in expected.records)}

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Publication lock 안에서 station dependency·교집합·12 horizon을 재증명한다."""
        item = _require_demand_evidence(evidence)
        locked_active = _active_station_ids_locked(cursor)
        locked_projection = _projection_from_snapshot(
            snapshot,
            active_sta_ids=locked_active,
        )
        if locked_projection != projection:
            raise ContractViolation(
                "demand staging 이후 active·model support projection이 바뀌었습니다."
            )
        validate_id_set_parameter(
            "station_demand_forecast",
            item.input_fingerprint,
            build_id_set(locked_projection.expected_sta_ids),
        )

    def validate_conditional_empty(
        cursor: Cursor[tuple[Any, ...]],
        evidence: VerifiedPublicationEvidence,
    ) -> bool:
        """EMPTY가 lock 안 active·두 model support 교집합 0개인지 증명한다."""
        if evidence.manifest.publication_key != "station_demand_forecast":
            raise ContractViolation("demand EMPTY evidence key가 다릅니다.")
        locked_projection = _projection_from_snapshot(
            snapshot,
            active_sta_ids=_active_station_ids_locked(cursor),
        )
        validate_id_set_parameter(
            "station_demand_forecast",
            evidence.input_fingerprint,
            build_id_set(locked_projection.expected_sta_ids),
        )
        return not locked_projection.records

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """검증된 demand 전체 projection을 claim과 같은 transaction에서 교체한다."""
        _require_demand_evidence(evidence)
        _reconcile_demand_records(cursor, projection.records)

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
        validate_conditional_empty=validate_conditional_empty,
    )


def demand_predictions_from_inference_parquet(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[DemandPredictionRecord, ...]:
    """Core exact authority Parquet을 Gold typed prediction으로 변환한다."""
    base = _utc_dttm(expected_base_dttm, "inference expected base_dttm")
    expected = build_id_set(_station_id_set(expected_sta_ids, "inference expected"))
    table = parse_inference_output_parquet(
        payload,
        logical_dttm=base,
        expected_sta_ids=expected,
    )
    return tuple(
        DemandPredictionRecord(
            base_dttm=base,
            station_id=row["station_id"],
            horizon=row["horizon"],
            target_dttm=_add_hours(
                base,
                row["horizon"] - 1,
                "inference target",
            ),
            rental_pred_mean=row["rental_pred_mean"],
            return_pred_mean=row["return_pred_mean"],
        )
        for row in table.to_pylist()
    )


def _read_inference_snapshot(
    object_store: ImmutableObjectStore,
    *,
    inference_manifest_uri: str,
    inference_manifest_sha256: str,
) -> DemandInferenceSnapshot:
    """URI·SHA로 actual inference manifest와 모든 transitive authority를 읽는다."""
    payload = object_store.read_bytes(
        inference_manifest_uri,
        inference_manifest_sha256,
        require_canonical_json=True,
    )
    manifest = parse_inference_snapshot_manifest(payload)
    inference_input = inference_output_input_artifact(
        manifest,
        inference_manifest_uri,
    )
    if inference_input.byte_sha256 != inference_manifest_sha256:
        raise ContractViolation(
            "inference manifest argument SHA가 canonical actual bytes와 다릅니다."
        )
    rental_payload = object_store.read_bytes(
        manifest.rental_model_manifest.uri,
        manifest.rental_model_manifest.byte_sha256,
        require_canonical_json=True,
    )
    return_payload = object_store.read_bytes(
        manifest.return_model_manifest.uri,
        manifest.return_model_manifest.byte_sha256,
        require_canonical_json=True,
    )
    return _build_inference_snapshot(
        object_store,
        manifest=manifest,
        inference_input=inference_input,
        rental_model_payload=rental_payload,
        return_model_payload=return_payload,
    )


def _read_inference_snapshot_from_verified_inputs(
    object_store: ImmutableObjectStore,
    *,
    inference_input: InputArtifact,
    rental_model_input: InputArtifact,
    return_model_input: InputArtifact,
    payloads: Mapping[str, bytes],
) -> DemandInferenceSnapshot:
    """공통 verifier가 읽은 세 manifest actual bytes에서 snapshot을 다시 만든다."""
    try:
        inference_payload = payloads[inference_input.uri]
        rental_payload = payloads[rental_model_input.uri]
        return_payload = payloads[return_model_input.uri]
    except KeyError as exc:
        raise ContractViolation(
            "demand verifier payload에 필수 manifest bytes가 없습니다."
        ) from exc
    manifest = parse_inference_snapshot_manifest(inference_payload)
    actual_inference_input = inference_output_input_artifact(
        manifest,
        inference_input.uri,
    )
    if actual_inference_input != inference_input:
        raise ContractViolation(
            "verified inference manifest bytes가 fingerprint input과 다릅니다."
        )
    snapshot = _build_inference_snapshot(
        object_store,
        manifest=manifest,
        inference_input=actual_inference_input,
        rental_model_payload=rental_payload,
        return_model_payload=return_payload,
    )
    if (
        snapshot.rental_model_input != rental_model_input
        or snapshot.return_model_input != return_model_input
    ):
        raise ContractViolation(
            "verified model manifest bytes가 fingerprint input과 다릅니다."
        )
    return snapshot


def _build_inference_snapshot(
    object_store: ImmutableObjectStore,
    *,
    manifest: InferenceSnapshotManifest,
    inference_input: InputArtifact,
    rental_model_payload: bytes,
    return_model_payload: bytes,
) -> DemandInferenceSnapshot:
    """Actual model·ID·output bytes를 manifest reference와 결합한다."""
    rental_model, rental_input, rental_support = _model_snapshot_from_payload(
        object_store,
        manifest.rental_model_manifest,
        rental_model_payload,
    )
    return_model, return_input, return_support = _model_snapshot_from_payload(
        object_store,
        manifest.return_model_manifest,
        return_model_payload,
    )
    del rental_model, return_model
    expected_ids = _read_id_set_artifact(
        object_store,
        manifest.expected_sta_ids,
        "inference expected",
    )
    if manifest.status is InferenceSnapshotStatus.EMPTY:
        predictions: tuple[DemandPredictionRecord, ...] = ()
    else:
        if manifest.output is None:
            raise ContractViolation("SUCCEEDED inference output reference가 없습니다.")
        output_payload = object_store.read_bytes(
            manifest.output.uri,
            manifest.output.byte_sha256,
        )
        table = read_parquet_bytes(output_payload)
        if table.num_rows != manifest.output.row_count:
            raise ContractViolation(
                "inference output actual row count가 manifest와 다릅니다."
            )
        predictions = demand_predictions_from_inference_parquet(
            output_payload,
            expected_base_dttm=manifest.logical_dttm,
            expected_sta_ids=expected_ids,
        )
    if len(predictions) != manifest.counts.actual_row_count:
        raise ContractViolation(
            "inference prediction actual row count가 manifest counts와 다릅니다."
        )
    actual_station_ids = _station_id_set(
        tuple({record.station_id for record in predictions}),
        "inference actual station",
    )
    if len(actual_station_ids) != manifest.counts.actual_station_count:
        raise ContractViolation(
            "inference actual station count가 manifest counts와 다릅니다."
        )
    return DemandInferenceSnapshot(
        manifest=manifest,
        inference_input=inference_input,
        rental_model_input=rental_input,
        return_model_input=return_input,
        rental_support_sta_ids=rental_support,
        return_support_sta_ids=return_support,
        expected_sta_ids=expected_ids,
        predictions=predictions,
    )


def _model_snapshot_from_payload(
    object_store: ImmutableObjectStore,
    reference: ModelManifestRef,
    payload: bytes,
) -> tuple[ModelSnapshotManifest, InputArtifact, tuple[str, ...]]:
    """Model manifest actual bytes와 support ID set actual bytes를 검증한다."""
    manifest = parse_model_snapshot_manifest(payload)
    validate_model_manifest_binding(reference, manifest)
    input_artifact = model_manifest_input_artifact(manifest, reference.uri)
    if input_artifact.byte_sha256 != reference.byte_sha256:
        raise ContractViolation(
            "model manifest actual bytes가 inference reference와 다릅니다."
        )
    support_ids = _read_id_set_artifact(
        object_store,
        manifest.support_sta_ids,
        f"{manifest.model_kind.value} model support",
    )
    return manifest, input_artifact, support_ids


def _read_id_set_artifact(
    object_store: ImmutableObjectStore,
    reference: IdSetArtifactRef,
    label: str,
) -> tuple[str, ...]:
    """Content-addressed ID set actual bytes를 ref schema·count·SHA와 결합한다."""
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
        raise ContractViolation(f"{label} ID set actual bytes가 reference와 다릅니다.")
    values = _station_id_set(id_set.ids, label)
    if values != id_set.ids:
        raise ContractViolation(f"{label} ID set이 canonical station 순서가 아닙니다.")
    return values


def _projection_from_snapshot(
    snapshot: DemandInferenceSnapshot,
    *,
    active_sta_ids: tuple[str, ...],
) -> DemandProjection:
    """Verified inference와 actual active topology로 complete projection을 만든다."""
    projection = build_demand_projection(
        snapshot.predictions,
        base_dttm=snapshot.manifest.logical_dttm,
        active_station_ids=active_sta_ids,
        rental_model_station_ids=snapshot.rental_support_sta_ids,
        return_model_station_ids=snapshot.return_support_sta_ids,
    )
    if projection.expected_sta_ids != snapshot.expected_sta_ids:
        raise ContractViolation(
            "inference expected ID set이 active·두 model support 교집합과 다릅니다."
        )
    return projection


def _validate_demand_artifact(
    publication: PreparedPublication,
    payloads: Mapping[str, bytes],
    projection: DemandProjection,
) -> None:
    """Gold output actual Parquet 또는 EMPTY가 projection과 정확히 같은지 검증한다."""
    artifacts = publication.manifest.artifacts
    if not projection.records:
        if artifacts:
            raise ContractViolation(
                "EMPTY demand publication에 output artifact가 있습니다."
            )
        return
    if len(artifacts) != 1 or artifacts[0].role != "station_demand_forecast":
        raise ContractViolation(
            "nonempty demand publication에 exact output artifact 하나가 필요합니다."
        )
    actual = demand_records_from_parquet(
        payloads[artifacts[0].uri],
        expected_base_dttm=projection.base_dttm,
        expected_sta_ids=projection.expected_sta_ids,
    )
    if actual != projection.records:
        raise ContractViolation(
            "demand output Parquet이 inference projection과 다릅니다."
        )


def _load_active_station_ids(connection: Connection[Any]) -> tuple[str, ...]:
    """Transaction 밖의 짧은 read로 현재 active Gold station ID를 읽는다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "active station loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return _active_station_ids_locked(cursor)


def _active_station_ids_locked(
    cursor: Cursor[tuple[Any, ...]],
) -> tuple[str, ...]:
    """Topology shared lock transaction에서 active station ID를 canonical 순서로 읽는다."""
    cursor.execute(
        """
        SELECT sta_id
          FROM station
         WHERE is_active
         ORDER BY sta_id COLLATE "C"
        """
    )
    values = tuple(row[0] for row in cursor.fetchall())
    canonical = _station_id_set(values, "active station")
    if canonical != values:
        raise ContractViolation(
            "DB active station ID가 canonical UTF-8 순서가 아닙니다."
        )
    return values


def _require_demand_evidence(
    evidence: tuple[VerifiedPublicationEvidence, ...],
) -> VerifiedPublicationEvidence:
    """Callback evidence가 demand publication 정확히 하나인지 검증한다."""
    if (
        len(evidence) != 1
        or evidence[0].manifest.publication_key != "station_demand_forecast"
    ):
        raise ContractViolation("demand publication evidence key가 잘못됐습니다.")
    return evidence[0]


def _reconcile_demand_records(
    cursor: Cursor[tuple[Any, ...]],
    records: tuple[DemandForecastRecord, ...],
) -> None:
    """Temp staging을 거쳐 demand projection 전체를 upsert·delete·readback한다."""
    cursor.execute(
        """
        CREATE TEMP TABLE gold_demand_staging (
            base_dttm TIMESTAMPTZ NOT NULL,
            sta_id TEXT NOT NULL,
            predicted_dttm TIMESTAMPTZ NOT NULL,
            predicted_rent_cnt INTEGER NOT NULL,
            predicted_rtn_cnt INTEGER NOT NULL,
            PRIMARY KEY (sta_id, predicted_dttm)
        ) ON COMMIT DROP
        """
    )
    if records:
        cursor.executemany(
            """
            INSERT INTO gold_demand_staging (
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
        cursor.execute(
            """
            INSERT INTO station_demand_forecast AS current_demand (
                base_dttm,
                sta_id,
                predicted_dttm,
                predicted_rent_cnt,
                predicted_rtn_cnt
            )
            SELECT base_dttm,
                   sta_id,
                   predicted_dttm,
                   predicted_rent_cnt,
                   predicted_rtn_cnt
              FROM gold_demand_staging
             ORDER BY sta_id COLLATE "C", predicted_dttm
            ON CONFLICT (sta_id, predicted_dttm) DO UPDATE
            SET base_dttm = EXCLUDED.base_dttm,
                predicted_rent_cnt = EXCLUDED.predicted_rent_cnt,
                predicted_rtn_cnt = EXCLUDED.predicted_rtn_cnt
            WHERE ROW(
                current_demand.base_dttm,
                current_demand.predicted_rent_cnt,
                current_demand.predicted_rtn_cnt
            ) IS DISTINCT FROM ROW(
                EXCLUDED.base_dttm,
                EXCLUDED.predicted_rent_cnt,
                EXCLUDED.predicted_rtn_cnt
            )
            """
        )
    cursor.execute(
        """
        DELETE FROM station_demand_forecast AS current_demand
         WHERE NOT EXISTS (
                   SELECT 1
                     FROM gold_demand_staging AS staging
                    WHERE staging.sta_id = current_demand.sta_id
                      AND staging.predicted_dttm = current_demand.predicted_dttm
               )
        """
    )
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
    actual = tuple(DemandForecastRecord(*row) for row in cursor.fetchall())
    if actual != records:
        raise ContractViolation(
            "station_demand_forecast full reconcile readback이 staging과 다릅니다."
        )


def _validate_record_snapshot(
    records: tuple[DemandForecastRecord, ...],
    *,
    expected_sta_ids: tuple[str, ...],
    expected_base_dttm: datetime | None = None,
) -> None:
    """output records를 authoritative base·station별 완전 12시간에 결합한다."""
    if type(records) is not tuple or any(
        type(record) is not DemandForecastRecord for record in records
    ):
        raise ContractViolation(
            "demand records는 DemandForecastRecord tuple이어야 합니다."
        )
    expected_ids = _station_id_set(expected_sta_ids, "expected station")
    if expected_sta_ids != expected_ids:
        raise ContractViolation(
            "expected station ID는 중복 없이 UTF-8 순이어야 합니다."
        )
    if not records:
        raise ContractViolation(
            "조건부 EMPTY demand는 Parquet artifact가 아니라 artifacts=[]여야 합니다."
        )
    base = records[0].base_dttm
    if expected_base_dttm is not None:
        base = _utc_dttm(expected_base_dttm, "expected demand base_dttm")
    DemandProjection(base, expected_ids, records)


def _station_id_set(values: tuple[str, ...], name: str) -> tuple[str, ...]:
    """station ID tuple을 검증하고 중복 없는 UTF-8 순 tuple로 반환한다."""
    if type(values) is not tuple:
        raise ContractViolation(f"{name} ID는 tuple이어야 합니다.")
    normalized = tuple(_station_id(value) for value in values)
    if len(normalized) != len(set(normalized)):
        raise ContractViolation(f"{name} ID에 중복이 있습니다.")
    return tuple(sorted(normalized, key=lambda value: value.encode("utf-8")))


def _station_id(value: object) -> str:
    """station ID를 target DDL의 canonical ST-숫자 문자열로 검증한다."""
    if type(value) is not str:
        raise ContractViolation("station ID는 문자열이어야 합니다.")
    normalized = unicodedata.normalize("NFC", value)
    if normalized != value or _STATION_ID.fullmatch(normalized) is None:
        raise ContractViolation("station ID는 canonical ST-숫자 형식이어야 합니다.")
    return normalized


def _utc_dttm(value: object, name: str) -> datetime:
    """offset 포함 Python datetime을 UTC instant로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None:
        raise ContractViolation(f"{name}은 offset 포함 datetime이어야 합니다.")
    try:
        offset = value.utcoffset()
        if offset is None:
            raise ValueError("missing UTC offset")
        return value.astimezone(UTC)
    except (OverflowError, ValueError) as exc:
        raise ContractViolation(
            f"{name}을 유한 UTC instant로 바꿀 수 없습니다."
        ) from exc


def _add_hours(value: datetime, hours: int, name: str) -> datetime:
    """UTC 시각에 hour를 더하고 Python datetime 범위 초과를 계약 오류로 바꾼다."""
    try:
        return value + timedelta(hours=hours)
    except OverflowError as exc:
        raise ContractViolation(f"{name}이 datetime 범위를 벗어났습니다.") from exc


def _prediction_mean(value: object, name: str) -> float:
    """inference mean을 finite·비음수 Python float64로 검증한다."""
    if type(value) is not float or not math.isfinite(value) or value < 0.0:
        raise ContractViolation(f"{name}은 finite·비음수 float64여야 합니다.")
    return value


def _round_prediction(value: float, name: str) -> int:
    """Python ties-to-even으로 수량을 반올림하고 INTEGER 범위를 확인한다."""
    rounded = round(_prediction_mean(value, name))
    if rounded > POSTGRES_INTEGER_MAX:
        raise ContractViolation(f"{name} 반올림 결과가 PostgreSQL INTEGER를 넘습니다.")
    return rounded


def _postgres_nonnegative_integer(value: object, name: str) -> int:
    """값을 target의 비음수 PostgreSQL INTEGER 범위로 검증한다."""
    if type(value) is not int or value < 0 or value > POSTGRES_INTEGER_MAX:
        raise ContractViolation(f"{name}은 비음수 PostgreSQL INTEGER여야 합니다.")
    return value
