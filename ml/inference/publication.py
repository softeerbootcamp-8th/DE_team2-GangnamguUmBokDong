"""Pinned serving release로 canonical inference authority를 게시한다.

Mutable champion pointer나 ``predictions/...`` overwrite는 이 모듈의 authority가
아니다. Serving-release pointer를 run 시작에 정확히 한 번 읽고, 계산이 실제로 읽은
non-model S3 bytes와 7-column 결과를 content-addressed object로 고정한 뒤 immutable
revision catalog를 claim하고 success/EMPTY manifest를 마지막에 기록한다.
"""

from __future__ import annotations

import re
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum

import pandas as pd
import pyarrow as pa
from core import s3 as s3_io
from core.gold_publication import (
    Dependency,
    IdSet,
    ImmutableObjectStore,
    ObjectMissingError,
    S3ImmutableObjectStore,
    build_id_set,
    sha256_hex,
)
from core.inference_catalog import (
    INFERENCE_REVISION_RECORD_SCHEMA_VERSION,
    InMemoryInferenceRevisionCatalog,
    InferenceCatalogError,
    InferenceCatalogSnapshot,
    InferenceRevisionCatalog,
    InferenceRevisionConflictError,
    InferenceRevisionRecord,
    S3InferenceRevisionCatalog,
    split_inference_object_base_uri,
)
from core.inference_snapshot import (
    INFERENCE_HORIZON_COUNT,
    ImmutableInputRef,
    InferenceSnapshotCounts,
    InferenceSnapshotManifest,
    InferenceSnapshotStatus,
    ModelManifestRef,
    ParquetOutputRef,
    ServingPlanRef,
    ServingReleaseRef,
    build_inference_snapshot_manifest,
    build_model_manifest_ref,
    canonicalize_inference_output_table,
    parse_inference_output_parquet,
    parse_inference_snapshot_manifest,
    serialize_inference_output_parquet,
)
from core.model_snapshot import (
    IdSetArtifactRef,
    build_id_set_artifact_ref,
    parse_station_categories,
)
from ml_core.scoring import build_pinned_scoring_model, use_pinned_scoring_models
from ml_core.serving_contract import SERVING_FEATURE_PROFILE_KEYS
from ml_core.serving_release import (
    PinnedServingRelease,
    ServingReleasePointerStore,
    VerifiedStationProfile,
    load_current_serving_release,
    parse_effective_serving_contract,
)

from . import config
from .predict_single import (
    authority_inference_run,
    predict_demand_multi_hour_all_stations,
)

INFERENCE_PRODUCER_VERSION = "gold-inference-producer-v1"
"""Inference manifest에 기록하는 producer implementation version이다."""

InferencePublicationError = InferenceCatalogError
"""기존 producer import와 호환되는 inference publication 오류 경계다."""


class InferenceRunStatus(StrEnum):
    """Caller가 신규 게시와 exact replay를 구분할 수 있는 결과 상태다."""

    PUBLISHED = "published"
    REPLAYED = "replayed"


@dataclass(frozen=True, slots=True)
class PublishedInferenceSnapshot:
    """Manifest-last readback까지 완료한 inference authority 결과다."""

    manifest: InferenceSnapshotManifest
    manifest_uri: str
    manifest_sha256: str
    status: InferenceRunStatus

    def __post_init__(self) -> None:
        """Returned identity가 manifest exact bytes와 일치하는지 검증한다."""
        if self.manifest.sha256 != self.manifest_sha256:
            raise InferencePublicationError(
                "returned manifest SHA가 실제 manifest와 다릅니다."
            )


def run_and_publish_inference(
    *,
    logical_dttm: datetime,
    station_dependency: Dependency,
    serving_plan: ServingPlanRef,
    expected_sta_ids: IdSet,
    object_base_uri: str,
    producer_version: str = INFERENCE_PRODUCER_VERSION,
    object_store: ImmutableObjectStore | None = None,
    pointer_store: ServingReleasePointerStore | None = None,
    pointer_key: str | None = None,
    revision_catalog: InferenceRevisionCatalog | None = None,
) -> PublishedInferenceSnapshot:
    """Pinned model 쌍으로 station×12를 계산하고 immutable authority를 공개한다.

    ``expected_sta_ids``는 caller가 prepared incoming active station과 pinned
    rental/return support의 교집합으로 결정한다. Producer는 caller 집합이 두 model
    support 안에 있는지 다시 검증하고 그 exact station×12만 계산한다. Partial이나
    failed station은 output/manifest를 쓰기 전에 실패한다. Model/release/profile
    bytes는 explicit manifest field라 generic ``inputs``에 중복하지 않고, 계산
    구간에서 ``core.s3``가 실제 반환한 non-model bytes만 stable
    ``s3_input_<key-sha256>`` role로 고정한다.
    """
    logical = _utc_dttm(logical_dttm)
    if type(station_dependency) is not Dependency:
        raise TypeError("station_dependency는 exact Dependency여야 합니다.")
    if station_dependency.publication_key != "station":
        raise InferencePublicationError(
            "station_dependency publication_key는 station이어야 합니다."
        )
    if station_dependency.logical_dttm > logical:
        raise InferencePublicationError(
            "station_dependency는 inference logical_dttm보다 미래일 수 없습니다."
        )
    if type(serving_plan) is not ServingPlanRef:
        raise TypeError("serving_plan은 exact ServingPlanRef여야 합니다.")
    if type(expected_sta_ids) is not IdSet:
        raise TypeError("expected_sta_ids는 exact IdSet이어야 합니다.")
    if type(producer_version) is not str or not producer_version:
        raise InferencePublicationError(
            "producer_version은 non-empty string이어야 합니다."
        )
    _split_base_uri(object_base_uri)
    if object_store is not None and revision_catalog is None:
        raise InferencePublicationError(
            "custom object_store에는 같은 backend의 revision_catalog를 명시해야 합니다."
        )

    default_client = None
    if object_store is None:
        default_client = s3_io._client()
        immutable = S3ImmutableObjectStore(default_client)
    else:
        immutable = object_store
    if revision_catalog is not None:
        catalog = revision_catalog
    else:
        bucket, _prefix = _split_base_uri(object_base_uri)
        if default_client is None:
            raise InferencePublicationError(
                "custom object_store에는 같은 backend의 revision_catalog를 명시해야 합니다."
            )
        catalog = S3InferenceRevisionCatalog(
            default_client,
            immutable,
            bucket=bucket,
            object_base_uri=object_base_uri,
        )
    initial_catalog = catalog.snapshot(logical)

    # 이 함수 안에서 유일한 serving pointer read다. Returned preflight가 exact
    # transitive payload를 보존하므로 이후 legacy champion pointer나 model key를
    # 다시 읽지 않는다.
    pinned = load_current_serving_release(
        object_store=immutable,
        pointer_store=pointer_store,
        pointer_key=pointer_key,
    )
    _validate_runtime_contract(pinned.preflight.effective_contract_payload)
    if logical.second != 0 or logical.microsecond != 0:
        raise InferencePublicationError("logical_dttm은 exact minute 경계여야 합니다.")
    serving_tick = config.SERVING_TICK_MINUTES
    kst_minute = pd.Timestamp(logical).tz_convert("Asia/Seoul").minute
    if kst_minute % serving_tick != 0:
        raise InferencePublicationError(
            f"logical_dttm은 {serving_tick}분 serving tick 경계여야 합니다."
        )
    support = _validate_expected_support(pinned, expected_sta_ids)
    required_station_nos = _required_station_nos(pinned)
    _validate_station_profile(
        pinned.preflight.station_profile,
        required_station_nos=required_station_nos,
    )
    pinned_models = {
        "rental": build_pinned_scoring_model(
            _artifact_payload_map(pinned.preflight.rental_snapshot.artifacts)
        ),
        "return": build_pinned_scoring_model(
            _artifact_payload_map(pinned.preflight.return_snapshot.artifacts)
        ),
    }

    expected_ref = _publish_expected_ids(
        support,
        object_base_uri=object_base_uri,
        object_store=immutable,
    )
    inputs: tuple[ImmutableInputRef, ...] = ()
    output_ref: ParquetOutputRef | None = None
    if expected_sta_ids.ids:
        kst_logical = pd.Timestamp(logical).tz_convert("Asia/Seoul")
        with (
            authority_inference_run(pinned.preflight.station_profile),
            use_pinned_scoring_models(pinned_models),
            s3_io.capture_object_reads() as read_capture,
        ):
            outcome = predict_demand_multi_hour_all_stations(
                date=kst_logical.strftime("%Y-%m-%d"),
                hour=int(kst_logical.hour),
                minute=int(kst_logical.minute),
                station_ids=list(expected_sta_ids.ids),
                n_hours=INFERENCE_HORIZON_COUNT,
            )
        table = _complete_output_table(outcome, logical, expected_sta_ids)
        inputs = _publish_captured_inputs(
            read_capture.objects,
            object_base_uri=object_base_uri,
            object_store=immutable,
        )
        if not inputs:
            raise InferencePublicationError(
                "SUCCEEDED inference 계산이 실제로 읽은 non-model S3 input이 없습니다."
            )
        output_ref = _publish_output(
            table,
            logical_dttm=logical,
            expected_sta_ids=expected_sta_ids,
            object_base_uri=object_base_uri,
            object_store=immutable,
        )
        status = InferenceSnapshotStatus.SUCCEEDED
        station_count = len(expected_sta_ids.ids)
    else:
        status = InferenceSnapshotStatus.EMPTY
        station_count = 0

    counts = InferenceSnapshotCounts(
        expected_station_count=station_count,
        actual_station_count=station_count,
        failed_station_count=0,
        expected_row_count=station_count * INFERENCE_HORIZON_COUNT,
        actual_row_count=station_count * INFERENCE_HORIZON_COUNT,
        failed_row_count=0,
    )
    return _publish_revisioned_manifest(
        logical_dttm=logical,
        producer_version=producer_version,
        pinned=pinned,
        serving_plan=serving_plan,
        station_dependency=station_dependency,
        inputs=inputs,
        expected_ref=expected_ref,
        counts=counts,
        status=status,
        output_ref=output_ref,
        object_base_uri=object_base_uri,
        object_store=immutable,
        catalog=catalog,
        initial_catalog=initial_catalog,
    )


def _validate_expected_support(pinned: PinnedServingRelease, expected: IdSet) -> IdSet:
    """Caller expected가 pinned rental/return support 교집합의 subset인지 검증한다."""
    rental = pinned.preflight.rental_snapshot.support_sta_ids
    returned = pinned.preflight.return_snapshot.support_sta_ids
    support = build_id_set(set(rental.ids).intersection(returned.ids))
    unsupported = tuple(sorted(set(expected.ids).difference(support.ids)))
    if unsupported:
        raise InferencePublicationError(
            "expected station IDs에 pinned rental/return support 밖의 ID가 있습니다: "
            f"{unsupported[:10]}"
        )
    return expected


def _validate_runtime_contract(payload: bytes) -> None:
    """Pinned 7-key effective contract와 현재 inference runtime을 exact 비교한다."""
    pinned_contract = parse_effective_serving_contract(payload)
    try:
        runtime_contract = {
            key: getattr(config, key) for key in SERVING_FEATURE_PROFILE_KEYS
        }
    except AttributeError as exc:
        raise InferencePublicationError(
            "inference runtime에 pinned serving contract key가 없습니다."
        ) from exc
    if runtime_contract != pinned_contract:
        mismatches = {
            key: (pinned_contract[key], runtime_contract[key])
            for key in SERVING_FEATURE_PROFILE_KEYS
            if pinned_contract[key] != runtime_contract[key]
        }
        raise InferencePublicationError(
            f"pinned serving contract와 runtime config가 다릅니다: {mismatches}"
        )


def _required_station_nos(pinned: PinnedServingRelease) -> set[int]:
    """두 pinned model category bytes에서 profile이 반드시 지원할 station_no를 얻는다."""
    rental = parse_station_categories(
        pinned.preflight.rental_snapshot.artifact_payload("station_categories")
    )
    returned = parse_station_categories(
        pinned.preflight.return_snapshot.artifact_payload("station_categories")
    )
    return set(rental).union(returned)


def _validate_station_profile(
    profile: VerifiedStationProfile, *, required_station_nos: set[int]
) -> None:
    """Release preflight가 검증한 profile을 runtime grid/model coverage에 결합한다."""
    if type(profile) is not VerifiedStationProfile:
        raise TypeError("station profile은 exact VerifiedStationProfile이어야 합니다.")
    if profile.grid_tick_minutes != config.GRID_TICK_MINUTES:
        raise InferencePublicationError(
            "station profile grid와 runtime model grid가 다릅니다: "
            f"profile={profile.grid_tick_minutes}, runtime={config.GRID_TICK_MINUTES}"
        )
    expected_minutes = tuple(range(0, 1440, config.GRID_TICK_MINUTES))
    if profile.minute_values != expected_minutes:
        raise InferencePublicationError(
            "station profile minute set이 runtime model grid와 다릅니다."
        )
    missing = sorted(required_station_nos.difference(profile.station_nos))
    if missing:
        raise InferencePublicationError(
            f"station profile이 model support station_no를 누락했습니다: {missing[:10]}"
        )


def _artifact_payload_map(artifacts: Iterable[object]) -> dict[str, bytes]:
    """VerifiedModelArtifact tuple을 role→exact payload mapping으로 바꾼다."""
    result: dict[str, bytes] = {}
    for artifact in artifacts:
        reference = getattr(artifact, "reference", None)
        payload = getattr(artifact, "payload", None)
        role = getattr(reference, "role", None)
        if type(role) is not str or type(payload) is not bytes or role in result:
            raise InferencePublicationError(
                "verified model artifact tuple이 잘못됐습니다."
            )
        result[role] = payload
    return result


def _publish_expected_ids(
    id_set: IdSet,
    *,
    object_base_uri: str,
    object_store: ImmutableObjectStore,
) -> IdSetArtifactRef:
    """Expected ID set canonical bytes를 put-once/readback하고 ref를 만든다."""
    uri = _content_uri(
        object_base_uri, "inference/expected-sta-ids", id_set.sha256, "json"
    )
    _put_once_and_readback(
        object_store, uri, id_set.canonical_bytes, require_canonical_json=True
    )
    return build_id_set_artifact_ref(id_set, uri)


def _complete_output_table(
    outcome: object,
    logical_dttm: datetime,
    expected_sta_ids: IdSet,
) -> pa.Table:
    """Legacy batch result를 exact complete 7-column authority table로 바꾼다."""
    if type(outcome) is not dict:
        raise InferencePublicationError("inference predictor 결과는 dict여야 합니다.")
    failed = outcome.get("failed")
    expected_count = len(expected_sta_ids.ids) * INFERENCE_HORIZON_COUNT
    if failed != []:
        raise InferencePublicationError(
            "partial/failed inference는 authority가 될 수 없습니다."
        )
    if (
        outcome.get("expected_count") != expected_count
        or outcome.get("actual_count") != expected_count
    ):
        raise InferencePublicationError(
            "inference expected/actual row count가 완전하지 않습니다."
        )
    results = outcome.get("results")
    if type(results) is not list:
        raise InferencePublicationError("inference results는 list여야 합니다.")
    rows: list[dict[str, object]] = []
    try:
        for result in results:
            rows.append(
                {
                    "station_id": result["station_id"],
                    "date": result["date"],
                    "hour": result["hour"],
                    "minute": result["minute"],
                    "horizon": result["horizon"],
                    "rental_pred_mean": result["rental"]["pred_mean"],
                    "return_pred_mean": result["return"]["pred_mean"],
                }
            )
    except (KeyError, TypeError) as exc:
        raise InferencePublicationError(
            "inference result row schema가 잘못됐습니다."
        ) from exc
    return canonicalize_inference_output_table(
        pd.DataFrame(rows),
        logical_dttm=logical_dttm,
        expected_sta_ids=expected_sta_ids,
    )


def _publish_captured_inputs(
    captured: Iterable[s3_io.CapturedS3Object],
    *,
    object_base_uri: str,
    object_store: ImmutableObjectStore,
) -> tuple[ImmutableInputRef, ...]:
    """실제 GET bytes를 source-key 기반 stable role의 immutable copy로 고정한다."""
    refs: list[ImmutableInputRef] = []
    for item in captured:
        key_digest = sha256_hex(item.key.encode("utf-8"))
        payload_digest = sha256_hex(item.payload)
        extension = _source_extension(item.key)
        uri = _content_uri(
            object_base_uri,
            f"inference/inputs/source-key-sha256={key_digest}",
            payload_digest,
            extension,
        )
        _put_once_and_readback(object_store, uri, item.payload)
        refs.append(
            ImmutableInputRef(
                byte_sha256=payload_digest,
                role=f"s3_input_{key_digest}",
                uri=uri,
            )
        )
    return tuple(sorted(refs, key=lambda ref: (ref.role.encode(), ref.uri.encode())))


def _publish_output(
    table: pa.Table,
    *,
    logical_dttm: datetime,
    expected_sta_ids: IdSet,
    object_base_uri: str,
    object_store: ImmutableObjectStore,
) -> ParquetOutputRef:
    """Canonical output Parquet을 content-addressed put-once/readback한다."""
    payload = serialize_inference_output_parquet(table)
    digest = sha256_hex(payload)
    uri = _content_uri(object_base_uri, "inference/outputs", digest, "parquet")
    _put_once_and_readback(object_store, uri, payload)
    readback = object_store.read_bytes(uri, digest)
    parsed = parse_inference_output_parquet(
        readback,
        logical_dttm=logical_dttm,
        expected_sta_ids=expected_sta_ids,
    )
    if not parsed.equals(table, check_metadata=True):
        raise InferencePublicationError(
            "inference output readback table이 입력과 다릅니다."
        )
    return ParquetOutputRef(byte_sha256=digest, row_count=table.num_rows, uri=uri)


def _publish_revisioned_manifest(
    *,
    logical_dttm: datetime,
    producer_version: str,
    pinned: PinnedServingRelease,
    serving_plan: ServingPlanRef,
    station_dependency: Dependency,
    inputs: tuple[ImmutableInputRef, ...],
    expected_ref: IdSetArtifactRef,
    counts: InferenceSnapshotCounts,
    status: InferenceSnapshotStatus,
    output_ref: ParquetOutputRef | None,
    object_base_uri: str,
    object_store: ImmutableObjectStore,
    catalog: InferenceRevisionCatalog,
    initial_catalog: InferenceCatalogSnapshot,
) -> PublishedInferenceSnapshot:
    """Exact replay 또는 next revision을 정하고 manifest-last로 공개한다."""
    serving_ref, rental_ref, return_ref = _pinned_manifest_refs(pinned)
    latest = initial_catalog.records[-1] if initial_catalog.records else None
    revision_no = 0 if latest is None else latest.revision_no
    same_revision_candidate = build_inference_snapshot_manifest(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=status,
        producer_version=producer_version,
        serving_release=serving_ref,
        serving_plan=serving_plan,
        rental_model_manifest=rental_ref,
        return_model_manifest=return_ref,
        station_dependency=station_dependency,
        inputs=inputs,
        expected_sta_ids=expected_ref,
        counts=counts,
        horizon_count=INFERENCE_HORIZON_COUNT,
        output=output_ref,
    )
    same_uri = _content_uri(
        object_base_uri,
        "inference/manifests",
        same_revision_candidate.sha256,
        "json",
    )
    if latest is not None and (
        latest.manifest_byte_sha256 == same_revision_candidate.sha256
        and latest.manifest_uri == same_uri
    ):
        if catalog.snapshot(logical_dttm).records != initial_catalog.records:
            raise InferenceRevisionConflictError(
                "같은 logical의 inference catalog가 변경되어 replay를 중단합니다."
            )
        try:
            _readback_manifest(object_store, latest, same_revision_candidate)
        except ObjectMissingError:
            # Catalog reservation은 manifest보다 먼저 쓰므로 process crash 뒤 exact
            # retry만 같은 bytes의 manifest-last 단계를 복구할 수 있다.
            _write_manifest_last(object_store, same_uri, same_revision_candidate)
            return PublishedInferenceSnapshot(
                manifest=same_revision_candidate,
                manifest_uri=same_uri,
                manifest_sha256=same_revision_candidate.sha256,
                status=InferenceRunStatus.PUBLISHED,
            )
        return PublishedInferenceSnapshot(
            manifest=same_revision_candidate,
            manifest_uri=same_uri,
            manifest_sha256=same_revision_candidate.sha256,
            status=InferenceRunStatus.REPLAYED,
        )

    if latest is not None:
        # 바뀐 계산은 기존 latest authority가 완전한 경우에만 다음 revision이 된다.
        payload = object_store.read_bytes(
            latest.manifest_uri,
            latest.manifest_byte_sha256,
            require_canonical_json=True,
        )
        parsed = parse_inference_snapshot_manifest(payload)
        if (
            parsed.revision_no != latest.revision_no
            or parsed.logical_dttm != logical_dttm
        ):
            raise InferencePublicationError(
                "catalog latest record와 manifest가 다릅니다."
            )
        revision_no = latest.revision_no + 1

    candidate = build_inference_snapshot_manifest(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        status=status,
        producer_version=producer_version,
        serving_release=serving_ref,
        serving_plan=serving_plan,
        rental_model_manifest=rental_ref,
        return_model_manifest=return_ref,
        station_dependency=station_dependency,
        inputs=inputs,
        expected_sta_ids=expected_ref,
        counts=counts,
        horizon_count=INFERENCE_HORIZON_COUNT,
        output=output_ref,
    )
    manifest_uri = _content_uri(
        object_base_uri,
        "inference/manifests",
        candidate.sha256,
        "json",
    )
    current_catalog = catalog.snapshot(logical_dttm)
    if current_catalog.records != initial_catalog.records:
        raise InferenceRevisionConflictError(
            "같은 logical의 inference catalog가 변경되어 writer를 중단합니다."
        )
    record = InferenceRevisionRecord(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        manifest_byte_sha256=candidate.sha256,
        manifest_uri=manifest_uri,
    )
    catalog.claim(record)
    _write_manifest_last(object_store, manifest_uri, candidate)
    return PublishedInferenceSnapshot(
        manifest=candidate,
        manifest_uri=manifest_uri,
        manifest_sha256=candidate.sha256,
        status=InferenceRunStatus.PUBLISHED,
    )


def _pinned_manifest_refs(
    pinned: PinnedServingRelease,
) -> tuple[ServingReleaseRef, ModelManifestRef, ModelManifestRef]:
    """Pinned ml_core release를 core inference manifest의 explicit refs로 바꾼다."""
    release = pinned.manifest
    serving_ref = ServingReleaseRef(
        byte_sha256=pinned.pointer.release_manifest_byte_sha256,
        effective_contract_version=release.effective_contract.version,
        release_version=release.release_version,
        uri=pinned.pointer.release_manifest_uri,
    )
    rental_ref = build_model_manifest_ref(
        pinned.preflight.rental_snapshot.manifest,
        release.rental_model_manifest.uri,
    )
    return_ref = build_model_manifest_ref(
        pinned.preflight.return_snapshot.manifest,
        release.return_model_manifest.uri,
    )
    return serving_ref, rental_ref, return_ref


def _write_manifest_last(
    object_store: ImmutableObjectStore,
    uri: str,
    manifest: InferenceSnapshotManifest,
) -> None:
    """Authority manifest를 마지막 immutable write로 기록하고 exact parse/readback한다."""
    object_store.put_once(
        uri,
        manifest.canonical_bytes,
        expected_sha256=manifest.sha256,
        require_canonical_json=True,
    )
    record = InferenceRevisionRecord(
        logical_dttm=manifest.logical_dttm,
        revision_no=manifest.revision_no,
        manifest_byte_sha256=manifest.sha256,
        manifest_uri=uri,
    )
    _readback_manifest(object_store, record, manifest)


def _readback_manifest(
    object_store: ImmutableObjectStore,
    record: InferenceRevisionRecord,
    expected: InferenceSnapshotManifest,
) -> None:
    """Catalog identity의 canonical manifest가 expected typed 값과 같은지 확인한다."""
    payload = object_store.read_bytes(
        record.manifest_uri,
        record.manifest_byte_sha256,
        require_canonical_json=True,
    )
    if parse_inference_snapshot_manifest(payload) != expected:
        raise InferencePublicationError(
            "inference manifest readback이 입력과 다릅니다."
        )


def _put_once_and_readback(
    object_store: ImmutableObjectStore,
    uri: str,
    payload: bytes,
    *,
    require_canonical_json: bool = False,
) -> None:
    """Actual bytes를 put-once하고 같은 SHA로 즉시 readback한다."""
    digest = sha256_hex(payload)
    object_store.put_once(
        uri,
        payload,
        expected_sha256=digest,
        require_canonical_json=require_canonical_json,
    )
    readback = object_store.read_bytes(
        uri,
        digest,
        require_canonical_json=require_canonical_json,
    )
    if readback != payload:
        raise InferencePublicationError(
            f"immutable object readback bytes가 다릅니다: {uri}"
        )


def _content_uri(base_uri: str, namespace: str, digest: str, extension: str) -> str:
    """Object base 아래 content-addressed exact S3 URI를 만든다."""
    bucket, prefix = _split_base_uri(base_uri)
    base = f"{prefix}/" if prefix else ""
    return f"s3://{bucket}/{base}{namespace}/sha256={digest}.{extension}"


def _split_base_uri(uri: str) -> tuple[str, str]:
    """Prefix를 가리키는 query/fragment 없는 S3 base URI를 검증한다."""
    return split_inference_object_base_uri(uri)


def _source_extension(key: str) -> str:
    """Source key suffix를 안전한 immutable-copy extension으로 정규화한다."""
    suffix = key.rsplit("/", 1)[-1].rsplit(".", 1)
    if len(suffix) == 2 and re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]*", suffix[1]):
        return suffix[1].lower()
    return "bin"


def _utc_dttm(value: datetime) -> datetime:
    """Timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise InferencePublicationError(
            "logical_dttm은 timezone-aware datetime이어야 합니다."
        )
    return value.astimezone(UTC)
