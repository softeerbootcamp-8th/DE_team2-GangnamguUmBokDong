"""Immutable bytes와 business time을 결합한 publication 검증 증거를 만든다."""

from __future__ import annotations

import unicodedata
import weakref
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

from .canonical import parse_canonical_json, sha256_hex
from .contract import (
    InputArtifact,
    InputFingerprint,
    PublicationManifest,
    get_publication_spec,
    parse_input_fingerprint,
    parse_publication_manifest,
    validate_input_fingerprint,
    validate_linked_dependency_manifests,
    validate_publication_manifest,
    validate_route_urgency_dependencies,
)
from .documents import (
    parse_route_coverage,
    parse_station_realtime_window_set,
    parse_station_relocation_approval,
)
from .errors import (
    CanonicalParseError,
    ContractViolation,
    ObjectChecksumMismatchError,
    ObjectNotCanonicalError,
    PublicationTimeError,
)
from .storage import ImmutableObjectStore

_REQUIRED_BUSINESS_TIME_FIELDS: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "weather_grid": (),
        "dispatch_center": (),
        "station": ("last_seen_dttm", "master_base_dttm"),
        "station_stock": ("base_dttm",),
        "station_demand_forecast": ("base_dttm",),
        "weather_forecast": ("base_dttm",),
        "event:cultural_event": ("last_seen_dttm",),
        "event:performance_event": ("last_seen_dttm",),
        "station_urgency": ("base_dttm",),
        "rebalance_route": ("proposed_dttm",),
    }
)

_LINKED_MANIFEST_ROLES: Mapping[str, tuple[str, ...]] = MappingProxyType(
    {
        "station_urgency": (
            "demand_publication_manifest",
            "stock_publication_manifest",
        ),
        "rebalance_route": ("urgency_publication_manifest",),
    }
)


@dataclass(frozen=True, slots=True)
class PreparedPublication:
    """Immutable evidence 검증 전 publication 문서 결합을 표현한다."""

    manifest: PublicationManifest
    manifest_uri: str
    input_fingerprint: InputFingerprint

    def __post_init__(self) -> None:
        """manifest와 fingerprint의 형식 및 canonical hash 결합을 검증한다."""
        if type(self.manifest) is not PublicationManifest:
            raise ContractViolation("manifest는 PublicationManifest여야 합니다.")
        if type(self.input_fingerprint) is not InputFingerprint:
            raise ContractViolation("input_fingerprint는 InputFingerprint여야 합니다.")
        _require_nfc_nonblank(self.manifest_uri, "publication manifest URI")

        validate_input_fingerprint(
            self.manifest.publication_key,
            self.input_fingerprint,
        )
        validate_publication_manifest(self.manifest)
        if self.manifest.input_fingerprint_sha256 != self.input_fingerprint.sha256:
            raise ContractViolation(
                "manifest input_fingerprint_sha256이 실제 canonical fingerprint와 다릅니다."
            )


@dataclass(frozen=True, slots=True)
class BusinessTimeEvidence:
    """한 publication projection의 bounded business time을 field별로 고정한다."""

    publication_key: str
    published_row_cnt: int
    values_by_field: Mapping[str, tuple[datetime, ...]]

    def __post_init__(self) -> None:
        """key별 exact field와 행별 시각 개수·timezone을 검증하고 동결한다."""
        get_publication_spec(self.publication_key)
        if type(self.published_row_cnt) is not int or self.published_row_cnt < 0:
            raise ContractViolation(
                "business time published_row_cnt는 0 이상 integer여야 합니다."
            )
        if not isinstance(self.values_by_field, Mapping):
            raise ContractViolation(
                "business time evidence는 field mapping이어야 합니다."
            )

        expected_fields = set(required_business_time_fields(self.publication_key))
        actual_fields = set(self.values_by_field)
        if actual_fields != expected_fields:
            raise ContractViolation(
                f"{self.publication_key} business time field가 정확하지 않습니다: "
                f"expected={sorted(expected_fields)}, actual={sorted(actual_fields)}"
            )

        frozen: dict[str, tuple[datetime, ...]] = {}
        for name in sorted(actual_fields):
            _require_nfc_nonblank(name, "business time field")
            values = self.values_by_field[name]
            if type(values) is not tuple:
                raise ContractViolation(
                    f"business time field {name} 값은 행별 tuple이어야 합니다."
                )
            if len(values) != self.published_row_cnt:
                raise ContractViolation(
                    f"{self.publication_key} business time field {name}의 값 개수가 "
                    f"published_row_cnt와 다릅니다: expected={self.published_row_cnt}, "
                    f"actual={len(values)}"
                )
            frozen[name] = tuple(_utc_dttm(value, name) for value in values)
        object.__setattr__(self, "values_by_field", MappingProxyType(frozen))

    @property
    def all_values(self) -> tuple[tuple[str, datetime], ...]:
        """field명과 행별 business time을 결정적 field 순서로 반환한다."""
        return tuple(
            (name, value)
            for name in sorted(self.values_by_field)
            for value in self.values_by_field[name]
        )


@dataclass(frozen=True, slots=True, weakref_slot=True, init=False)
class VerifiedPublicationEvidence:
    """공통 verifier만 만들 수 있는 immutable publication 검증 token이다."""

    publication: PreparedPublication
    business_times: BusinessTimeEvidence

    def __init__(self, *_args: object, **_kwargs: object) -> None:
        """외부 생성과 ``dataclasses.replace`` 기반 token 복제를 거부한다."""
        raise ContractViolation(
            "공통 verifier가 만들지 않은 publication evidence입니다."
        )

    @property
    def manifest(self) -> PublicationManifest:
        """검증된 publication manifest를 반환한다."""
        return self.publication.manifest

    @property
    def manifest_uri(self) -> str:
        """검증하고 마지막에 기록한 publication manifest URI를 반환한다."""
        return self.publication.manifest_uri

    @property
    def input_fingerprint(self) -> InputFingerprint:
        """검증된 input fingerprint를 반환한다."""
        return self.publication.input_fingerprint


StagingEvidenceValidator = Callable[
    [PreparedPublication, Mapping[str, bytes]],
    Mapping[str, Iterable[datetime]],
]


@dataclass(frozen=True, slots=True)
class _ObjectExpectation:
    """URI 하나에 결합된 checksum·canonical·exact bytes 기대값을 보관한다."""

    sha256: str
    require_canonical_json: bool = False
    exact_payload: bytes | None = None


def required_business_time_fields(publication_key: str) -> tuple[str, ...]:
    """publication key가 행별로 증명해야 하는 bounded time field를 반환한다."""
    get_publication_spec(publication_key)
    return _REQUIRED_BUSINESS_TIME_FIELDS[publication_key]


def _verify_publication_evidence(
    publication: PreparedPublication,
    object_store: ImmutableObjectStore,
    validate_staging: StagingEvidenceValidator,
) -> tuple[PreparedPublication, BusinessTimeEvidence]:
    """실제 immutable bytes와 staging 시각을 검증하고 sealed evidence를 만든다.

    모든 input·output·fingerprint를 먼저 검증하고 table별 staging validator까지 성공한 뒤
    publication manifest를 ``put_once``로 마지막에 기록하고 다시 읽는다.
    """
    if type(publication) is not PreparedPublication:
        raise ContractViolation("publication은 PreparedPublication이어야 합니다.")
    if not callable(getattr(object_store, "read_bytes", None)) or not callable(
        getattr(object_store, "put_once", None)
    ):
        raise ContractViolation("immutable object store가 필요합니다.")
    if not callable(validate_staging):
        raise ContractViolation("key별 staging evidence validator가 필요합니다.")

    # frozen dataclass도 ``object.__setattr__``로 변조할 수 있으므로 immutable
    # object를 읽기 직전에 typed 계약과 hash 결합을 다시 검증한다.
    publication = PreparedPublication(
        manifest=publication.manifest,
        manifest_uri=publication.manifest_uri,
        input_fingerprint=publication.input_fingerprint,
    )

    expectations: dict[str, _ObjectExpectation] = {}
    fingerprint_payload = publication.input_fingerprint.canonical_bytes
    _add_expectation(
        expectations,
        publication.manifest.input_fingerprint_uri,
        _ObjectExpectation(
            publication.manifest.input_fingerprint_sha256,
            require_canonical_json=True,
            exact_payload=fingerprint_payload,
        ),
    )
    for artifact in publication.input_fingerprint.input_artifacts:
        _add_expectation(
            expectations,
            artifact.uri,
            _ObjectExpectation(artifact.byte_sha256),
        )
    for artifact in publication.manifest.artifacts:
        _add_expectation(
            expectations,
            artifact.uri,
            _ObjectExpectation(artifact.byte_sha256),
        )
    if publication.manifest_uri in expectations:
        raise ContractViolation(
            "publication manifest URI는 fingerprint와 input/output artifact URI와 달라야 합니다."
        )

    payload_by_uri = {
        uri: _read_verified_object(object_store, uri, expectation)
        for uri, expectation in expectations.items()
    }
    linked_roles = _LINKED_MANIFEST_ROLES.get(
        publication.manifest.publication_key,
        (),
    )
    input_by_role = {
        artifact.role: artifact
        for artifact in publication.input_fingerprint.input_artifacts
    }
    linked_payloads = {
        role: payload_by_uri[input_by_role[role].uri] for role in linked_roles
    }
    parsed_linked = validate_linked_dependency_manifests(
        publication.manifest.publication_key,
        publication.input_fingerprint,
        linked_payloads,
    )
    _validate_special_input_documents(
        publication.manifest.publication_key,
        input_by_role,
        payload_by_uri,
    )

    if publication.manifest.publication_key == "rebalance_route":
        urgency_manifest = parsed_linked["urgency_publication_manifest"]
        nested_uri = urgency_manifest.input_fingerprint_uri
        if nested_uri == publication.manifest_uri:
            raise ContractViolation(
                "route nested urgency fingerprint URI는 publication manifest URI와 달라야 합니다."
            )
        nested_expectation = _ObjectExpectation(
            urgency_manifest.input_fingerprint_sha256,
            require_canonical_json=True,
        )
        _add_expectation(expectations, nested_uri, nested_expectation)
        if nested_uri not in payload_by_uri:
            payload_by_uri[nested_uri] = _read_verified_object(
                object_store,
                nested_uri,
                expectations[nested_uri],
            )
        urgency_fingerprint = parse_input_fingerprint(
            payload_by_uri[nested_uri],
            "station_urgency",
        )
        validate_route_urgency_dependencies(
            publication.input_fingerprint,
            urgency_fingerprint,
        )

    raw_business_times = validate_staging(
        publication,
        MappingProxyType(dict(payload_by_uri)),
    )
    if not isinstance(raw_business_times, Mapping):
        raise ContractViolation(
            "staging validator는 business time field mapping을 반환해야 합니다."
        )
    business_times = BusinessTimeEvidence(
        publication_key=publication.manifest.publication_key,
        published_row_cnt=publication.manifest.published_row_cnt,
        values_by_field=MappingProxyType(
            _materialize_business_times(raw_business_times)
        ),
    )

    manifest_payload = publication.manifest.canonical_bytes
    object_store.put_once(
        publication.manifest_uri,
        manifest_payload,
        expected_sha256=publication.manifest.sha256,
        require_canonical_json=True,
    )
    _read_verified_object(
        object_store,
        publication.manifest_uri,
        _ObjectExpectation(
            publication.manifest.sha256,
            require_canonical_json=True,
            exact_payload=manifest_payload,
        ),
    )
    return publication, business_times


def _add_expectation(
    expectations: dict[str, _ObjectExpectation],
    uri: str,
    incoming: _ObjectExpectation,
) -> None:
    """URI별 기대값을 병합하고 서로 다른 checksum·bytes 결합을 거부한다."""
    existing = expectations.get(uri)
    if existing is None:
        expectations[uri] = incoming
        return
    if existing.sha256 != incoming.sha256:
        raise ContractViolation(
            f"같은 immutable URI에 서로 다른 checksum이 결합됐습니다: {uri}"
        )
    if (
        existing.exact_payload is not None
        and incoming.exact_payload is not None
        and existing.exact_payload != incoming.exact_payload
    ):
        raise ContractViolation(
            f"같은 immutable URI에 서로 다른 canonical bytes가 결합됐습니다: {uri}"
        )
    expectations[uri] = _ObjectExpectation(
        sha256=existing.sha256,
        require_canonical_json=(
            existing.require_canonical_json or incoming.require_canonical_json
        ),
        exact_payload=(
            existing.exact_payload
            if existing.exact_payload is not None
            else incoming.exact_payload
        ),
    )


def _read_verified_object(
    object_store: ImmutableObjectStore,
    uri: str,
    expectation: _ObjectExpectation,
) -> bytes:
    """store 반환값도 다시 checksum·canonical·exact bytes로 독립 검증한다."""
    payload = object_store.read_bytes(
        uri,
        expectation.sha256,
        require_canonical_json=expectation.require_canonical_json,
    )
    if type(payload) is not bytes:
        raise ContractViolation(
            f"immutable object store가 bytes가 아닌 값을 반환했습니다: {uri}"
        )
    actual_sha256 = sha256_hex(payload)
    if actual_sha256 != expectation.sha256:
        raise ObjectChecksumMismatchError(
            f"immutable object checksum이 다릅니다: uri={uri}, "
            f"expected={expectation.sha256}, actual={actual_sha256}"
        )
    if expectation.require_canonical_json:
        try:
            parse_canonical_json(payload)
        except CanonicalParseError as exc:
            raise ObjectNotCanonicalError(
                f"immutable JSON object가 canonical bytes가 아닙니다: {uri}"
            ) from exc
    if expectation.exact_payload is not None and payload != expectation.exact_payload:
        raise ContractViolation(
            f"immutable canonical document가 준비한 typed document와 다릅니다: {uri}"
        )
    return payload


def _materialize_business_times(
    values_by_field: Mapping[str, Iterable[datetime]],
) -> dict[str, tuple[datetime, ...]]:
    """validator 반환 iterable을 field별 immutable tuple로 물질화한다."""
    materialized: dict[str, tuple[datetime, ...]] = {}
    for name, values in values_by_field.items():
        _require_nfc_nonblank(name, "business time field")
        if type(values) in {str, bytes}:
            raise ContractViolation(
                f"business time field {name}은 datetime iterable이어야 합니다."
            )
        try:
            materialized[name] = tuple(values)
        except TypeError as exc:
            raise ContractViolation(
                f"business time field {name}은 datetime iterable이어야 합니다."
            ) from exc
    return materialized


def _validate_special_input_documents(
    publication_key: str,
    input_by_role: Mapping[str, InputArtifact],
    payload_by_uri: Mapping[str, bytes],
) -> None:
    """SSOT가 canonical JSON으로 정한 특수 input을 typed parser로 검증한다."""
    if publication_key == "station":
        window_artifact = input_by_role["station_realtime_window_set"]
        parse_station_realtime_window_set(payload_by_uri[window_artifact.uri])
        relocation_artifact = input_by_role.get("station_relocation_approval")
        if relocation_artifact is not None:
            parse_station_relocation_approval(payload_by_uri[relocation_artifact.uri])
    elif publication_key == "rebalance_route":
        coverage_artifact = input_by_role["route_coverage"]
        parse_route_coverage(payload_by_uri[coverage_artifact.uri])


def _utc_dttm(value: datetime, name: str) -> datetime:
    """timezone-aware datetime을 UTC로 바꾸고 naive 값을 거부한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise PublicationTimeError(f"{name}은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)


def _require_nfc_nonblank(value: Any, name: str) -> str:
    """문자열이 NFC이며 공백이 아닌지 검증하고 그대로 반환한다."""
    if type(value) is not str or not value.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    if unicodedata.normalize("NFC", value) != value:
        raise ContractViolation(f"{name}은 Unicode NFC여야 합니다.")
    return value


def _evidence_material(value: VerifiedPublicationEvidence) -> tuple[Any, ...]:
    """token의 현재 값을 재검증해 closure registry용 불변 재료로 반환한다."""
    fingerprint_payload = value.publication.input_fingerprint.canonical_bytes
    fingerprint = parse_input_fingerprint(
        fingerprint_payload,
        value.publication.manifest.publication_key,
    )
    manifest_payload = value.publication.manifest.canonical_bytes
    manifest = parse_publication_manifest(manifest_payload)
    PreparedPublication(
        manifest=manifest,
        manifest_uri=value.publication.manifest_uri,
        input_fingerprint=fingerprint,
    )
    business_times = BusinessTimeEvidence(
        publication_key=value.business_times.publication_key,
        published_row_cnt=value.business_times.published_row_cnt,
        values_by_field=value.business_times.values_by_field,
    )
    return (
        manifest_payload,
        value.publication.manifest_uri,
        fingerprint_payload,
        business_times.publication_key,
        business_times.published_row_cnt,
        tuple(
            (name, tuple(business_times.values_by_field[name]))
            for name in sorted(business_times.values_by_field)
        ),
    )


def _evidence_from_material(material: tuple[Any, ...]) -> VerifiedPublicationEvidence:
    """closure registry의 불변 재료에서 executor용 fresh snapshot을 만든다."""
    (
        manifest_payload,
        manifest_uri,
        fingerprint_payload,
        business_publication_key,
        business_row_count,
        business_fields,
    ) = material
    manifest = parse_publication_manifest(manifest_payload)
    fingerprint = parse_input_fingerprint(
        fingerprint_payload,
        manifest.publication_key,
    )
    publication = PreparedPublication(manifest, manifest_uri, fingerprint)
    business_times = BusinessTimeEvidence(
        business_publication_key,
        business_row_count,
        dict(business_fields),
    )
    snapshot = object.__new__(VerifiedPublicationEvidence)
    object.__setattr__(snapshot, "publication", publication)
    object.__setattr__(snapshot, "business_times", business_times)
    return snapshot


def _build_verification_boundary() -> tuple[
    Callable[..., Any],
    Callable[[object], bool],
    Callable[[object], VerifiedPublicationEvidence | None],
]:
    """발급 registry를 closure에 숨긴 verifier와 token 검증기를 만든다."""
    issued: dict[
        int,
        tuple[
            weakref.ReferenceType[VerifiedPublicationEvidence],
            tuple[Any, ...],
        ],
    ] = {}

    def verify_publication_evidence(
        publication: PreparedPublication,
        object_store: ImmutableObjectStore,
        validate_staging: StagingEvidenceValidator,
    ) -> VerifiedPublicationEvidence:
        """실제 object·business time·manifest-last를 검증하고 token을 발급한다."""
        verified_publication, business_times = _verify_publication_evidence(
            publication,
            object_store,
            validate_staging,
        )
        token = object.__new__(VerifiedPublicationEvidence)
        object.__setattr__(token, "publication", verified_publication)
        object.__setattr__(token, "business_times", business_times)
        identifier = id(token)

        def unregister(
            reference: weakref.ReferenceType[VerifiedPublicationEvidence],
        ) -> None:
            """token이 소멸하면 같은 identity의 registry 항목만 제거한다."""
            current = issued.get(identifier)
            if current is not None and current[0] is reference:
                issued.pop(identifier, None)

        reference = weakref.ref(token, unregister)
        issued[identifier] = (reference, _evidence_material(token))
        return token

    def snapshot_verified_publication_evidence(
        value: object,
    ) -> VerifiedPublicationEvidence | None:
        """발급·무변조 token의 registry 재료로 executor용 fresh snapshot을 만든다."""
        if type(value) is not VerifiedPublicationEvidence:
            return None
        registered = issued.get(id(value))
        if registered is None or registered[0]() is not value:
            return None
        try:
            if registered[1] != _evidence_material(value):
                return None
            return _evidence_from_material(registered[1])
        except (AttributeError, ContractViolation, TypeError, ValueError):
            return None

    def is_verified_publication_evidence(value: object) -> bool:
        """값이 현재 process verifier가 발급하고 변조되지 않은 token인지 반환한다."""
        return snapshot_verified_publication_evidence(value) is not None

    return (
        verify_publication_evidence,
        is_verified_publication_evidence,
        snapshot_verified_publication_evidence,
    )


(
    verify_publication_evidence,
    is_verified_publication_evidence,
    _snapshot_verified_publication_evidence,
) = _build_verification_boundary()
del _build_verification_boundary
