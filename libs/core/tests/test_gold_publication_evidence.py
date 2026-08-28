"""Gold publication immutable evidence와 business-time proof 경계를 검증한다."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import UTC, datetime

import pytest
from core.gold_publication.canonical import parse_canonical_json, sha256_hex
from core.gold_publication.contract import (
    Artifact,
    Dependency,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PublicationManifest,
    build_artifact_set,
    build_input_fingerprint,
    build_publication_manifest,
    get_publication_spec,
)
from core.gold_publication.documents import build_route_coverage
from core.gold_publication.errors import (
    ContractViolation,
    ObjectChecksumMismatchError,
    ObjectCollisionError,
    ObjectMissingError,
)
from core.gold_publication.evidence import (
    PreparedPublication,
    verify_publication_evidence,
)
from core.gold_publication.storage import ImmutablePutOutcome

_LOGICAL_DTTM = datetime(2026, 8, 19, 16, 0, tzinfo=UTC)


class _MemoryObjectStore:
    """검증 순서와 실제 bytes를 관찰할 수 있는 in-memory immutable store다."""

    def __init__(
        self,
        objects: Mapping[str, bytes],
        *,
        trust_expected_hash: bool = False,
        discard_put: bool = False,
    ) -> None:
        """초기 object와 의도적인 오동작 모드를 설정한다."""
        self.objects = dict(objects)
        self.trust_expected_hash = trust_expected_hash
        self.discard_put = discard_put
        self.operations: list[tuple[str, str]] = []

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """정확한 URI를 읽고 일반 모드에서는 checksum과 canonical JSON을 검증한다."""
        self.operations.append(("read", uri))
        try:
            payload = self.objects[uri]
        except KeyError as exc:
            raise ObjectMissingError(f"immutable object가 없습니다: {uri}") from exc
        if not self.trust_expected_hash and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"immutable checksum mismatch: {uri}")
        if require_canonical_json:
            parse_canonical_json(payload)
        return payload

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """기존 bytes를 덮지 않고 새 object만 생성한다."""
        self.operations.append(("put", uri))
        if expected_sha256 is not None and sha256_hex(payload) != expected_sha256:
            raise ObjectChecksumMismatchError(f"immutable put checksum mismatch: {uri}")
        if require_canonical_json:
            parse_canonical_json(payload)
        existing = self.objects.get(uri)
        if existing is not None:
            if existing == payload:
                return ImmutablePutOutcome.ALREADY_EXISTS
            raise ObjectCollisionError(f"immutable URI collision: {uri}")
        if not self.discard_put:
            self.objects[uri] = payload
        return ImmutablePutOutcome.CREATED


def test_verifier_reads_every_object_and_puts_manifest_last() -> None:
    """fingerprint·모든 artifact를 읽은 뒤 manifest를 마지막 write로 생성한다."""
    prepared, objects = _event_prepared()
    store = _MemoryObjectStore(objects)
    staging_calls: list[Mapping[str, bytes]] = []

    def validate_staging(
        _publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, Iterable[datetime]]:
        """검증된 payload와 event 행별 last-seen 시각을 반환한다."""
        staging_calls.append(payloads)
        return {"last_seen_dttm": (_LOGICAL_DTTM,)}

    evidence = verify_publication_evidence(prepared, store, validate_staging)

    expected_read_uris = {
        prepared.manifest.input_fingerprint_uri,
        *(artifact.uri for artifact in prepared.input_fingerprint.input_artifacts),
        *(artifact.uri for artifact in prepared.manifest.artifacts),
        prepared.manifest_uri,
    }
    assert evidence.manifest == prepared.manifest
    assert {uri for operation, uri in store.operations if operation == "read"} == (
        expected_read_uris
    )
    assert [item for item in store.operations if item[0] == "put"] == [
        ("put", prepared.manifest_uri)
    ]
    assert store.objects[prepared.manifest_uri] == prepared.manifest.canonical_bytes
    assert staging_calls
    manifest_put_index = store.operations.index(("put", prepared.manifest_uri))
    assert all(
        index < manifest_put_index
        for index, (operation, uri) in enumerate(store.operations)
        if operation == "read" and uri != prepared.manifest_uri
    )


@pytest.mark.parametrize("missing_kind", ["fingerprint", "input", "output"])
def test_missing_immutable_object_fails_before_manifest_write(
    missing_kind: str,
) -> None:
    """fingerprint 또는 input/output 한 개가 없으면 manifest와 evidence를 만들지 않는다."""
    prepared, objects = _event_prepared()
    missing_uri = {
        "fingerprint": prepared.manifest.input_fingerprint_uri,
        "input": prepared.input_fingerprint.input_artifacts[0].uri,
        "output": prepared.manifest.artifacts[0].uri,
    }[missing_kind]
    objects.pop(missing_uri)
    store = _MemoryObjectStore(objects)

    with pytest.raises(ObjectMissingError, match=missing_uri):
        verify_publication_evidence(prepared, store, _event_business_times)

    assert prepared.manifest_uri not in store.objects
    assert ("put", prepared.manifest_uri) not in store.operations


def test_verifier_rehashes_store_result_instead_of_trusting_store() -> None:
    """store가 expected hash를 무시해도 공통 verifier가 checksum drift를 거부한다."""
    prepared, objects = _event_prepared()
    output_uri = prepared.manifest.artifacts[0].uri
    objects[output_uri] = b"drifted output"
    store = _MemoryObjectStore(objects, trust_expected_hash=True)

    with pytest.raises(ObjectChecksumMismatchError, match=output_uri):
        verify_publication_evidence(prepared, store, _event_business_times)

    assert prepared.manifest_uri not in store.objects


def test_existing_different_manifest_is_never_overwritten() -> None:
    """같은 manifest URI의 다른 bytes는 collision으로 실패하고 그대로 보존한다."""
    prepared, objects = _event_prepared()
    objects[prepared.manifest_uri] = b"existing different manifest"
    store = _MemoryObjectStore(objects)

    with pytest.raises(ObjectCollisionError, match="collision"):
        verify_publication_evidence(prepared, store, _event_business_times)

    assert store.objects[prepared.manifest_uri] == b"existing different manifest"


def test_manifest_put_is_read_back_before_evidence_is_issued() -> None:
    """put_once가 실제 object를 만들지 않으면 readback 실패로 evidence를 만들지 않는다."""
    prepared, objects = _event_prepared()
    store = _MemoryObjectStore(objects, discard_put=True)

    with pytest.raises(ObjectMissingError, match=prepared.manifest_uri):
        verify_publication_evidence(prepared, store, _event_business_times)


def test_verifier_reads_linked_manifest_and_nested_fingerprint() -> None:
    """route verifier가 urgency manifest와 그 fingerprint 실제 bytes를 모두 검증한다."""
    prepared, objects, nested_uri = _route_prepared()
    store = _MemoryObjectStore(objects)

    evidence = verify_publication_evidence(
        prepared,
        store,
        lambda _publication, _payloads: {"proposed_dttm": (_LOGICAL_DTTM,)},
    )

    urgency_manifest_uri = next(
        artifact.uri
        for artifact in prepared.input_fingerprint.input_artifacts
        if artifact.role == "urgency_publication_manifest"
    )
    assert evidence.manifest == prepared.manifest
    assert ("read", urgency_manifest_uri) in store.operations
    assert ("read", nested_uri) in store.operations


def test_linked_manifest_tuple_mismatch_fails_before_manifest_write() -> None:
    """실제 urgency manifest와 다른 dependency tuple이면 route evidence를 만들지 않는다."""
    prepared, objects, _nested_uri = _route_prepared(linked_revision_offset=1)
    store = _MemoryObjectStore(objects)

    with pytest.raises(ContractViolation, match="dependency tuple"):
        verify_publication_evidence(
            prepared,
            store,
            lambda _publication, _payloads: {"proposed_dttm": (_LOGICAL_DTTM,)},
        )

    assert prepared.manifest_uri not in store.objects


def test_route_nested_dependency_mismatch_fails_before_manifest_write() -> None:
    """route와 urgency가 읽은 station tuple이 다르면 evidence를 만들지 않는다."""
    prepared, objects, _nested_uri = _route_prepared(route_station_salt="corrected")
    store = _MemoryObjectStore(objects)

    with pytest.raises(ContractViolation, match="station"):
        verify_publication_evidence(
            prepared,
            store,
            lambda _publication, _payloads: {"proposed_dttm": (_LOGICAL_DTTM,)},
        )

    assert prepared.manifest_uri not in store.objects


def test_route_coverage_bytes_must_be_exact_typed_document() -> None:
    """SHA가 맞아도 route coverage가 canonical typed 문서가 아니면 거부한다."""
    malformed = b'{"schema_version":"gold-route-coverage-v1"}'
    prepared, objects, _nested_uri = _route_prepared(route_coverage_payload=malformed)
    store = _MemoryObjectStore(objects)

    with pytest.raises(ContractViolation, match="key"):
        verify_publication_evidence(
            prepared,
            store,
            lambda _publication, _payloads: {"proposed_dttm": (_LOGICAL_DTTM,)},
        )

    assert prepared.manifest_uri not in store.objects


@pytest.mark.parametrize(
    "business_times",
    [
        {},
        {"last_seen_dttm": ()},
        {
            "last_seen_dttm": (_LOGICAL_DTTM,),
            "unexpected_dttm": (_LOGICAL_DTTM,),
        },
    ],
)
def test_business_time_evidence_requires_exact_fields_and_row_count(
    business_times: Mapping[str, tuple[datetime, ...]],
) -> None:
    """필수 field 누락·행 수 누락·추가 field를 모두 manifest write 전에 거부한다."""
    prepared, objects = _event_prepared()
    store = _MemoryObjectStore(objects)

    with pytest.raises(ContractViolation, match="business time"):
        verify_publication_evidence(
            prepared,
            store,
            lambda _publication, _payloads: business_times,
        )

    assert prepared.manifest_uri not in store.objects


def test_raw_business_tuple_cannot_enter_prepared_publication() -> None:
    """구 raw business time tuple API를 PreparedPublication에서 거부한다."""
    prepared, _objects = _event_prepared()

    with pytest.raises(TypeError, match="business_dttms"):
        PreparedPublication(
            manifest=prepared.manifest,
            manifest_uri=prepared.manifest_uri,
            input_fingerprint=prepared.input_fingerprint,
            business_dttms=(_LOGICAL_DTTM,),  # type: ignore[call-arg]
        )

def _event_business_times(
    _publication: PreparedPublication,
    _payloads: Mapping[str, bytes],
) -> Mapping[str, Iterable[datetime]]:
    """event 한 행의 complete bounded business time을 반환한다."""
    return {"last_seen_dttm": (_LOGICAL_DTTM,)}


def _event_prepared() -> tuple[PreparedPublication, dict[str, bytes]]:
    """manifest를 제외한 실제 immutable object를 가진 event publication을 만든다."""
    input_uri = "s3://fixture/input/cultural-event.json"
    input_payload = b"source event manifest bytes"
    fingerprint = build_input_fingerprint(
        "event:cultural_event",
        (),
        (
            InputArtifact(
                byte_sha256=sha256_hex(input_payload),
                role="cultural_event_manifest",
                uri=input_uri,
            ),
        ),
        (
            Parameter("event_identity_version", "event-id-v1"),
            Parameter("event_policy_version", "event-policy-v1"),
        ),
    )
    output_uri = "s3://fixture/output/cultural-event.parquet"
    output_payload = b"event parquet bytes"
    artifact_set = build_artifact_set(
        (
            Artifact(
                byte_sha256=sha256_hex(output_payload),
                role="event_cultural_event",
                row_count=1,
                uri=output_uri,
            ),
        )
    )
    fingerprint_uri = "s3://fixture/input/cultural-event-fingerprint.json"
    manifest = build_publication_manifest(
        publication_key="event:cultural_event",
        artifact_set=artifact_set,
        input_fingerprint=fingerprint,
        input_fingerprint_uri=fingerprint_uri,
        logical_dttm=_LOGICAL_DTTM,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"event": 1},
    )
    prepared = PreparedPublication(
        manifest=manifest,
        manifest_uri="s3://fixture/manifest/cultural-event.json",
        input_fingerprint=fingerprint,
    )
    return prepared, {
        input_uri: input_payload,
        output_uri: output_payload,
        fingerprint_uri: fingerprint.canonical_bytes,
    }


def _route_prepared(
    *,
    linked_revision_offset: int = 0,
    route_station_salt: str = "base",
    route_coverage_payload: bytes | None = None,
) -> tuple[PreparedPublication, dict[str, bytes], str]:
    """linked manifest와 nested fingerprint를 포함한 route fixture를 만든다."""
    urgency_station = _dependency("station", "base")
    demand_manifest, demand_manifest_uri, _demand_fingerprint = _fixture_manifest(
        "station_demand_forecast",
        (urgency_station,),
        "demand",
    )
    demand_dependency = _dependency_from_manifest(demand_manifest, demand_manifest_uri)
    stock_manifest, stock_manifest_uri, _stock_fingerprint = _fixture_manifest(
        "station_stock",
        (),
        "stock",
    )
    stock_dependency = _dependency_from_manifest(stock_manifest, stock_manifest_uri)

    urgency_dependencies = (
        urgency_station,
        demand_dependency,
        stock_dependency,
    )
    urgency_fingerprint, _urgency_inputs = _fixture_fingerprint(
        "station_urgency",
        urgency_dependencies,
        "urgency",
        {
            "demand_publication_manifest": (
                demand_manifest_uri,
                demand_manifest.canonical_bytes,
            ),
            "stock_publication_manifest": (
                stock_manifest_uri,
                stock_manifest.canonical_bytes,
            ),
        },
    )
    urgency_manifest, urgency_manifest_uri = _manifest_from_fingerprint(
        "station_urgency",
        urgency_fingerprint,
        "urgency",
    )
    urgency_dependency = _dependency_from_manifest(
        urgency_manifest,
        urgency_manifest_uri,
    )
    if linked_revision_offset:
        urgency_dependency = Dependency(
            artifact_set_sha256=urgency_dependency.artifact_set_sha256,
            input_fingerprint_sha256=urgency_dependency.input_fingerprint_sha256,
            logical_dttm=urgency_dependency.logical_dttm,
            manifest_uri=urgency_dependency.manifest_uri,
            publication_key=urgency_dependency.publication_key,
            revision_no=urgency_dependency.revision_no + linked_revision_offset,
        )

    route_dependencies = (
        _dependency("dispatch_center", "base"),
        _dependency("station", route_station_salt),
        demand_dependency,
        stock_dependency,
        urgency_dependency,
    )
    route_fingerprint, objects = _fixture_fingerprint(
        "rebalance_route",
        route_dependencies,
        "route",
        {
            "route_coverage": (
                "s3://fixture/route/input/route_coverage.json",
                route_coverage_payload
                if route_coverage_payload is not None
                else build_route_coverage(
                    stock_anchor_dttm=_LOGICAL_DTTM,
                    routes=(),
                ).canonical_bytes,
            ),
            "urgency_publication_manifest": (
                urgency_manifest_uri,
                urgency_manifest.canonical_bytes,
            ),
        },
    )
    route_manifest, route_manifest_uri = _manifest_from_fingerprint(
        "rebalance_route",
        route_fingerprint,
        "route",
    )
    for artifact in route_manifest.artifacts:
        objects[artifact.uri] = f"route:{artifact.role}:output".encode()
    objects[route_manifest.input_fingerprint_uri] = route_fingerprint.canonical_bytes
    objects[urgency_manifest.input_fingerprint_uri] = (
        urgency_fingerprint.canonical_bytes
    )
    return (
        PreparedPublication(
            manifest=route_manifest,
            manifest_uri=route_manifest_uri,
            input_fingerprint=route_fingerprint,
        ),
        objects,
        urgency_manifest.input_fingerprint_uri,
    )


def _fixture_manifest(
    publication_key: str,
    dependencies: tuple[Dependency, ...],
    salt: str,
) -> tuple[PublicationManifest, str, InputFingerprint]:
    """다른 fingerprint가 실제 bytes로 참조할 valid manifest를 만든다."""
    fingerprint, _objects = _fixture_fingerprint(
        publication_key,
        dependencies,
        salt,
        {},
    )
    manifest, manifest_uri = _manifest_from_fingerprint(
        publication_key,
        fingerprint,
        salt,
    )
    return manifest, manifest_uri, fingerprint


def _fixture_fingerprint(
    publication_key: str,
    dependencies: tuple[Dependency, ...],
    salt: str,
    role_overrides: Mapping[str, tuple[str, bytes]],
) -> tuple[InputFingerprint, dict[str, bytes]]:
    """registry role과 parameter를 만족하는 fingerprint와 실제 input bytes를 만든다."""
    spec = get_publication_spec(publication_key)
    artifacts: list[InputArtifact] = []
    objects: dict[str, bytes] = {}
    for cardinality in spec.input_roles:
        if cardinality.minimum == 0:
            continue
        uri, payload = role_overrides.get(
            cardinality.role,
            (
                f"s3://fixture/{salt}/input/{cardinality.role}.json",
                f"{salt}:{cardinality.role}:input".encode(),
            ),
        )
        objects[uri] = payload
        artifacts.append(
            InputArtifact(
                byte_sha256=sha256_hex(payload),
                role=cardinality.role,
                uri=uri,
            )
        )
    artifact_by_role = {artifact.role: artifact for artifact in artifacts}
    parameters = tuple(
        Parameter(
            name,
            sha256_hex(f"{salt}:expected-station-ids".encode())
            if name == "expected_sta_id_sha256"
            else artifact_by_role["route_coverage"].byte_sha256
            if name == "route_coverage_sha256"
            else "fixture:rebalance_policy_config"
            if name == "rebalance_policy_config"
            else f"{salt}:{name}",
        )
        for name in spec.parameter_names
    )
    return (
        build_input_fingerprint(
            publication_key,
            dependencies,
            artifacts,
            parameters,
        ),
        objects,
    )


def _manifest_from_fingerprint(
    publication_key: str,
    fingerprint: InputFingerprint,
    salt: str,
) -> tuple[PublicationManifest, str]:
    """fingerprint에 결합된 non-empty manifest와 외부 URI를 만든다."""
    spec = get_publication_spec(publication_key)
    target_counts = {target: 1 for target in spec.target_tables}
    artifacts = tuple(
        Artifact(
            byte_sha256=sha256_hex(f"{salt}:{role}:output".encode()),
            role=role,
            row_count=target_counts[target],
            uri=f"s3://fixture/{salt}/output/{role}.parquet",
        )
        for role, target in spec.output_targets
    )
    manifest = build_publication_manifest(
        publication_key=publication_key,
        artifact_set=build_artifact_set(artifacts),
        input_fingerprint=fingerprint,
        input_fingerprint_uri=f"s3://fixture/{salt}/input/fingerprint.json",
        logical_dttm=_LOGICAL_DTTM,
        publisher_version="fixture-v1",
        revision_no=0,
        target_row_counts=target_counts,
    )
    return manifest, f"s3://fixture/{salt}/manifest.json"


def _dependency(publication_key: str, salt: str) -> Dependency:
    """nested dependency 대조에 쓸 결정적인 state identity를 만든다."""
    return Dependency(
        artifact_set_sha256=sha256_hex(f"{salt}:{publication_key}:artifacts".encode()),
        input_fingerprint_sha256=sha256_hex(
            f"{salt}:{publication_key}:inputs".encode()
        ),
        logical_dttm=_LOGICAL_DTTM,
        manifest_uri=f"s3://fixture/{salt}/{publication_key}/manifest.json",
        publication_key=publication_key,
        revision_no=0,
    )


def _dependency_from_manifest(
    manifest: PublicationManifest,
    manifest_uri: str,
) -> Dependency:
    """실제 manifest의 state identity를 dependency tuple로 만든다."""
    return Dependency(
        artifact_set_sha256=manifest.artifact_set_sha256,
        input_fingerprint_sha256=manifest.input_fingerprint_sha256,
        logical_dttm=manifest.logical_dttm,
        manifest_uri=manifest_uri,
        publication_key=manifest.publication_key,
        revision_no=manifest.revision_no,
    )
