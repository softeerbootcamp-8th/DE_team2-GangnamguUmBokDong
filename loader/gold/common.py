"""Gold publisher가 공유하는 immutable object와 publication 조립 경계를 제공한다."""

from __future__ import annotations

import io
import json
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import yaml
from core.gold_publication import (
    Artifact,
    ArtifactSet,
    Dependency,
    ImmutableObjectStore,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PreparedPublication,
    PublicationManifest,
    PublicationResult,
    VerifiedPublicationEvidence,
    build_artifact_set,
    build_input_fingerprint,
    build_publication_manifest,
    execute_publication,
    get_publication_spec,
    sha256_hex,
)
from core.gold_publication.errors import CanonicalParseError, ContractViolation
from core.gold_publication.evidence import StagingEvidenceValidator
from core.gold_publication.transaction import (
    ConditionalEmptyValidator,
    LockedValidator,
    MutationCallback,
)
from core.source_snapshot import (
    SourceSnapshotContractError,
    SourceSnapshotManifest,
    SourceSnapshotStatus,
    parse_source_snapshot_manifest,
)
from psycopg import Connection

_SAFE_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._:-]+")


class _UniqueKeySafeLoader(yaml.SafeLoader):
    """중복 mapping key를 거부하는 PyYAML safe loader다."""


def _construct_unique_mapping(
    loader: _UniqueKeySafeLoader,
    node: yaml.MappingNode,
    deep: bool = False,
) -> dict[Any, Any]:
    """YAML mapping을 만들며 같은 key가 두 번 나오면 거부한다."""
    loader.flatten_mapping(node)
    result: dict[Any, Any] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        try:
            duplicate = key in result
        except TypeError as exc:
            raise ContractViolation(
                "YAML mapping key는 hashable이어야 합니다."
            ) from exc
        if duplicate:
            raise ContractViolation(f"YAML mapping key가 중복됐습니다: {key!r}")
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueKeySafeLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG,
    _construct_unique_mapping,
)


@dataclass(frozen=True, slots=True)
class OutputObject:
    """publication output artifact로 쓸 완성된 bytes를 표현한다."""

    role: str
    payload: bytes
    row_count: int
    suffix: str = "parquet"

    def __post_init__(self) -> None:
        """output field가 content-addressed object를 만들 수 있는지 검증한다."""
        _require_path_segment(self.role, "output role")
        if type(self.payload) is not bytes:
            raise ContractViolation("output payload는 bytes여야 합니다.")
        if type(self.row_count) is not int or self.row_count < 0:
            raise ContractViolation("output row_count는 0 이상 integer여야 합니다.")
        _require_path_segment(self.suffix, "output suffix")


@dataclass(frozen=True, slots=True)
class PublicationMaterials:
    """revision과 manifest를 정하기 전 고정된 artifact·fingerprint를 보관한다."""

    artifact_set: ArtifactSet
    input_fingerprint: InputFingerprint
    input_fingerprint_uri: str

    def __post_init__(self) -> None:
        """material 문서와 URI 타입을 검증한다."""
        if type(self.artifact_set) is not ArtifactSet:
            raise ContractViolation("artifact_set은 ArtifactSet이어야 합니다.")
        if type(self.input_fingerprint) is not InputFingerprint:
            raise ContractViolation("input_fingerprint는 InputFingerprint여야 합니다.")
        _require_s3_base_or_object_uri(
            self.input_fingerprint_uri,
            "input fingerprint URI",
        )


@dataclass(frozen=True, slots=True)
class PublicationExecution:
    """게시 결과와 실제 검증된 immutable evidence를 함께 반환한다."""

    result: PublicationResult
    evidence: tuple[VerifiedPublicationEvidence, ...]


@dataclass(frozen=True, slots=True)
class SourceSnapshotPayload:
    """검증된 source manifest와 그 manifest가 소유한 Silver bytes를 결합한다."""

    manifest: SourceSnapshotManifest
    manifest_bytes: bytes
    silver_bytes: bytes | None

    def __post_init__(self) -> None:
        """status와 Silver payload 유무가 source snapshot 계약과 같은지 검증한다."""
        if type(self.manifest) is not SourceSnapshotManifest:
            raise ContractViolation(
                "source snapshot manifest 타입이 올바르지 않습니다."
            )
        if type(self.manifest_bytes) is not bytes:
            raise ContractViolation("source snapshot manifest bytes가 필요합니다.")
        if self.manifest.canonical_bytes != self.manifest_bytes:
            raise ContractViolation(
                "source snapshot manifest canonical bytes가 다릅니다."
            )
        if self.manifest.status is SourceSnapshotStatus.SUCCEEDED:
            if type(self.silver_bytes) is not bytes:
                raise ContractViolation(
                    "SUCCEEDED source snapshot은 Silver bytes가 필요합니다."
                )
        elif self.silver_bytes is not None:
            raise ContractViolation(
                "EMPTY source snapshot은 Silver bytes를 가질 수 없습니다."
            )


def parse_yaml_mapping(payload: bytes) -> Mapping[str, Any]:
    """UTF-8 YAML bytes를 중복 key 없는 root mapping으로 읽는다."""
    if type(payload) is not bytes:
        raise ContractViolation("YAML payload는 bytes여야 합니다.")
    try:
        text = payload.decode("utf-8", errors="strict")
    except UnicodeDecodeError as exc:
        raise ContractViolation("YAML payload는 UTF-8이어야 합니다.") from exc
    try:
        value = yaml.load(text, Loader=_UniqueKeySafeLoader)
    except yaml.YAMLError as exc:
        raise ContractViolation("YAML payload를 안전하게 파싱할 수 없습니다.") from exc
    if type(value) is not dict:
        raise ContractViolation("YAML root는 mapping이어야 합니다.")
    if any(type(key) is not str for key in value):
        raise ContractViolation("YAML root key는 문자열이어야 합니다.")
    return value


def parse_json_mapping(payload: bytes) -> Mapping[str, Any]:
    """UTF-8 JSON bytes를 중복 key·비표준 상수 없는 root mapping으로 읽는다."""
    if type(payload) is not bytes:
        raise ContractViolation("JSON payload는 bytes여야 합니다.")

    def reject_constant(value: str) -> None:
        """NaN과 Infinity 같은 비표준 JSON 상수를 거부한다."""
        raise ContractViolation(f"JSON 비표준 상수는 허용하지 않습니다: {value}")

    def unique_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        """JSON object pair를 mapping으로 만들며 중복 key를 거부한다."""
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ContractViolation(f"JSON object key가 중복됐습니다: {key}")
            result[key] = value
        return result

    try:
        value = json.loads(
            payload.decode("utf-8", errors="strict"),
            object_pairs_hook=unique_pairs,
            parse_constant=reject_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ContractViolation("JSON payload를 엄격하게 파싱할 수 없습니다.") from exc
    if type(value) is not dict:
        raise ContractViolation("JSON root는 object여야 합니다.")
    return value


def read_source_snapshot_payload(
    object_store: ImmutableObjectStore,
    *,
    manifest_artifact: InputArtifact,
    verified_payloads: Mapping[str, bytes],
    expected_source_id: str,
    expected_logical_dttm: datetime | None = None,
) -> SourceSnapshotPayload:
    """verifier가 읽은 manifest에서 authoritative Silver exact bytes를 연다.

    manifest 자체는 input fingerprint URI·SHA로 공통 verifier가 먼저 읽어야 한다.
    SUCCEEDED일 때만 manifest의 content-addressed Silver URI·SHA를 추가로 exact read하며,
    confirmed EMPTY는 Silver를 읽거나 임의의 빈 table로 승격하지 않는다.
    """
    if type(manifest_artifact) is not InputArtifact:
        raise ContractViolation("manifest_artifact는 InputArtifact여야 합니다.")
    if not isinstance(verified_payloads, Mapping):
        raise ContractViolation("verified_payloads는 URI별 bytes mapping이어야 합니다.")
    if type(expected_source_id) is not str or not expected_source_id.strip():
        raise ContractViolation("expected_source_id는 nonblank 문자열이어야 합니다.")
    try:
        manifest_payload = verified_payloads[manifest_artifact.uri]
    except KeyError as exc:
        raise ContractViolation(
            "verifier payload에 source manifest가 없습니다."
        ) from exc
    if type(manifest_payload) is not bytes:
        raise ContractViolation("source manifest payload는 bytes여야 합니다.")
    if sha256_hex(manifest_payload) != manifest_artifact.byte_sha256:
        raise ContractViolation(
            "source manifest payload checksum이 fingerprint와 다릅니다."
        )
    try:
        manifest = parse_source_snapshot_manifest(manifest_payload)
    except (SourceSnapshotContractError, CanonicalParseError) as exc:
        raise ContractViolation(
            "source snapshot manifest 계약이 올바르지 않습니다."
        ) from exc
    if manifest.source_id != expected_source_id:
        raise ContractViolation(
            "source snapshot source_id가 publisher 기대값과 다릅니다: "
            f"expected={expected_source_id}, actual={manifest.source_id}"
        )
    if expected_logical_dttm is not None:
        expected_time = _utc_datetime(
            expected_logical_dttm,
            "expected source logical_dttm",
        )
        if manifest.logical_dttm != expected_time:
            raise ContractViolation(
                "source snapshot logical_dttm이 publisher 기대값과 다릅니다."
            )

    silver_payload: bytes | None = None
    if manifest.status is SourceSnapshotStatus.SUCCEEDED:
        if manifest.silver_uri is None or manifest.silver_byte_sha256 is None:
            raise ContractViolation(
                "SUCCEEDED source snapshot의 Silver identity가 없습니다."
            )
        silver_payload = object_store.read_bytes(
            manifest.silver_uri,
            manifest.silver_byte_sha256,
        )
        if sha256_hex(silver_payload) != manifest.silver_byte_sha256:
            raise ContractViolation(
                "source Silver actual bytes checksum이 manifest와 다릅니다."
            )
    return SourceSnapshotPayload(manifest, manifest_payload, silver_payload)


def source_snapshot_parquet(snapshot: SourceSnapshotPayload) -> pa.Table:
    """SUCCEEDED source snapshot의 검증된 Silver bytes를 Parquet table로 읽는다."""
    if type(snapshot) is not SourceSnapshotPayload:
        raise ContractViolation("snapshot은 SourceSnapshotPayload여야 합니다.")
    if snapshot.manifest.status is not SourceSnapshotStatus.SUCCEEDED:
        raise ContractViolation("EMPTY source snapshot에는 Silver Parquet이 없습니다.")
    if snapshot.silver_bytes is None:
        raise ContractViolation("SUCCEEDED source snapshot의 Silver bytes가 없습니다.")
    table = read_parquet_bytes(snapshot.silver_bytes)
    if table.num_rows != snapshot.manifest.counts.kept:
        raise ContractViolation(
            "source Silver physical row count가 manifest counts.kept와 다릅니다."
        )
    return table


def parquet_bytes(table: pa.Table) -> bytes:
    """PyArrow table을 publisher output용 Parquet bytes로 직렬화한다."""
    if type(table) is not pa.Table:
        raise ContractViolation("Parquet output은 pyarrow.Table이어야 합니다.")
    sink = io.BytesIO()
    pq.write_table(
        table,
        sink,
        compression="zstd",
        use_dictionary=False,
        write_statistics=False,
        version="2.6",
    )
    return sink.getvalue()


def read_parquet_bytes(payload: bytes) -> pa.Table:
    """완전한 Parquet bytes를 PyArrow table로 읽고 trailing bytes를 거부한다."""
    if type(payload) is not bytes:
        raise ContractViolation("Parquet payload는 bytes여야 합니다.")
    try:
        source = pa.BufferReader(payload)
        table = pq.read_table(source)
    except (pa.ArrowException, OSError, ValueError) as exc:
        raise ContractViolation("Parquet payload를 읽을 수 없습니다.") from exc
    return table


def content_addressed_uri(
    base_uri: str,
    *,
    publication_key: str,
    category: str,
    name: str,
    payload: bytes,
    suffix: str,
) -> str:
    """payload SHA-256을 포함한 결정적 S3 immutable object URI를 만든다."""
    base = _require_s3_base_or_object_uri(base_uri, "object base URI").rstrip("/")
    for value, label in (
        (publication_key, "publication key"),
        (category, "object category"),
        (name, "object name"),
        (suffix, "object suffix"),
    ):
        _require_path_segment(value, label)
    if type(payload) is not bytes:
        raise ContractViolation("content-addressed payload는 bytes여야 합니다.")
    return f"{base}/{publication_key}/{category}/{name}-{sha256_hex(payload)}.{suffix}"


def store_input_payload(
    object_store: ImmutableObjectStore,
    *,
    base_uri: str,
    publication_key: str,
    role: str,
    payload: bytes,
    suffix: str,
    require_canonical_json: bool = False,
) -> InputArtifact:
    """실제 input bytes를 immutable URI에 기록하고 fingerprint artifact를 만든다."""
    if type(require_canonical_json) is not bool:
        raise ContractViolation("require_canonical_json은 bool이어야 합니다.")
    uri = content_addressed_uri(
        base_uri,
        publication_key=publication_key,
        category="inputs",
        name=role,
        payload=payload,
        suffix=suffix,
    )
    byte_sha256 = sha256_hex(payload)
    object_store.put_once(
        uri,
        payload,
        expected_sha256=byte_sha256,
        require_canonical_json=require_canonical_json,
    )
    return InputArtifact(byte_sha256=byte_sha256, role=role, uri=uri)


def materialize_publication(
    object_store: ImmutableObjectStore,
    *,
    base_uri: str,
    publication_key: str,
    dependencies: Iterable[Dependency] = (),
    input_artifacts: Iterable[InputArtifact],
    parameters: Iterable[Parameter],
    outputs: Iterable[OutputObject],
) -> PublicationMaterials:
    """output과 fingerprint를 검증 후 immutable write하고 manifest 재료를 반환한다."""
    output_values = tuple(outputs)
    if any(type(output) is not OutputObject for output in output_values):
        raise ContractViolation("outputs는 OutputObject iterable이어야 합니다.")
    spec = get_publication_spec(publication_key)
    output_roles = tuple(
        sorted(
            (output.role for output in output_values),
            key=lambda role: role.encode("utf-8"),
        )
    )
    if output_roles and output_roles != spec.output_roles:
        raise ContractViolation(
            f"{publication_key} output role 집합이 registry와 다릅니다."
        )
    artifacts = tuple(
        Artifact(
            byte_sha256=sha256_hex(output.payload),
            role=output.role,
            row_count=output.row_count,
            uri=content_addressed_uri(
                base_uri,
                publication_key=publication_key,
                category="outputs",
                name=output.role,
                payload=output.payload,
                suffix=output.suffix,
            ),
        )
        for output in output_values
    )
    artifact_set = build_artifact_set(artifacts)
    fingerprint = build_input_fingerprint(
        publication_key,
        dependencies,
        input_artifacts,
        parameters,
    )
    fingerprint_uri = content_addressed_uri(
        base_uri,
        publication_key=publication_key,
        category="fingerprints",
        name="input-fingerprint",
        payload=fingerprint.canonical_bytes,
        suffix="json",
    )

    for output, artifact in zip(output_values, artifacts, strict=True):
        object_store.put_once(
            artifact.uri,
            output.payload,
            expected_sha256=artifact.byte_sha256,
        )
    object_store.put_once(
        fingerprint_uri,
        fingerprint.canonical_bytes,
        expected_sha256=fingerprint.sha256,
        require_canonical_json=True,
    )
    return PublicationMaterials(artifact_set, fingerprint, fingerprint_uri)


def build_prepared_publication(
    *,
    base_uri: str,
    publication_key: str,
    logical_dttm: datetime,
    publisher_version: str,
    revision_no: int,
    target_row_counts: Mapping[str, int],
    materials: PublicationMaterials,
    conditional_empty_candidate: bool = False,
) -> PreparedPublication:
    """인증된 artifact·fingerprint와 revision으로 manifest-last 준비물을 만든다.

    ``conditional_empty_candidate``는 조건부 EMPTY manifest를 lock 전에
    구성하도록 허용할 뿐이다. 최종 근거는 ``publish_verified``가
    받는 locked callback에서 다시 증명해야 한다.
    """
    if type(conditional_empty_candidate) is not bool:
        raise ContractViolation("conditional_empty_candidate는 bool이어야 합니다.")
    manifest = build_publication_manifest(
        publication_key=publication_key,
        artifact_set=materials.artifact_set,
        input_fingerprint=materials.input_fingerprint,
        input_fingerprint_uri=materials.input_fingerprint_uri,
        logical_dttm=logical_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts=target_row_counts,
        conditional_empty_proven=conditional_empty_candidate,
    )
    manifest_uri = _manifest_uri(base_uri, manifest)
    return PreparedPublication(
        manifest=manifest,
        manifest_uri=manifest_uri,
        input_fingerprint=materials.input_fingerprint,
    )


def publish_verified(
    connection: Connection[Any],
    prepared_and_validators: Iterable[
        tuple[PreparedPublication, StagingEvidenceValidator]
    ],
    object_store: ImmutableObjectStore,
    mutate_targets: MutationCallback,
    *,
    validate_locked: LockedValidator | None = None,
    validate_conditional_empty: ConditionalEmptyValidator | None = None,
) -> PublicationExecution:
    """공통 evidence verifier와 transaction 실행기만 거쳐 publication을 게시한다."""
    from core.gold_publication import verify_publication_evidence

    pairs = tuple(prepared_and_validators)
    if not pairs:
        raise ContractViolation("게시할 prepared publication이 하나 이상 필요합니다.")
    evidence = tuple(
        verify_publication_evidence(prepared, object_store, validator)
        for prepared, validator in pairs
    )
    result = execute_publication(
        connection,
        evidence,
        mutate_targets,
        validate_locked=validate_locked,
        validate_conditional_empty=validate_conditional_empty,
    )
    return PublicationExecution(result=result, evidence=evidence)


def _manifest_uri(base_uri: str, manifest: PublicationManifest) -> str:
    """manifest canonical bytes의 content-addressed URI를 반환한다."""
    return content_addressed_uri(
        base_uri,
        publication_key=manifest.publication_key,
        category="manifests",
        name="publication",
        payload=manifest.canonical_bytes,
        suffix="json",
    )


def _require_path_segment(value: Any, name: str) -> str:
    """URI path 구성요소가 nonblank ASCII 안전 문자열인지 확인한다."""
    if (
        type(value) is not str
        or value in {".", ".."}
        or _SAFE_PATH_SEGMENT.fullmatch(value) is None
    ):
        raise ContractViolation(f"{name}은 안전한 단일 path segment여야 합니다.")
    return value


def _require_s3_base_or_object_uri(value: Any, name: str) -> str:
    """값이 bucket과 nonempty key를 가진 S3 URI인지 확인한다."""
    if (
        type(value) is not str
        or not value.startswith("s3://")
        or "?" in value
        or "#" in value
    ):
        raise ContractViolation(f"{name}은 s3:// URI여야 합니다.")
    remainder = value.removeprefix("s3://")
    bucket, separator, key = remainder.partition("/")
    if not bucket or not separator or not key or key.startswith("/"):
        raise ContractViolation(f"{name}은 bucket과 key를 모두 가져야 합니다.")
    if "//" in key or any(part in {"", ".", ".."} for part in key.split("/")):
        raise ContractViolation(f"{name}에 모호한 path를 사용할 수 없습니다.")
    return value


def _utc_datetime(value: Any, name: str) -> datetime:
    """timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation(f"{name}은 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)
