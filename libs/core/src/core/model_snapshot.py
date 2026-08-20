"""Inference가 고정해 읽는 immutable ML model snapshot 계약을 제공한다."""

from __future__ import annotations

import json
import re
import unicodedata
from collections.abc import Iterable
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, cast
from urllib.parse import urlsplit

from .gold_publication.canonical import (
    JsonValue,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from .gold_publication.contract import (
    ID_SET_SCHEMA_VERSION,
    IdSet,
    InputArtifact,
    build_id_set,
)
from .gold_publication.errors import ContractViolation

MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION = "ml-model-snapshot-manifest-v1"
"""Immutable model snapshot manifest의 schema version이다."""

MODEL_BUNDLE_IDENTITY_SCHEMA_VERSION = "ml-model-bundle-identity-v1"
"""Model version을 결정하는 versionless bundle identity schema다."""

MODEL_ARTIFACT_ROLES = (
    "booster_poisson",
    "booster_q10",
    "booster_q50",
    "booster_q90",
    "conformal_correction",
    "effective_profile",
    "metrics",
    "station_categories",
    "station_crosswalk",
)
"""Inference가 모델 하나에서 반드시 읽는 artifact role의 exact 집합이다."""

_ARTIFACT_EXTENSION_BY_ROLE = {
    "booster_poisson": "txt",
    "booster_q10": "txt",
    "booster_q50": "txt",
    "booster_q90": "txt",
    "conformal_correction": "json",
    "effective_profile": "json",
    "metrics": "json",
    "station_categories": "json",
    "station_crosswalk": "json",
}

STATION_CROSSWALK_SCHEMA_VERSION = "ml-station-crosswalk-v1"
"""Station number를 Gold ``sta_id``로 고정하는 crosswalk schema다."""

SERVING_FEATURE_PROFILE_KEYS = (
    "ROLLING_TICK_MINUTES",
    "ROLLING_WINDOW_MINUTES",
    "ROLLING_EMBARGO_MINUTES",
    "TARGET_HORIZON_MINUTES",
    "GRID_TICK_MINUTES",
    "TRAIN_ANCHOR_TICK_MINUTES",
    "HORIZON_COUNT",
)
"""Full effective profile에서 train-serve 의미를 결정하는 7-key다."""
_MANIFEST_KEYS = frozenset(
    {
        "artifacts",
        "effective_contract_version",
        "model_kind",
        "model_version",
        "schema_version",
        "support_sta_ids",
    }
)
_ARTIFACT_KEYS = frozenset({"byte_sha256", "role", "uri"})
_ID_SET_REF_KEYS = frozenset({"byte_sha256", "id_count", "schema_version", "uri"})
_CROSSWALK_KEYS = frozenset({"entries", "schema_version"})
_CROSSWALK_ENTRY_KEYS = frozenset({"sta_id", "station_no"})
_ROLE_PATTERN = re.compile(r"[a-z][a-z0-9_]*\Z")
_CONTENT_ADDRESSED_OBJECT = re.compile(
    r"sha256=(?P<checksum>[0-9a-f]{64})\.(?P<extension>[a-z0-9][a-z0-9._-]*)\Z"
)
_CONTENT_VERSION_PATTERN = re.compile(r"sha256:(?P<checksum>[0-9a-f]{64})\Z")
_MAX_SAFE_INTEGER = 2**53 - 1


class ModelSnapshotContractError(ContractViolation):
    """Model snapshot bytes 또는 typed 값이 계약을 위반했다."""


class ModelKind(StrEnum):
    """Gold demand가 결합하는 두 model 종류다."""

    RENTAL = "rental"
    RETURN = "return"


@dataclass(frozen=True, slots=True)
class StationCrosswalkEntry:
    """Model ``station_no`` 하나와 Gold ``sta_id`` 하나의 1:1 mapping이다."""

    station_no: int
    sta_id: str

    def __post_init__(self) -> None:
        """Station number와 Gold ID의 exact scalar contract를 검증한다."""
        _require_station_no(self.station_no, "crosswalk station_no")
        _require_sta_id(self.sta_id, "crosswalk sta_id")


@dataclass(frozen=True, slots=True)
class StationCrosswalk:
    """Immutable station number→Gold ID crosswalk canonical document다."""

    schema_version: str
    entries: tuple[StationCrosswalkEntry, ...]

    def __post_init__(self) -> None:
        """Schema, exact entry type, sort와 양쪽 1:1 mapping을 검증한다."""
        _require_exact_string(
            self.schema_version,
            STATION_CROSSWALK_SCHEMA_VERSION,
            "station crosswalk schema_version",
        )
        if type(self.entries) is not tuple:
            raise ModelSnapshotContractError(
                "station crosswalk entries는 tuple이어야 합니다."
            )
        _require_instances(
            self.entries,
            StationCrosswalkEntry,
            "station crosswalk entry",
        )
        if not self.entries:
            raise ModelSnapshotContractError(
                "station crosswalk는 한 개 이상의 mapping이 필요합니다."
            )
        station_nos = tuple(entry.station_no for entry in self.entries)
        sta_ids = tuple(entry.sta_id for entry in self.entries)
        if len(set(station_nos)) != len(station_nos):
            raise ModelSnapshotContractError(
                "station crosswalk station_no는 중복될 수 없습니다."
            )
        if len(set(sta_ids)) != len(sta_ids):
            raise ModelSnapshotContractError(
                "station crosswalk sta_id는 여러 station_no에 mapping될 수 없습니다."
            )
        if station_nos != tuple(sorted(station_nos)):
            raise ModelSnapshotContractError(
                "station crosswalk entries는 station_no 오름차순이어야 합니다."
            )

    @property
    def canonical_bytes(self) -> bytes:
        """Crosswalk의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_station_crosswalk_document(self))

    @property
    def sha256(self) -> str:
        """Crosswalk canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class ModelArtifact:
    """Model snapshot이 소유하는 content-addressed object 한 개다."""

    byte_sha256: str
    role: str
    uri: str

    def __post_init__(self) -> None:
        """Artifact role, checksum, URI와 확장자를 함께 검증한다."""
        _require_sha256(self.byte_sha256, "model artifact byte_sha256")
        _require_role(self.role, "model artifact role")
        expected_extension = _ARTIFACT_EXTENSION_BY_ROLE.get(self.role)
        if expected_extension is None:
            raise ModelSnapshotContractError(
                f"허용되지 않은 model artifact role입니다: {self.role}"
            )
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension=expected_extension,
        )


@dataclass(frozen=True, slots=True)
class IdSetArtifactRef:
    """Immutable ``gold-id-set-v1`` object의 exact identity다."""

    byte_sha256: str
    id_count: int
    schema_version: str
    uri: str

    def __post_init__(self) -> None:
        """ID set schema, count, checksum과 content-addressed URI를 검증한다."""
        _require_sha256(self.byte_sha256, "ID set byte_sha256")
        _require_nonnegative_integer(self.id_count, "ID set id_count")
        _require_exact_string(
            self.schema_version,
            ID_SET_SCHEMA_VERSION,
            "ID set schema_version",
        )
        validate_content_addressed_s3_uri(
            self.uri,
            self.byte_sha256,
            expected_extension="json",
        )


@dataclass(frozen=True, slots=True)
class ModelSnapshotManifest:
    """한 rental 또는 return 모델의 inference-ready bundle을 고정한다."""

    schema_version: str
    model_kind: ModelKind
    model_version: str
    effective_contract_version: str
    artifacts: tuple[ModelArtifact, ...]
    support_sta_ids: IdSetArtifactRef

    def __post_init__(self) -> None:
        """Manifest의 exact field type, role 집합과 URI 관계를 검증한다."""
        validate_model_snapshot_manifest(self)

    @property
    def canonical_bytes(self) -> bytes:
        """Manifest의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_manifest_document(self))

    @property
    def sha256(self) -> str:
        """Manifest canonical bytes의 lowercase SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


def build_id_set_artifact_ref(id_set: IdSet, uri: str) -> IdSetArtifactRef:
    """검증된 Gold ID set과 content-addressed URI를 immutable ref로 묶는다."""
    if type(id_set) is not IdSet:
        raise ModelSnapshotContractError("id_set은 exact IdSet이어야 합니다.")
    return IdSetArtifactRef(
        byte_sha256=id_set.sha256,
        id_count=len(id_set.ids),
        schema_version=id_set.schema_version,
        uri=uri,
    )


def canonical_station_categories_bytes(categories: Iterable[int]) -> bytes:
    """Model categorical order를 보존한 unique station number JSON bytes를 만든다."""
    values = _station_categories_tuple(categories)
    return canonical_json_bytes(list(values))


def parse_station_categories(payload: bytes) -> tuple[int, ...]:
    """Canonical station category JSON array를 exact integer tuple로 읽는다."""
    values = _require_array(
        parse_canonical_json(payload),
        "station categories",
    )
    return _station_categories_tuple(values)


def build_station_crosswalk(
    entries: Iterable[StationCrosswalkEntry],
) -> StationCrosswalk:
    """Crosswalk entry를 station number 순으로 정렬해 canonical document를 만든다."""
    values = tuple(entries)
    _require_instances(values, StationCrosswalkEntry, "station crosswalk entry")
    return StationCrosswalk(
        schema_version=STATION_CROSSWALK_SCHEMA_VERSION,
        entries=tuple(sorted(values, key=lambda entry: entry.station_no)),
    )


def parse_station_crosswalk(payload: bytes) -> StationCrosswalk:
    """Canonical bytes를 exact-key station crosswalk document로 파싱한다."""
    document = _require_exact_object(
        parse_canonical_json(payload),
        _CROSSWALK_KEYS,
        "station crosswalk",
    )
    entries = tuple(
        _parse_station_crosswalk_entry(value)
        for value in _require_array(document["entries"], "station crosswalk entries")
    )
    return StationCrosswalk(
        schema_version=_require_string(
            document["schema_version"],
            "station crosswalk schema_version",
        ),
        entries=entries,
    )


def build_model_support_sta_ids(
    station_categories: tuple[int, ...],
    station_crosswalk: StationCrosswalk,
) -> IdSet:
    """Model categories와 1:1 crosswalk에서 Gold support ID set을 파생한다."""
    if type(station_categories) is not tuple:
        raise ModelSnapshotContractError(
            "station_categories는 exact tuple이어야 합니다."
        )
    categories = _station_categories_tuple(station_categories)
    if type(station_crosswalk) is not StationCrosswalk:
        raise ModelSnapshotContractError(
            "station_crosswalk는 exact StationCrosswalk이어야 합니다."
        )
    by_number = {entry.station_no: entry.sta_id for entry in station_crosswalk.entries}
    missing = tuple(number for number in categories if number not in by_number)
    if missing:
        raise ModelSnapshotContractError(
            f"model station category에 crosswalk mapping이 없습니다: {missing}"
        )
    return build_id_set(by_number[number] for number in categories)


def derive_model_support_sta_ids(
    manifest: ModelSnapshotManifest,
    station_categories_payload: bytes,
    station_crosswalk_payload: bytes,
) -> IdSet:
    """Pinned category·crosswalk bytes에서 support를 재산출해 manifest와 결합한다."""
    validate_model_snapshot_manifest(manifest)
    categories_payload = _require_bytes(
        station_categories_payload,
        "station categories payload",
    )
    crosswalk_payload = _require_bytes(
        station_crosswalk_payload,
        "station crosswalk payload",
    )
    categories_artifact = _artifact_for_role(manifest, "station_categories")
    crosswalk_artifact = _artifact_for_role(manifest, "station_crosswalk")
    if sha256_hex(categories_payload) != categories_artifact.byte_sha256:
        raise ModelSnapshotContractError(
            "station categories payload SHA-256이 manifest artifact와 다릅니다."
        )
    if sha256_hex(crosswalk_payload) != crosswalk_artifact.byte_sha256:
        raise ModelSnapshotContractError(
            "station crosswalk payload SHA-256이 manifest artifact와 다릅니다."
        )
    derived = build_model_support_sta_ids(
        parse_station_categories(categories_payload),
        parse_station_crosswalk(crosswalk_payload),
    )
    if (
        derived.sha256 != manifest.support_sta_ids.byte_sha256
        or len(derived.ids) != manifest.support_sta_ids.id_count
    ):
        raise ModelSnapshotContractError(
            "categories∩crosswalk support ID set이 manifest support ref와 다릅니다."
        )
    return derived


def extract_serving_feature_contract_bytes(profile_payload: bytes) -> bytes:
    """Full effective profile bytes에서 serving-only 7-key canonical bytes를 추출한다."""
    payload = _require_bytes(profile_payload, "effective profile payload")
    try:
        decoded = payload.decode("utf-8")
        profile = json.loads(
            decoded,
            object_pairs_hook=_unique_json_object,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ModelSnapshotContractError(
            "effective profile은 duplicate key·nonfinite number 없는 UTF-8 JSON이어야 "
            "합니다."
        ) from exc
    if type(profile) is not dict:
        raise ModelSnapshotContractError(
            "effective profile의 top-level은 JSON object여야 합니다."
        )
    missing = tuple(key for key in SERVING_FEATURE_PROFILE_KEYS if key not in profile)
    if missing:
        raise ModelSnapshotContractError(
            f"effective profile에 serving contract key가 없습니다: {missing}"
        )
    subset: dict[str, JsonValue] = {}
    for key in SERVING_FEATURE_PROFILE_KEYS:
        subset[key] = _require_positive_integer(
            profile[key],
            f"effective profile {key}",
        )
    return canonical_json_bytes(subset)


def effective_contract_version_from_profile(profile_payload: bytes) -> str:
    """Full profile의 serving-only canonical subset SHA로 contract version을 만든다."""
    return (
        f"sha256:{sha256_hex(extract_serving_feature_contract_bytes(profile_payload))}"
    )


def validate_model_effective_contract_binding(
    manifest: ModelSnapshotManifest,
    effective_profile_payload: bytes,
) -> bytes:
    """Pinned full profile의 SHA와 serving subset version을 model manifest에 결합한다."""
    validate_model_snapshot_manifest(manifest)
    payload = _require_bytes(effective_profile_payload, "effective profile payload")
    profile_artifact = _artifact_for_role(manifest, "effective_profile")
    if sha256_hex(payload) != profile_artifact.byte_sha256:
        raise ModelSnapshotContractError(
            "effective profile payload SHA-256이 manifest artifact와 다릅니다."
        )
    contract_bytes = extract_serving_feature_contract_bytes(payload)
    expected_version = f"sha256:{sha256_hex(contract_bytes)}"
    if manifest.effective_contract_version != expected_version:
        raise ModelSnapshotContractError(
            "effective_contract_version은 full profile의 serving-only 7-key canonical "
            "subset SHA-256이어야 합니다."
        )
    return contract_bytes


def build_model_snapshot_manifest(
    *,
    model_kind: ModelKind,
    effective_contract_version: str,
    artifacts: Iterable[ModelArtifact],
    support_sta_ids: IdSetArtifactRef,
) -> ModelSnapshotManifest:
    """검증된 model bundle을 정렬하고 content-derived version으로 묶는다."""
    values = tuple(artifacts)
    _require_instances(values, ModelArtifact, "model artifact")
    ordered = tuple(sorted(values, key=lambda artifact: _utf8_key(artifact.role)))
    model_version = model_bundle_version(
        model_kind=model_kind,
        effective_contract_version=effective_contract_version,
        artifacts=ordered,
        support_sta_ids=support_sta_ids,
    )
    return ModelSnapshotManifest(
        schema_version=MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        model_kind=model_kind,
        model_version=model_version,
        effective_contract_version=effective_contract_version,
        artifacts=ordered,
        support_sta_ids=support_sta_ids,
    )


def model_bundle_version(
    *,
    model_kind: ModelKind,
    effective_contract_version: str,
    artifacts: Iterable[ModelArtifact],
    support_sta_ids: IdSetArtifactRef,
) -> str:
    """Versionless canonical bundle identity에서 model version을 파생한다."""
    values = tuple(artifacts)
    _require_instances(values, ModelArtifact, "model artifact")
    ordered = tuple(sorted(values, key=lambda artifact: _utf8_key(artifact.role)))
    _validate_model_bundle(
        model_kind=model_kind,
        effective_contract_version=effective_contract_version,
        artifacts=ordered,
        support_sta_ids=support_sta_ids,
    )
    identity_bytes = canonical_json_bytes(
        _bundle_identity_document(
            model_kind=model_kind,
            effective_contract_version=effective_contract_version,
            artifacts=ordered,
            support_sta_ids=support_sta_ids,
        )
    )
    return f"sha256:{sha256_hex(identity_bytes)}"


def parse_model_snapshot_manifest(payload: bytes) -> ModelSnapshotManifest:
    """Canonical bytes를 exact-key v1 model snapshot manifest로 파싱한다."""
    document = _require_exact_object(
        parse_canonical_json(payload),
        _MANIFEST_KEYS,
        "model snapshot manifest",
    )
    model_kind_text = _require_string(document["model_kind"], "model_kind")
    try:
        model_kind = ModelKind(model_kind_text)
    except ValueError as exc:
        raise ModelSnapshotContractError(
            "model_kind는 rental 또는 return이어야 합니다."
        ) from exc

    artifact_values = _require_array(document["artifacts"], "artifacts")
    artifacts = tuple(_parse_model_artifact(value) for value in artifact_values)
    return ModelSnapshotManifest(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        model_kind=model_kind,
        model_version=_require_string(document["model_version"], "model_version"),
        effective_contract_version=_require_string(
            document["effective_contract_version"],
            "effective_contract_version",
        ),
        artifacts=artifacts,
        support_sta_ids=_parse_id_set_ref(document["support_sta_ids"]),
    )


def validate_model_snapshot_manifest(manifest: ModelSnapshotManifest) -> None:
    """Typed model manifest의 schema, bundle과 support authority를 검증한다."""
    if type(manifest) is not ModelSnapshotManifest:
        raise ModelSnapshotContractError(
            "manifest는 exact ModelSnapshotManifest여야 합니다."
        )
    _require_exact_string(
        manifest.schema_version,
        MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
        "model snapshot schema_version",
    )
    if type(manifest.model_kind) is not ModelKind:
        raise ModelSnapshotContractError("model_kind는 exact ModelKind여야 합니다.")
    _require_content_version(manifest.model_version, "model_version")
    _validate_model_bundle(
        model_kind=manifest.model_kind,
        effective_contract_version=manifest.effective_contract_version,
        artifacts=manifest.artifacts,
        support_sta_ids=manifest.support_sta_ids,
    )
    expected_version = model_bundle_version(
        model_kind=manifest.model_kind,
        effective_contract_version=manifest.effective_contract_version,
        artifacts=manifest.artifacts,
        support_sta_ids=manifest.support_sta_ids,
    )
    if manifest.model_version != expected_version:
        raise ModelSnapshotContractError(
            "model_version은 versionless canonical bundle identity SHA-256이어야 "
            f"합니다: expected={expected_version}"
        )
    canonical_json_bytes(_manifest_document(manifest))


def model_manifest_input_artifact(
    manifest: ModelSnapshotManifest,
    uri: str,
) -> InputArtifact:
    """Model manifest bytes를 Gold demand fingerprint input으로 변환한다."""
    validate_model_snapshot_manifest(manifest)
    validate_content_addressed_s3_uri(
        uri,
        manifest.sha256,
        expected_extension="json",
    )
    return InputArtifact(
        byte_sha256=manifest.sha256,
        role=f"{manifest.model_kind.value}_model_manifest",
        uri=uri,
    )


def validate_content_addressed_s3_uri(
    uri: str,
    checksum: str,
    *,
    expected_extension: str | None = None,
) -> str:
    """URI가 checksum을 filename에 고정한 exact S3 object인지 검증한다."""
    _require_nonblank_nfc(uri, "artifact URI")
    digest = _require_sha256(checksum, "artifact checksum")
    if expected_extension is not None:
        _require_extension(expected_extension)
    if "?" in uri or "#" in uri or "\\" in uri or "%" in uri:
        raise ModelSnapshotContractError(
            "content-addressed S3 URI에는 query, fragment, escape를 쓸 수 없습니다."
        )
    try:
        parsed = urlsplit(uri)
    except ValueError as exc:
        raise ModelSnapshotContractError("S3 URI를 해석할 수 없습니다.") from exc
    segments = parsed.path.removeprefix("/").split("/")
    if (
        parsed.scheme != "s3"
        or not parsed.netloc
        or "@" in parsed.netloc
        or ":" in parsed.netloc
        or parsed.query
        or parsed.fragment
        or not segments
        or any(not segment or segment in {".", ".."} for segment in segments)
    ):
        raise ModelSnapshotContractError(
            "artifact URI는 canonical S3 object URI여야 합니다."
        )
    matched = _CONTENT_ADDRESSED_OBJECT.fullmatch(segments[-1])
    if matched is None or matched.group("checksum") != digest:
        raise ModelSnapshotContractError(
            "artifact URI filename은 byte SHA-256으로 content-addressed되어야 합니다."
        )
    if (
        expected_extension is not None
        and matched.group("extension") != expected_extension
    ):
        raise ModelSnapshotContractError(
            f"artifact URI 확장자는 .{expected_extension}이어야 합니다."
        )
    return uri


def _manifest_document(manifest: ModelSnapshotManifest) -> dict[str, JsonValue]:
    """Typed model manifest를 exact canonical JSON object로 바꾼다."""
    return {
        "artifacts": [
            {
                "byte_sha256": artifact.byte_sha256,
                "role": artifact.role,
                "uri": artifact.uri,
            }
            for artifact in manifest.artifacts
        ],
        "effective_contract_version": manifest.effective_contract_version,
        "model_kind": manifest.model_kind.value,
        "model_version": manifest.model_version,
        "schema_version": manifest.schema_version,
        "support_sta_ids": {
            "byte_sha256": manifest.support_sta_ids.byte_sha256,
            "id_count": manifest.support_sta_ids.id_count,
            "schema_version": manifest.support_sta_ids.schema_version,
            "uri": manifest.support_sta_ids.uri,
        },
    }


def _bundle_identity_document(
    *,
    model_kind: ModelKind,
    effective_contract_version: str,
    artifacts: tuple[ModelArtifact, ...],
    support_sta_ids: IdSetArtifactRef,
) -> dict[str, JsonValue]:
    """Model version의 자기참조 없는 canonical identity object를 만든다."""
    return {
        "artifacts": [
            {
                "byte_sha256": artifact.byte_sha256,
                "role": artifact.role,
                "uri": artifact.uri,
            }
            for artifact in artifacts
        ],
        "effective_contract_version": effective_contract_version,
        "model_kind": model_kind.value,
        "schema_version": MODEL_BUNDLE_IDENTITY_SCHEMA_VERSION,
        "support_sta_ids": {
            "byte_sha256": support_sta_ids.byte_sha256,
            "id_count": support_sta_ids.id_count,
            "schema_version": support_sta_ids.schema_version,
            "uri": support_sta_ids.uri,
        },
    }


def _station_crosswalk_document(
    crosswalk: StationCrosswalk,
) -> dict[str, JsonValue]:
    """Typed station crosswalk를 exact canonical JSON object로 바꾼다."""
    return {
        "entries": [
            {"sta_id": entry.sta_id, "station_no": entry.station_no}
            for entry in crosswalk.entries
        ],
        "schema_version": crosswalk.schema_version,
    }


def _validate_model_bundle(
    *,
    model_kind: ModelKind,
    effective_contract_version: str,
    artifacts: tuple[ModelArtifact, ...],
    support_sta_ids: IdSetArtifactRef,
) -> None:
    """Bundle의 exact role, immutable ref와 effective contract 결합을 검증한다."""
    if type(model_kind) is not ModelKind:
        raise ModelSnapshotContractError("model_kind는 exact ModelKind여야 합니다.")
    _require_content_version(
        effective_contract_version,
        "effective_contract_version",
    )
    if type(artifacts) is not tuple:
        raise ModelSnapshotContractError("artifacts는 tuple이어야 합니다.")
    _require_instances(artifacts, ModelArtifact, "model artifact")
    if type(support_sta_ids) is not IdSetArtifactRef:
        raise ModelSnapshotContractError(
            "support_sta_ids는 exact IdSetArtifactRef여야 합니다."
        )

    roles = tuple(artifact.role for artifact in artifacts)
    if roles != MODEL_ARTIFACT_ROLES:
        raise ModelSnapshotContractError(
            "model artifact role은 inference-required exact 집합이어야 합니다: "
            f"expected={MODEL_ARTIFACT_ROLES}, actual={roles}"
        )
    uris = tuple(artifact.uri for artifact in artifacts)
    if len(set(uris)) != len(uris):
        raise ModelSnapshotContractError("model artifact URI는 중복될 수 없습니다.")
    if support_sta_ids.uri in set(uris):
        raise ModelSnapshotContractError(
            "support ID set URI는 model artifact URI와 달라야 합니다."
        )


def _parse_station_crosswalk_entry(value: JsonValue) -> StationCrosswalkEntry:
    """JSON object를 exact StationCrosswalkEntry로 파싱한다."""
    document = _require_exact_object(
        value,
        _CROSSWALK_ENTRY_KEYS,
        "station crosswalk entry",
    )
    return StationCrosswalkEntry(
        station_no=_require_station_no(
            document["station_no"],
            "station crosswalk entry station_no",
        ),
        sta_id=_require_string(
            document["sta_id"],
            "station crosswalk entry sta_id",
        ),
    )


def _parse_model_artifact(value: JsonValue) -> ModelArtifact:
    """JSON object를 exact ModelArtifact로 파싱한다."""
    document = _require_exact_object(value, _ARTIFACT_KEYS, "model artifact")
    return ModelArtifact(
        byte_sha256=_require_string(
            document["byte_sha256"], "model artifact byte_sha256"
        ),
        role=_require_string(document["role"], "model artifact role"),
        uri=_require_string(document["uri"], "model artifact URI"),
    )


def _parse_id_set_ref(value: JsonValue) -> IdSetArtifactRef:
    """JSON object를 exact IdSetArtifactRef로 파싱한다."""
    document = _require_exact_object(value, _ID_SET_REF_KEYS, "support ID set")
    return IdSetArtifactRef(
        byte_sha256=_require_string(
            document["byte_sha256"], "support ID set byte_sha256"
        ),
        id_count=_require_nonnegative_integer(
            document["id_count"], "support ID set id_count"
        ),
        schema_version=_require_string(
            document["schema_version"], "support ID set schema_version"
        ),
        uri=_require_string(document["uri"], "support ID set URI"),
    )


def _require_exact_object(
    value: JsonValue,
    expected_keys: frozenset[str],
    label: str,
) -> dict[str, JsonValue]:
    """JSON 값이 exact builtin object와 key 집합인지 확인한다."""
    if type(value) is not dict:
        raise ModelSnapshotContractError(f"{label}는 JSON object여야 합니다.")
    document = cast(dict[str, JsonValue], value)
    actual_keys = frozenset(document)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys.difference(actual_keys))
        extra = sorted(actual_keys.difference(expected_keys))
        raise ModelSnapshotContractError(
            f"{label} key가 정확하지 않습니다: missing={missing}, extra={extra}"
        )
    return document


def _require_array(value: JsonValue, label: str) -> list[JsonValue]:
    """JSON 값이 exact builtin array인지 확인한다."""
    if type(value) is not list:
        raise ModelSnapshotContractError(f"{label}는 JSON array여야 합니다.")
    return cast(list[JsonValue], value)


def _require_string(value: JsonValue, label: str) -> str:
    """JSON 값이 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise ModelSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    return _require_nfc(value, label)


def _require_nonblank_nfc(value: Any, label: str) -> str:
    """값이 공백·제어 문자 없는 exact builtin NFC string인지 확인한다."""
    if type(value) is not str:
        raise ModelSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    normalized = _require_nfc(value, label)
    if (
        not normalized
        or normalized != normalized.strip()
        or any(
            ord(character) < 0x20 or ord(character) == 0x7F for character in normalized
        )
    ):
        raise ModelSnapshotContractError(
            f"{label}은 공백·제어 문자 없는 NFC 문자열이어야 합니다."
        )
    return normalized


def _require_nfc(value: str, label: str) -> str:
    """문자열이 surrogate·noncharacter 없는 NFC인지 확인한다."""
    if type(value) is not str:
        raise ModelSnapshotContractError(f"{label}은 문자열이어야 합니다.")
    if unicodedata.normalize("NFC", value) != value:
        raise ModelSnapshotContractError(f"{label}은 Unicode NFC여야 합니다.")
    for character in value:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise ModelSnapshotContractError(
                f"{label}에 Unicode surrogate를 쓸 수 없습니다."
            )
        if 0xFDD0 <= code_point <= 0xFDEF or code_point & 0xFFFF in {
            0xFFFE,
            0xFFFF,
        }:
            raise ModelSnapshotContractError(
                f"{label}에 Unicode noncharacter를 쓸 수 없습니다."
            )
    return value


def _require_exact_string(value: Any, expected: str, label: str) -> str:
    """값이 exact builtin string이며 contract 고정값과 같은지 확인한다."""
    if type(value) is not str or value != expected:
        raise ModelSnapshotContractError(f"{label}은 정확히 {expected!r}이어야 합니다.")
    return value


def _require_sha256(value: Any, label: str) -> str:
    """값이 exact lowercase SHA-256 string인지 확인한다."""
    if type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None:
        raise ModelSnapshotContractError(
            f"{label}은 정확히 64자리 lowercase hex여야 합니다."
        )
    return value


def _require_content_version(value: Any, label: str) -> str:
    """Version이 ``sha256:<lowercase digest>`` exact string인지 확인한다."""
    if type(value) is not str:
        raise ModelSnapshotContractError(
            f"{label}은 sha256:<64 lowercase hex> 문자열이어야 합니다."
        )
    matched = _CONTENT_VERSION_PATTERN.fullmatch(value)
    if matched is None:
        raise ModelSnapshotContractError(
            f"{label}은 sha256:<64 lowercase hex> 형식이어야 합니다."
        )
    return matched.group("checksum")


def _require_nonnegative_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 canonical-safe nonnegative builtin integer인지 확인한다."""
    if type(value) is not int or not 0 <= value <= _MAX_SAFE_INTEGER:
        raise ModelSnapshotContractError(
            f"{label}은 0 이상 {_MAX_SAFE_INTEGER} 이하 integer여야 합니다."
        )
    return value


def _require_positive_integer(value: Any, label: str) -> int:
    """값이 bool이 아닌 canonical-safe positive builtin integer인지 확인한다."""
    if type(value) is not int or not 1 <= value <= _MAX_SAFE_INTEGER:
        raise ModelSnapshotContractError(
            f"{label}은 1 이상 {_MAX_SAFE_INTEGER} 이하 integer여야 합니다."
        )
    return value


def _require_station_no(value: Any, label: str) -> int:
    """Station number가 bool이 아닌 nonnegative int16 범위인지 확인한다."""
    if type(value) is not int or not 0 <= value <= 32767:
        raise ModelSnapshotContractError(
            f"{label}은 0..32767 exact integer여야 합니다."
        )
    return value


def _require_sta_id(value: Any, label: str) -> str:
    """Gold station ID가 exact ``ST-[0-9]+`` NFC string인지 확인한다."""
    normalized = _require_nonblank_nfc(value, label)
    if re.fullmatch(r"ST-[0-9]+", normalized) is None:
        raise ModelSnapshotContractError(
            f"{label}은 ST- 뒤에 ASCII 숫자만 오는 형식이어야 합니다."
        )
    return normalized


def _station_categories_tuple(values: Iterable[Any]) -> tuple[int, ...]:
    """Category iterable을 order-preserving exact unique station number tuple로 검증한다."""
    if type(values) in {str, bytes}:
        raise ModelSnapshotContractError(
            "station categories는 scalar가 아닌 iterable이어야 합니다."
        )
    try:
        categories = tuple(values)
    except TypeError as exc:
        raise ModelSnapshotContractError(
            "station categories는 iterable이어야 합니다."
        ) from exc
    if not categories:
        raise ModelSnapshotContractError(
            "station categories는 한 개 이상이어야 합니다."
        )
    for value in categories:
        _require_station_no(value, "station category")
    if len(set(categories)) != len(categories):
        raise ModelSnapshotContractError(
            "station category station_no는 중복될 수 없습니다."
        )
    return cast(tuple[int, ...], categories)


def _artifact_for_role(
    manifest: ModelSnapshotManifest,
    role: str,
) -> ModelArtifact:
    """Exact-role manifest에서 지정한 artifact 하나를 반환한다."""
    return manifest.artifacts[MODEL_ARTIFACT_ROLES.index(role)]


def _require_bytes(value: Any, label: str) -> bytes:
    """값이 subclass가 아닌 exact bytes인지 확인한다."""
    if type(value) is not bytes:
        raise ModelSnapshotContractError(f"{label}는 exact bytes여야 합니다.")
    return value


def _unique_json_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    """Full profile JSON의 duplicate object key를 fail closed한다."""
    document: dict[str, Any] = {}
    for key, value in pairs:
        if key in document:
            raise ModelSnapshotContractError(
                f"effective profile에 duplicate JSON key가 있습니다: {key}"
            )
        document[key] = value
    return document


def _reject_json_constant(value: str) -> None:
    """Full profile JSON의 NaN·Infinity extension을 거부한다."""
    raise ModelSnapshotContractError(
        f"effective profile에 nonfinite JSON number를 쓸 수 없습니다: {value}"
    )


def _require_role(value: Any, label: str) -> str:
    """Role이 lowercase snake_case exact builtin string인지 확인한다."""
    if type(value) is not str or _ROLE_PATTERN.fullmatch(value) is None:
        raise ModelSnapshotContractError(f"{label}은 lowercase snake_case여야 합니다.")
    return value


def _require_extension(value: Any) -> str:
    """Expected extension이 URI contract에 안전한 exact string인지 확인한다."""
    if type(value) is not str or re.fullmatch(r"[a-z0-9][a-z0-9._-]*", value) is None:
        raise ModelSnapshotContractError("expected_extension이 유효하지 않습니다.")
    return value


def _require_instances(
    values: tuple[Any, ...],
    expected_type: type[Any],
    label: str,
) -> None:
    """Tuple 원소가 subclass가 아닌 exact dataclass인지 확인한다."""
    if any(type(value) is not expected_type for value in values):
        raise ModelSnapshotContractError(
            f"모든 {label} 값은 exact {expected_type.__name__}이어야 합니다."
        )


def _utf8_key(value: str) -> bytes:
    """Contract 배열 정렬에 쓰는 NFC string의 UTF-8 bytes를 반환한다."""
    return _require_nonblank_nfc(value, "sort key").encode("utf-8")
