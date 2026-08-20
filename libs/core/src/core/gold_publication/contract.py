"""Gold publication v1 문서와 key별 registry 계약을 정의한다."""

from __future__ import annotations

import unicodedata
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import StrEnum
from types import MappingProxyType
from typing import Any, cast

from .canonical import (
    canonical_json_bytes,
    format_utc_dttm,
    parse_canonical_json,
    parse_utc_dttm,
    sha256_hex,
    validate_sha256_hex,
)
from .documents import StationRealtimeWindowSet
from .errors import ContractViolation

ARTIFACT_SET_SCHEMA_VERSION = "gold-artifact-set-v1"
INPUT_FINGERPRINT_SCHEMA_VERSION = "gold-input-fingerprint-v1"
ID_SET_SCHEMA_VERSION = "gold-id-set-v1"
PUBLICATION_MANIFEST_SCHEMA_VERSION = "gold-publication-manifest-v1"
TARGET_SCHEMA_VERSION = "gold-postgis-v1"

EMPTY_ARTIFACT_SET_SHA256 = (
    "98f11969010a550c3b20fd37879e45ec1682b3b05d4c7a25e590a7f0874a4cdb"
)

_MAX_SAFE_INTEGER = 2**53 - 1
_ARTIFACT_SET_KEYS = frozenset(("artifacts", "schema_version"))
_ARTIFACT_KEYS = frozenset(("byte_sha256", "role", "row_count", "uri"))
_INPUT_FINGERPRINT_KEYS = frozenset(
    ("dependencies", "input_artifacts", "parameters", "schema_version")
)
_DEPENDENCY_KEYS = frozenset(
    (
        "artifact_set_sha256",
        "input_fingerprint_sha256",
        "logical_dttm",
        "manifest_uri",
        "publication_key",
        "revision_no",
    )
)
_INPUT_ARTIFACT_KEYS = frozenset(("byte_sha256", "role", "uri"))
_PARAMETER_KEYS = frozenset(("name", "value"))
_ID_SET_KEYS = frozenset(("ids", "schema_version"))
_PUBLICATION_MANIFEST_KEYS = frozenset(
    (
        "artifact_set_sha256",
        "artifacts",
        "input_fingerprint_schema",
        "input_fingerprint_sha256",
        "input_fingerprint_uri",
        "logical_dttm",
        "publication_key",
        "published_row_cnt",
        "publisher_version",
        "revision_no",
        "schema_version",
        "target_row_counts",
        "target_schema_version",
    )
)


def _require_nfc_string(value: Any, name: str) -> str:
    """값이 surrogate·noncharacter 없는 NFC 문자열인지 확인한다."""
    if type(value) is not str:
        raise ContractViolation(f"{name}은 문자열이어야 합니다.")
    for character in value:
        code_point = ord(character)
        if 0xD800 <= code_point <= 0xDFFF:
            raise ContractViolation(f"{name}에 Unicode surrogate를 사용할 수 없습니다.")
        if 0xFDD0 <= code_point <= 0xFDEF or code_point & 0xFFFF in {
            0xFFFE,
            0xFFFF,
        }:
            raise ContractViolation(
                f"{name}에 Unicode noncharacter를 사용할 수 없습니다."
            )
    if unicodedata.normalize("NFC", value) != value:
        raise ContractViolation(f"{name}은 Unicode NFC여야 합니다.")
    return value


def _require_nfc_nonblank(value: Any, name: str) -> str:
    """값이 NFC이며 공백뿐이지 않은 문자열인지 확인한다."""
    result = _require_nfc_string(value, name)
    if not result.strip():
        raise ContractViolation(f"{name}은 nonblank 문자열이어야 합니다.")
    return result


def _require_nonnegative_integer(value: Any, name: str) -> int:
    """값이 canonical JSON에서 안전한 0 이상 integer인지 확인한다."""
    if type(value) is not int:
        raise ContractViolation(f"{name}은 integer여야 합니다.")
    if not 0 <= value <= _MAX_SAFE_INTEGER:
        raise ContractViolation(
            f"{name}은 0 이상 {_MAX_SAFE_INTEGER} 이하의 integer여야 합니다."
        )
    return value


def _require_tuple(value: Any, name: str) -> None:
    """typed document의 배열 field가 immutable tuple인지 확인한다."""
    if type(value) is not tuple:
        raise ContractViolation(f"{name}은 tuple이어야 합니다.")


def _require_sorted_unique(
    values: tuple[Any, ...],
    *,
    key: Any,
    name: str,
) -> None:
    """tuple이 주어진 sort key로 엄격한 오름차순인지 확인한다."""
    keys = tuple(key(value) for value in values)
    if len(keys) != len(set(keys)):
        raise ContractViolation(f"중복 {name}은 허용하지 않습니다.")
    if keys != tuple(sorted(keys)):
        raise ContractViolation(f"{name} 배열이 contract 오름차순이 아닙니다.")


def _require_sorted_unique_strings(values: tuple[str, ...], name: str) -> None:
    """문자열 tuple이 NFC·nonblank·UTF-8 byte 오름차순인지 확인한다."""
    _require_tuple(values, name)
    for value in values:
        _require_nfc_nonblank(value, name)
    _require_sorted_unique(values, key=_utf8_key, name=name)


def _utf8_key(value: str) -> bytes:
    """SSOT 배열 정렬에 쓰는 NFC 문자열의 UTF-8 bytes를 반환한다."""
    _require_nfc_string(value, "sort key")
    return value.encode("utf-8")


class EmptyPolicy(StrEnum):
    """publication key가 정상 EMPTY를 허용하는 방식을 나타낸다."""

    FORBIDDEN = "forbidden"
    ALLOWED = "allowed"
    CONDITIONAL = "conditional"


@dataclass(frozen=True, slots=True)
class RoleCardinality:
    """input artifact role의 허용 개수와 조건을 정의한다."""

    role: str
    minimum: int = 1
    maximum: int = 1
    condition: str | None = None

    def __post_init__(self) -> None:
        """role cardinality 정의 자체를 검증한다."""
        _require_nfc_nonblank(self.role, "input artifact role")
        _require_nonnegative_integer(self.minimum, "role minimum")
        _require_nonnegative_integer(self.maximum, "role maximum")
        if self.minimum > self.maximum:
            raise ContractViolation(
                "input artifact role의 minimum이 maximum보다 큽니다."
            )
        if self.condition is not None:
            _require_nfc_nonblank(self.condition, "role condition")


@dataclass(frozen=True, slots=True)
class PublicationSpec:
    """publication key 하나의 정확한 입출력과 EMPTY 계약을 정의한다."""

    publication_key: str
    dependencies: tuple[str, ...]
    input_roles: tuple[RoleCardinality, ...]
    parameter_names: tuple[str, ...]
    output_targets: tuple[tuple[str, str], ...]
    representative_target: str
    empty_policy: EmptyPolicy
    conditional_empty_requirement: str | None = None

    def __post_init__(self) -> None:
        """registry 명세가 모호하거나 중복되지 않았는지 검증한다."""
        _require_nfc_nonblank(self.publication_key, "publication key")
        _require_sorted_unique_strings(self.dependencies, "dependency publication key")
        _require_sorted_unique_strings(self.parameter_names, "parameter name")
        roles = tuple(role.role for role in self.input_roles)
        _require_sorted_unique_strings(roles, "input artifact role")
        output_roles = tuple(role for role, _target in self.output_targets)
        _require_sorted_unique_strings(output_roles, "output artifact role")
        targets = tuple(target for _role, target in self.output_targets)
        if self.representative_target not in targets:
            raise ContractViolation("대표 target이 output target 목록에 없습니다.")
        if self.empty_policy is EmptyPolicy.CONDITIONAL:
            if self.conditional_empty_requirement is None:
                raise ContractViolation("조건부 EMPTY에는 검증 조건 설명이 필요합니다.")
            _require_nfc_nonblank(
                self.conditional_empty_requirement,
                "conditional EMPTY requirement",
            )
        elif self.conditional_empty_requirement is not None:
            raise ContractViolation(
                "조건부가 아닌 EMPTY 정책에는 조건을 둘 수 없습니다."
            )

    @property
    def output_roles(self) -> tuple[str, ...]:
        """정확한 output artifact role 목록을 반환한다."""
        return tuple(role for role, _target in self.output_targets)

    @property
    def target_tables(self) -> tuple[str, ...]:
        """정확한 target row count key 목록을 반환한다."""
        return tuple(
            sorted({target for _role, target in self.output_targets}, key=_utf8_key)
        )


@dataclass(frozen=True, slots=True)
class Artifact:
    """publication output artifact 한 개를 표현한다."""

    byte_sha256: str
    role: str
    row_count: int
    uri: str

    def __post_init__(self) -> None:
        """artifact field를 byte contract에 맞게 검증한다."""
        validate_sha256_hex(self.byte_sha256)
        _require_nfc_nonblank(self.role, "artifact role")
        _require_nonnegative_integer(self.row_count, "artifact row_count")
        _require_nfc_nonblank(self.uri, "artifact URI")


@dataclass(frozen=True, slots=True)
class ArtifactSet:
    """정렬된 output artifact 집합 문서를 표현한다."""

    schema_version: str
    artifacts: tuple[Artifact, ...]

    def __post_init__(self) -> None:
        """schema version과 artifact tuple의 순서·중복을 검증한다."""
        _require_exact_value(
            self.schema_version,
            ARTIFACT_SET_SCHEMA_VERSION,
            "artifact set schema_version",
        )
        _require_tuple(self.artifacts, "artifact set artifacts")
        _require_instances(self.artifacts, Artifact, "artifact")
        _require_sorted_unique(
            self.artifacts,
            key=lambda artifact: (_utf8_key(artifact.role), _utf8_key(artifact.uri)),
            name="artifact (role, uri)",
        )

    @property
    def canonical_bytes(self) -> bytes:
        """artifact set의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_artifact_set_document(self))

    @property
    def sha256(self) -> str:
        """artifact set canonical bytes의 SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class Dependency:
    """Gold 선행 publication state 6-tuple을 표현한다."""

    artifact_set_sha256: str
    input_fingerprint_sha256: str
    logical_dttm: datetime
    manifest_uri: str
    publication_key: str
    revision_no: int

    def __post_init__(self) -> None:
        """dependency field를 검증하고 시각을 UTC로 정규화한다."""
        validate_sha256_hex(self.artifact_set_sha256)
        validate_sha256_hex(self.input_fingerprint_sha256)
        object.__setattr__(self, "logical_dttm", _utc_dttm(self.logical_dttm))
        _require_nfc_nonblank(self.manifest_uri, "dependency manifest URI")
        _require_nfc_nonblank(self.publication_key, "dependency publication key")
        _require_nonnegative_integer(self.revision_no, "dependency revision_no")


@dataclass(frozen=True, slots=True)
class InputArtifact:
    """input fingerprint가 고정하는 upstream artifact를 표현한다."""

    byte_sha256: str
    role: str
    uri: str

    def __post_init__(self) -> None:
        """input artifact field를 byte contract에 맞게 검증한다."""
        validate_sha256_hex(self.byte_sha256)
        _require_nfc_nonblank(self.role, "input artifact role")
        _require_nfc_nonblank(self.uri, "input artifact URI")


@dataclass(frozen=True, slots=True)
class Parameter:
    """계산 결과에 영향을 주는 이름·문자열 값을 표현한다."""

    name: str
    value: str

    def __post_init__(self) -> None:
        """parameter 이름과 값이 NFC 문자열인지 검증한다."""
        _require_nfc_nonblank(self.name, "parameter name")
        _require_nfc_string(self.value, "parameter value")


@dataclass(frozen=True, slots=True)
class InputFingerprint:
    """dependency·input artifact·parameter 입력 문서를 표현한다."""

    schema_version: str
    dependencies: tuple[Dependency, ...]
    input_artifacts: tuple[InputArtifact, ...]
    parameters: tuple[Parameter, ...]

    def __post_init__(self) -> None:
        """입력 배열의 schema version과 정확한 정렬·중복을 검증한다."""
        _require_exact_value(
            self.schema_version,
            INPUT_FINGERPRINT_SCHEMA_VERSION,
            "input fingerprint schema_version",
        )
        _require_tuple(self.dependencies, "dependencies")
        _require_tuple(self.input_artifacts, "input_artifacts")
        _require_tuple(self.parameters, "parameters")
        _require_instances(self.dependencies, Dependency, "dependency")
        _require_instances(self.input_artifacts, InputArtifact, "input artifact")
        _require_instances(self.parameters, Parameter, "parameter")
        _require_sorted_unique(
            self.dependencies,
            key=lambda dependency: _utf8_key(dependency.publication_key),
            name="dependency publication_key",
        )
        _require_sorted_unique(
            self.input_artifacts,
            key=lambda artifact: (_utf8_key(artifact.role), _utf8_key(artifact.uri)),
            name="input artifact (role, uri)",
        )
        _require_sorted_unique(
            self.parameters,
            key=lambda parameter: _utf8_key(parameter.name),
            name="parameter name",
        )

    @property
    def canonical_bytes(self) -> bytes:
        """input fingerprint의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_input_fingerprint_document(self))

    @property
    def sha256(self) -> str:
        """input fingerprint canonical bytes의 SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class IdSet:
    """정렬되고 중복 없는 Gold ID 집합 문서를 표현한다."""

    schema_version: str
    ids: tuple[str, ...]

    def __post_init__(self) -> None:
        """ID가 NFC·nonblank이며 UTF-8 byte 순서인지 검증한다."""
        _require_exact_value(
            self.schema_version, ID_SET_SCHEMA_VERSION, "ID set schema_version"
        )
        _require_tuple(self.ids, "ID set ids")
        for identifier in self.ids:
            _require_nfc_nonblank(identifier, "ID")
        _require_sorted_unique(self.ids, key=_utf8_key, name="ID")

    @property
    def canonical_bytes(self) -> bytes:
        """ID set의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_id_set_document(self))

    @property
    def sha256(self) -> str:
        """ID set canonical bytes의 SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


@dataclass(frozen=True, slots=True)
class PublicationManifest:
    """Gold publication manifest의 13개 typed field를 표현한다."""

    artifact_set_sha256: str
    artifacts: tuple[Artifact, ...]
    input_fingerprint_schema: str
    input_fingerprint_sha256: str
    input_fingerprint_uri: str
    logical_dttm: datetime
    publication_key: str
    published_row_cnt: int
    publisher_version: str
    revision_no: int
    schema_version: str
    target_row_counts: Mapping[str, int]
    target_schema_version: str

    def __post_init__(self) -> None:
        """manifest field를 고정하고 registry와 artifact hash를 검증한다."""
        _require_tuple(self.artifacts, "manifest artifacts")
        _require_instances(self.artifacts, Artifact, "artifact")
        if not isinstance(self.target_row_counts, Mapping):
            raise ContractViolation("target_row_counts는 mapping이어야 합니다.")
        frozen_counts = MappingProxyType(dict(self.target_row_counts))
        object.__setattr__(self, "target_row_counts", frozen_counts)
        object.__setattr__(self, "logical_dttm", _utc_dttm(self.logical_dttm))
        validate_publication_manifest(self)

    @property
    def canonical_bytes(self) -> bytes:
        """publication manifest의 canonical JSON bytes를 반환한다."""
        return canonical_json_bytes(_publication_manifest_document(self))

    @property
    def sha256(self) -> str:
        """publication manifest canonical bytes의 SHA-256을 반환한다."""
        return sha256_hex(self.canonical_bytes)


def _role(
    role: str,
    minimum: int = 1,
    maximum: int = 1,
    condition: str | None = None,
) -> RoleCardinality:
    """간결한 immutable role cardinality를 만든다."""
    return RoleCardinality(role, minimum, maximum, condition)


_REGISTRY = {
    "weather_grid": PublicationSpec(
        publication_key="weather_grid",
        dependencies=(),
        input_roles=(_role("weather_grid_seed"),),
        parameter_names=("expected_grid_count", "grid_seed_version"),
        output_targets=(("weather_grid", "weather_grid"),),
        representative_target="weather_grid",
        empty_policy=EmptyPolicy.FORBIDDEN,
    ),
    "dispatch_center": PublicationSpec(
        publication_key="dispatch_center",
        dependencies=(),
        input_roles=(_role("dispatch_center_seed"),),
        parameter_names=("center_seed_version", "expected_center_count"),
        output_targets=(("dispatch_center", "dispatch_center"),),
        representative_target="dispatch_center",
        empty_policy=EmptyPolicy.FORBIDDEN,
    ),
    "station": PublicationSpec(
        publication_key="station",
        dependencies=("dispatch_center", "weather_grid"),
        input_roles=(
            _role("bike_station_master_manifest"),
            _role(
                "station_previous_projection",
                0,
                1,
                "previous_station_state_exists",
            ),
            _role("station_realtime_window_set"),
            _role(
                "station_relocation_approval",
                0,
                1,
                "approved_over_100m_relocation_is_applied",
            ),
        ),
        parameter_names=(
            "center_assignment_version",
            "grid_conversion_version",
            "station_policy_version",
        ),
        output_targets=(("station", "station"),),
        representative_target="station",
        empty_policy=EmptyPolicy.FORBIDDEN,
    ),
    "station_stock": PublicationSpec(
        publication_key="station_stock",
        dependencies=(),
        input_roles=(_role("bike_station_realtime_manifest"),),
        parameter_names=("station_stock_policy_version",),
        output_targets=(("station_stock", "station_stock"),),
        representative_target="station_stock",
        empty_policy=EmptyPolicy.FORBIDDEN,
    ),
    "station_demand_forecast": PublicationSpec(
        publication_key="station_demand_forecast",
        dependencies=("station",),
        input_roles=(
            _role("inference_output"),
            _role("rental_model_manifest"),
            _role("return_model_manifest"),
        ),
        parameter_names=("expected_sta_id_sha256", "horizon_count", "rounding_mode"),
        output_targets=(("station_demand_forecast", "station_demand_forecast"),),
        representative_target="station_demand_forecast",
        empty_policy=EmptyPolicy.CONDITIONAL,
        conditional_empty_requirement=(
            "active_station_and_both_model_support_intersection_is_empty"
        ),
    ),
    "weather_forecast": PublicationSpec(
        publication_key="weather_forecast",
        dependencies=("station", "weather_grid"),
        input_roles=(
            _role("short_term_manifest"),
            _role("ultra_short_manifest"),
        ),
        parameter_names=("forecast_hour_count", "resolver_version"),
        output_targets=(("weather_forecast", "weather_forecast"),),
        representative_target="weather_forecast",
        empty_policy=EmptyPolicy.CONDITIONAL,
        conditional_empty_requirement="active_weather_grid_set_is_empty",
    ),
    "event:cultural_event": PublicationSpec(
        publication_key="event:cultural_event",
        dependencies=(),
        input_roles=(_role("cultural_event_manifest"),),
        parameter_names=("event_identity_version", "event_policy_version"),
        output_targets=(("event_cultural_event", "event"),),
        representative_target="event",
        empty_policy=EmptyPolicy.ALLOWED,
    ),
    "event:performance_event": PublicationSpec(
        publication_key="event:performance_event",
        dependencies=(),
        input_roles=(
            _role("performance_event_manifest"),
            _role("stadium_coordinate_seed"),
        ),
        parameter_names=("event_policy_version", "stadium_coordinate_version"),
        output_targets=(("event_performance_event", "event"),),
        representative_target="event",
        empty_policy=EmptyPolicy.ALLOWED,
    ),
    "station_urgency": PublicationSpec(
        publication_key="station_urgency",
        dependencies=("station", "station_demand_forecast", "station_stock"),
        input_roles=(
            _role("demand_publication_manifest"),
            _role("stock_history_manifest_01"),
            _role("stock_history_manifest_02"),
            _role("stock_history_manifest_03"),
            _role("stock_history_manifest_04"),
            _role("stock_history_manifest_05"),
            _role("stock_publication_manifest"),
            _role("urgency_output"),
        ),
        parameter_names=(
            "expected_sta_id_sha256",
            "scoring_config_version",
            "stock_window_count",
        ),
        output_targets=(("station_urgency", "station_urgency"),),
        representative_target="station_urgency",
        empty_policy=EmptyPolicy.CONDITIONAL,
        conditional_empty_requirement="expected_station_id_set_is_empty",
    ),
    "rebalance_route": PublicationSpec(
        publication_key="rebalance_route",
        dependencies=(
            "dispatch_center",
            "station",
            "station_demand_forecast",
            "station_stock",
            "station_urgency",
        ),
        input_roles=(
            _role("route_coverage"),
            _role("urgency_publication_manifest"),
        ),
        parameter_names=(
            "route_algorithm_version",
            "route_coverage_sha256",
            "truck_capacity",
            "truck_capacity_config_version",
        ),
        output_targets=(
            ("route_stops", "rebalance_route_stop"),
            ("routes", "rebalance_route"),
        ),
        representative_target="rebalance_route",
        empty_policy=EmptyPolicy.ALLOWED,
    ),
}

PUBLICATION_REGISTRY: Mapping[str, PublicationSpec] = MappingProxyType(_REGISTRY)

_MANIFEST_ROLE_DEPENDENCY = MappingProxyType(
    {
        "station_urgency": {
            "demand_publication_manifest": "station_demand_forecast",
            "stock_publication_manifest": "station_stock",
        },
        "rebalance_route": {
            "urgency_publication_manifest": "station_urgency",
        },
    }
)


def get_publication_spec(publication_key: str) -> PublicationSpec:
    """등록된 publication key의 immutable 명세를 반환한다."""
    _require_nfc_nonblank(publication_key, "publication key")
    try:
        return PUBLICATION_REGISTRY[publication_key]
    except KeyError as exc:
        raise ContractViolation(
            f"등록되지 않은 publication key입니다: {publication_key}"
        ) from exc


def build_artifact_set(artifacts: Iterable[Artifact]) -> ArtifactSet:
    """artifact를 `(role, uri)` UTF-8 byte 순서로 정렬해 집합을 만든다."""
    values = tuple(artifacts)
    _require_instances(values, Artifact, "artifact")
    ordered = tuple(
        sorted(
            values,
            key=lambda artifact: (_utf8_key(artifact.role), _utf8_key(artifact.uri)),
        )
    )
    return ArtifactSet(ARTIFACT_SET_SCHEMA_VERSION, ordered)


def parse_artifact_set(payload: bytes) -> ArtifactSet:
    """canonical artifact set bytes를 exact-key typed 문서로 파싱한다."""
    document = _parse_object(payload, _ARTIFACT_SET_KEYS, "artifact set")
    artifacts_value = _require_array(document["artifacts"], "artifact set artifacts")
    artifacts = tuple(_parse_artifact(value) for value in artifacts_value)
    return ArtifactSet(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        artifacts=artifacts,
    )


def build_input_fingerprint(
    publication_key: str,
    dependencies: Iterable[Dependency],
    input_artifacts: Iterable[InputArtifact],
    parameters: Iterable[Parameter],
) -> InputFingerprint:
    """입력을 canonical 순서로 정렬하고 key별 exact registry를 검증한다."""
    dependency_values = tuple(dependencies)
    artifact_values = tuple(input_artifacts)
    parameter_values = tuple(parameters)
    _require_instances(dependency_values, Dependency, "dependency")
    _require_instances(artifact_values, InputArtifact, "input artifact")
    _require_instances(parameter_values, Parameter, "parameter")
    fingerprint = InputFingerprint(
        schema_version=INPUT_FINGERPRINT_SCHEMA_VERSION,
        dependencies=tuple(
            sorted(
                dependency_values, key=lambda value: _utf8_key(value.publication_key)
            )
        ),
        input_artifacts=tuple(
            sorted(
                artifact_values,
                key=lambda value: (_utf8_key(value.role), _utf8_key(value.uri)),
            )
        ),
        parameters=tuple(
            sorted(parameter_values, key=lambda value: _utf8_key(value.name))
        ),
    )
    validate_input_fingerprint(publication_key, fingerprint)
    return fingerprint


def parse_input_fingerprint(payload: bytes, publication_key: str) -> InputFingerprint:
    """canonical input fingerprint bytes를 파싱하고 key별 입력 집합을 검증한다."""
    document = _parse_object(payload, _INPUT_FINGERPRINT_KEYS, "input fingerprint")
    dependencies = tuple(
        _parse_dependency(value)
        for value in _require_array(document["dependencies"], "dependencies")
    )
    input_artifacts = tuple(
        _parse_input_artifact(value)
        for value in _require_array(document["input_artifacts"], "input_artifacts")
    )
    parameters = tuple(
        _parse_parameter(value)
        for value in _require_array(document["parameters"], "parameters")
    )
    fingerprint = InputFingerprint(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        dependencies=dependencies,
        input_artifacts=input_artifacts,
        parameters=parameters,
    )
    validate_input_fingerprint(publication_key, fingerprint)
    return fingerprint


def validate_input_fingerprint(
    publication_key: str,
    fingerprint: InputFingerprint,
) -> None:
    """fingerprint의 dependency·role·parameter 집합을 registry와 대조한다."""
    if type(fingerprint) is not InputFingerprint:
        raise ContractViolation("fingerprint는 InputFingerprint여야 합니다.")
    _require_exact_value(
        fingerprint.schema_version,
        INPUT_FINGERPRINT_SCHEMA_VERSION,
        "input fingerprint schema_version",
    )
    _require_tuple(fingerprint.dependencies, "dependencies")
    _require_tuple(fingerprint.input_artifacts, "input_artifacts")
    _require_tuple(fingerprint.parameters, "parameters")
    _require_instances(fingerprint.dependencies, Dependency, "dependency")
    _require_instances(
        fingerprint.input_artifacts,
        InputArtifact,
        "input artifact",
    )
    _require_instances(fingerprint.parameters, Parameter, "parameter")
    InputFingerprint(
        fingerprint.schema_version,
        tuple(
            Dependency(
                dependency.artifact_set_sha256,
                dependency.input_fingerprint_sha256,
                dependency.logical_dttm,
                dependency.manifest_uri,
                dependency.publication_key,
                dependency.revision_no,
            )
            for dependency in fingerprint.dependencies
        ),
        tuple(
            InputArtifact(
                artifact.byte_sha256,
                artifact.role,
                artifact.uri,
            )
            for artifact in fingerprint.input_artifacts
        ),
        tuple(
            Parameter(parameter.name, parameter.value)
            for parameter in fingerprint.parameters
        ),
    )
    spec = get_publication_spec(publication_key)
    actual_dependencies = tuple(
        dependency.publication_key for dependency in fingerprint.dependencies
    )
    if actual_dependencies != spec.dependencies:
        raise ContractViolation(
            f"{publication_key} dependency 집합이 registry와 다릅니다: "
            f"expected={spec.dependencies}, actual={actual_dependencies}"
        )

    role_counts = Counter(artifact.role for artifact in fingerprint.input_artifacts)
    expected_roles = {cardinality.role for cardinality in spec.input_roles}
    extra_roles = set(role_counts).difference(expected_roles)
    if extra_roles:
        raise ContractViolation(
            f"{publication_key}에 허용되지 않은 input artifact role입니다: "
            f"{sorted(extra_roles, key=_utf8_key)}"
        )
    for cardinality in spec.input_roles:
        count = role_counts[cardinality.role]
        if not cardinality.minimum <= count <= cardinality.maximum:
            raise ContractViolation(
                f"{publication_key} input artifact role cardinality가 다릅니다: "
                f"role={cardinality.role}, expected={cardinality.minimum}.."
                f"{cardinality.maximum}, actual={count}"
            )

    actual_parameters = tuple(parameter.name for parameter in fingerprint.parameters)
    if actual_parameters != spec.parameter_names:
        raise ContractViolation(
            f"{publication_key} parameter 집합이 registry와 다릅니다: "
            f"expected={spec.parameter_names}, actual={actual_parameters}"
        )
    _validate_hash_parameters(publication_key, fingerprint)
    _validate_dependency_manifest_uris(publication_key, fingerprint)


def validate_id_set_parameter(
    publication_key: str,
    fingerprint: InputFingerprint,
    expected_ids: IdSet,
) -> None:
    """lock 안에서 만든 ID set digest를 fingerprint parameter와 대조한다."""
    if type(expected_ids) is not IdSet:
        raise ContractViolation("expected_ids는 IdSet이어야 합니다.")
    IdSet(expected_ids.schema_version, expected_ids.ids)
    if publication_key not in {"station_demand_forecast", "station_urgency"}:
        raise ContractViolation(
            f"{publication_key}에는 expected_sta_id_sha256 parameter가 없습니다."
        )
    validate_input_fingerprint(publication_key, fingerprint)
    parameters = {
        parameter.name: parameter.value for parameter in fingerprint.parameters
    }
    if parameters["expected_sta_id_sha256"] != expected_ids.sha256:
        raise ContractViolation(
            "expected_sta_id_sha256이 lock 안에서 만든 gold-id-set-v1 digest와 다릅니다."
        )


def validate_station_conditional_inputs(
    fingerprint: InputFingerprint,
    *,
    previous_state_exists: bool,
    relocation_applied: bool,
) -> None:
    """station 조건부 input role을 실제 state·반영 여부와 대조한다."""
    if type(previous_state_exists) is not bool or type(relocation_applied) is not bool:
        raise ContractViolation("station 조건부 입력 근거는 bool이어야 합니다.")
    validate_input_fingerprint("station", fingerprint)
    role_counts = Counter(artifact.role for artifact in fingerprint.input_artifacts)
    expected_previous = 1 if previous_state_exists else 0
    expected_approval = 1 if relocation_applied else 0
    if role_counts["station_previous_projection"] != expected_previous:
        raise ContractViolation(
            "station_previous_projection cardinality가 현재 station state와 다릅니다."
        )
    if role_counts["station_relocation_approval"] != expected_approval:
        raise ContractViolation(
            "station_relocation_approval cardinality가 실제 relocation 반영 여부와 다릅니다."
        )


def validate_linked_dependency_manifests(
    publication_key: str,
    fingerprint: InputFingerprint,
    payload_by_role: Mapping[str, bytes],
) -> Mapping[str, PublicationManifest]:
    """동명 input manifest의 실제 bytes와 dependency tuple 결합을 검증한다."""
    validate_input_fingerprint(publication_key, fingerprint)
    bindings = _MANIFEST_ROLE_DEPENDENCY.get(publication_key, {})
    if set(payload_by_role) != set(bindings):
        raise ContractViolation(
            f"linked manifest payload role이 정확하지 않습니다: "
            f"expected={sorted(bindings)}, actual={sorted(payload_by_role)}"
        )

    dependencies = {
        dependency.publication_key: dependency
        for dependency in fingerprint.dependencies
    }
    artifacts = {artifact.role: artifact for artifact in fingerprint.input_artifacts}
    parsed: dict[str, PublicationManifest] = {}
    for role, dependency_key in bindings.items():
        payload = payload_by_role[role]
        if type(payload) is not bytes:
            raise ContractViolation(f"{role} manifest payload는 bytes여야 합니다.")
        artifact = artifacts[role]
        if sha256_hex(payload) != artifact.byte_sha256:
            raise ContractViolation(f"{role} 실제 manifest bytes SHA가 다릅니다.")
        manifest = parse_publication_manifest(payload)
        _validate_manifest_dependency_tuple(manifest, dependencies[dependency_key])
        parsed[role] = manifest
    return MappingProxyType(parsed)


def validate_route_urgency_dependencies(
    route_fingerprint: InputFingerprint,
    urgency_fingerprint: InputFingerprint,
) -> None:
    """route와 urgency fingerprint의 station·demand·stock tuple을 대조한다."""
    validate_input_fingerprint("rebalance_route", route_fingerprint)
    validate_input_fingerprint("station_urgency", urgency_fingerprint)
    route_dependencies = {
        dependency.publication_key: dependency
        for dependency in route_fingerprint.dependencies
    }
    urgency_dependencies = {
        dependency.publication_key: dependency
        for dependency in urgency_fingerprint.dependencies
    }
    for dependency_key in (
        "station",
        "station_demand_forecast",
        "station_stock",
    ):
        if route_dependencies[dependency_key] != urgency_dependencies[dependency_key]:
            raise ContractViolation(
                "route fingerprint와 urgency nested dependency tuple이 다릅니다: "
                f"{dependency_key}"
            )


def validate_station_stock_release(
    station_fingerprint: InputFingerprint,
    stock_fingerprint: InputFingerprint,
    window_set: StationRealtimeWindowSet,
) -> None:
    """station·stock이 같은 window-set candidate manifest를 사용했는지 검증한다."""
    validate_input_fingerprint("station", station_fingerprint)
    validate_input_fingerprint("station_stock", stock_fingerprint)
    if type(window_set) is not StationRealtimeWindowSet:
        raise ContractViolation("window_set은 StationRealtimeWindowSet이어야 합니다.")

    station_artifacts = {
        artifact.role: artifact for artifact in station_fingerprint.input_artifacts
    }
    window_artifact = station_artifacts["station_realtime_window_set"]
    if window_artifact.byte_sha256 != window_set.sha256:
        raise ContractViolation(
            "station_realtime_window_set 실제 canonical bytes SHA가 fingerprint와 다릅니다."
        )

    stock_artifacts = {
        artifact.role: artifact for artifact in stock_fingerprint.input_artifacts
    }
    stock_manifest = stock_artifacts["bike_station_realtime_manifest"]
    candidate = window_set.windows[0]
    if (stock_manifest.uri, stock_manifest.byte_sha256) != (
        candidate.uri,
        candidate.byte_sha256,
    ):
        raise ContractViolation(
            "station_stock realtime manifest가 station window-set 첫 candidate와 다릅니다."
        )


def build_id_set(ids: Iterable[str]) -> IdSet:
    """NFC·nonblank ID를 UTF-8 byte 순서로 정렬해 중복 없는 집합을 만든다."""
    if type(ids) in {str, bytes}:
        raise ContractViolation(
            "ID set 입력은 scalar 문자열이 아닌 iterable이어야 합니다."
        )
    values = tuple(ids)
    for identifier in values:
        _require_nfc_nonblank(identifier, "ID")
    ordered = tuple(sorted(values, key=_utf8_key))
    return IdSet(ID_SET_SCHEMA_VERSION, ordered)


def parse_id_set(payload: bytes) -> IdSet:
    """canonical ID set bytes를 exact-key typed 문서로 파싱한다."""
    document = _parse_object(payload, _ID_SET_KEYS, "ID set")
    identifiers = tuple(
        _require_string(value, "ID")
        for value in _require_array(document["ids"], "ID set ids")
    )
    return IdSet(
        schema_version=_require_string(document["schema_version"], "schema_version"),
        ids=identifiers,
    )


def build_publication_manifest(
    *,
    publication_key: str,
    artifact_set: ArtifactSet,
    input_fingerprint: InputFingerprint,
    input_fingerprint_uri: str,
    logical_dttm: datetime,
    publisher_version: str,
    revision_no: int,
    target_row_counts: Mapping[str, int],
    conditional_empty_proven: bool = False,
) -> PublicationManifest:
    """검증된 문서에서 13-key publication manifest를 결정적으로 만든다."""
    if type(artifact_set) is not ArtifactSet:
        raise ContractViolation("artifact_set은 ArtifactSet이어야 합니다.")
    validate_input_fingerprint(publication_key, input_fingerprint)
    spec = get_publication_spec(publication_key)
    published_row_cnt = _representative_row_count(spec, target_row_counts)
    manifest = PublicationManifest(
        artifact_set_sha256=artifact_set.sha256,
        artifacts=artifact_set.artifacts,
        input_fingerprint_schema=INPUT_FINGERPRINT_SCHEMA_VERSION,
        input_fingerprint_sha256=input_fingerprint.sha256,
        input_fingerprint_uri=input_fingerprint_uri,
        logical_dttm=logical_dttm,
        publication_key=publication_key,
        published_row_cnt=published_row_cnt,
        publisher_version=publisher_version,
        revision_no=revision_no,
        schema_version=PUBLICATION_MANIFEST_SCHEMA_VERSION,
        target_row_counts=target_row_counts,
        target_schema_version=TARGET_SCHEMA_VERSION,
    )
    validate_publication_manifest(
        manifest,
        conditional_empty_proven=conditional_empty_proven,
    )
    return manifest


def parse_publication_manifest(payload: bytes) -> PublicationManifest:
    """canonical publication manifest bytes를 exact 13-field typed 문서로 파싱한다."""
    document = _parse_object(
        payload, _PUBLICATION_MANIFEST_KEYS, "publication manifest"
    )
    artifacts = tuple(
        _parse_artifact(value)
        for value in _require_array(document["artifacts"], "manifest artifacts")
    )
    target_value = _require_object(document["target_row_counts"], "target_row_counts")
    target_row_counts = {
        _require_nfc_nonblank(
            key, "target row count key"
        ): _require_nonnegative_integer(
            value,
            f"target row count {key}",
        )
        for key, value in target_value.items()
    }
    return PublicationManifest(
        artifact_set_sha256=_require_string(
            document["artifact_set_sha256"],
            "artifact_set_sha256",
        ),
        artifacts=artifacts,
        input_fingerprint_schema=_require_string(
            document["input_fingerprint_schema"],
            "input_fingerprint_schema",
        ),
        input_fingerprint_sha256=_require_string(
            document["input_fingerprint_sha256"],
            "input_fingerprint_sha256",
        ),
        input_fingerprint_uri=_require_string(
            document["input_fingerprint_uri"],
            "input_fingerprint_uri",
        ),
        logical_dttm=parse_utc_dttm(
            _require_string(document["logical_dttm"], "logical_dttm")
        ),
        publication_key=_require_string(document["publication_key"], "publication_key"),
        published_row_cnt=_require_nonnegative_integer(
            document["published_row_cnt"],
            "published_row_cnt",
        ),
        publisher_version=_require_string(
            document["publisher_version"],
            "publisher_version",
        ),
        revision_no=_require_nonnegative_integer(
            document["revision_no"], "revision_no"
        ),
        schema_version=_require_string(document["schema_version"], "schema_version"),
        target_row_counts=target_row_counts,
        target_schema_version=_require_string(
            document["target_schema_version"],
            "target_schema_version",
        ),
    )


def validate_publication_manifest(
    manifest: PublicationManifest,
    *,
    conditional_empty_proven: bool | None = None,
) -> None:
    """manifest hash·output·대표 행 수·EMPTY 정책을 registry와 대조한다.

    `conditional_empty_proven=None`은 wire 문서만 구조적으로 읽을 때 사용한다. 조건부 EMPTY를
    실제 게시하기 전에는 호출자가 lock 안의 topology·manifest 근거를 확인하고 `True`로
    다시 검증해야 한다.
    """
    if type(manifest) is not PublicationManifest:
        raise ContractViolation("manifest는 PublicationManifest여야 합니다.")
    if (
        conditional_empty_proven is not None
        and type(conditional_empty_proven) is not bool
    ):
        raise ContractViolation(
            "conditional_empty_proven은 bool 또는 None이어야 합니다."
        )
    _require_exact_value(
        manifest.schema_version,
        PUBLICATION_MANIFEST_SCHEMA_VERSION,
        "manifest schema_version",
    )
    _require_exact_value(
        manifest.input_fingerprint_schema,
        INPUT_FINGERPRINT_SCHEMA_VERSION,
        "manifest input_fingerprint_schema",
    )
    _require_exact_value(
        manifest.target_schema_version,
        TARGET_SCHEMA_VERSION,
        "manifest target_schema_version",
    )
    validate_sha256_hex(manifest.artifact_set_sha256)
    validate_sha256_hex(manifest.input_fingerprint_sha256)
    _require_nfc_nonblank(manifest.input_fingerprint_uri, "input fingerprint URI")
    _utc_dttm(manifest.logical_dttm)
    _require_nfc_nonblank(manifest.publisher_version, "publisher_version")
    _require_nonnegative_integer(manifest.revision_no, "revision_no")
    _require_nonnegative_integer(manifest.published_row_cnt, "published_row_cnt")
    _require_tuple(manifest.artifacts, "manifest artifacts")
    _require_instances(manifest.artifacts, Artifact, "artifact")
    if not isinstance(manifest.target_row_counts, Mapping):
        raise ContractViolation("target_row_counts는 mapping이어야 합니다.")

    spec = get_publication_spec(manifest.publication_key)
    artifact_set = ArtifactSet(
        ARTIFACT_SET_SCHEMA_VERSION,
        tuple(
            Artifact(
                artifact.byte_sha256,
                artifact.role,
                artifact.row_count,
                artifact.uri,
            )
            for artifact in manifest.artifacts
        ),
    )
    if artifact_set.sha256 != manifest.artifact_set_sha256:
        raise ContractViolation(
            "manifest artifact_set_sha256이 embedded artifacts와 다릅니다."
        )

    actual_targets = set(manifest.target_row_counts)
    expected_targets = set(spec.target_tables)
    if actual_targets != expected_targets:
        raise ContractViolation(
            f"{manifest.publication_key} target_row_counts key가 registry와 다릅니다: "
            f"expected={sorted(expected_targets)}, actual={sorted(actual_targets)}"
        )
    for target, count in manifest.target_row_counts.items():
        _require_nfc_nonblank(target, "target row count key")
        _require_nonnegative_integer(count, f"target row count {target}")

    representative_count = _representative_row_count(spec, manifest.target_row_counts)
    if manifest.published_row_cnt != representative_count:
        raise ContractViolation(
            "published_row_cnt가 registry의 대표 target row count와 다릅니다."
        )
    _validate_output_artifacts(spec, artifact_set, manifest.target_row_counts)
    _validate_empty_policy(
        spec,
        manifest.published_row_cnt,
        conditional_empty_proven=conditional_empty_proven,
    )


def _validate_output_artifacts(
    spec: PublicationSpec,
    artifact_set: ArtifactSet,
    target_row_counts: Mapping[str, int],
) -> None:
    """artifact role과 target별 행 수가 registry mapping과 같은지 검증한다."""
    if target_row_counts[spec.representative_target] == 0:
        if any(count != 0 for count in target_row_counts.values()):
            raise ContractViolation(
                "정상 EMPTY manifest의 모든 target row count는 0이어야 합니다."
            )
        if artifact_set.artifacts:
            raise ContractViolation(
                "정상 EMPTY manifest의 artifacts는 빈 배열이어야 합니다."
            )
        if artifact_set.sha256 != EMPTY_ARTIFACT_SET_SHA256:
            raise ContractViolation("정상 EMPTY artifact set hash가 회귀값과 다릅니다.")
        return

    actual_roles = tuple(artifact.role for artifact in artifact_set.artifacts)
    if actual_roles != spec.output_roles:
        raise ContractViolation(
            f"{spec.publication_key} output artifact role이 registry와 다릅니다: "
            f"expected={spec.output_roles}, actual={actual_roles}"
        )
    target_by_role = dict(spec.output_targets)
    for artifact in artifact_set.artifacts:
        target = target_by_role[artifact.role]
        if artifact.row_count != target_row_counts[target]:
            raise ContractViolation(
                f"output artifact {artifact.role}의 row_count가 target {target}과 다릅니다."
            )


def _validate_empty_policy(
    spec: PublicationSpec,
    published_row_cnt: int,
    *,
    conditional_empty_proven: bool | None,
) -> None:
    """대표 행 수가 0일 때 key별 EMPTY 허용 정책을 적용한다."""
    if published_row_cnt != 0:
        return
    if spec.empty_policy is EmptyPolicy.FORBIDDEN:
        raise ContractViolation(
            f"{spec.publication_key} publication은 EMPTY를 금지합니다."
        )
    if (
        spec.empty_policy is EmptyPolicy.CONDITIONAL
        and conditional_empty_proven is False
    ):
        raise ContractViolation(
            f"{spec.publication_key} EMPTY는 다음 근거 확인 뒤에만 허용됩니다: "
            f"{spec.conditional_empty_requirement}"
        )


def _validate_dependency_manifest_uris(
    publication_key: str,
    fingerprint: InputFingerprint,
) -> None:
    """dependency와 동명 publication manifest input의 URI 결합을 검증한다."""
    bindings = _MANIFEST_ROLE_DEPENDENCY.get(publication_key, {})
    if not bindings:
        return
    dependencies = {
        dependency.publication_key: dependency
        for dependency in fingerprint.dependencies
    }
    artifacts = {artifact.role: artifact for artifact in fingerprint.input_artifacts}
    for role, dependency_key in bindings.items():
        if artifacts[role].uri != dependencies[dependency_key].manifest_uri:
            raise ContractViolation(
                f"{role} URI가 {dependency_key} dependency manifest_uri와 다릅니다."
            )


def _validate_hash_parameters(
    publication_key: str,
    fingerprint: InputFingerprint,
) -> None:
    """별도 canonical document를 가리키는 SHA parameter 의미를 검증한다."""
    parameters = {
        parameter.name: parameter.value for parameter in fingerprint.parameters
    }
    if "expected_sta_id_sha256" in parameters:
        validate_sha256_hex(parameters["expected_sta_id_sha256"])
    if publication_key != "rebalance_route":
        return

    coverage_sha256 = validate_sha256_hex(parameters["route_coverage_sha256"])
    coverage_artifacts = tuple(
        artifact
        for artifact in fingerprint.input_artifacts
        if artifact.role == "route_coverage"
    )
    if len(coverage_artifacts) != 1:
        raise ContractViolation(
            "route_coverage input artifact가 정확히 하나 필요합니다."
        )
    if coverage_sha256 != coverage_artifacts[0].byte_sha256:
        raise ContractViolation(
            "route_coverage_sha256 parameter가 route_coverage 실제 bytes SHA와 다릅니다."
        )


def _validate_manifest_dependency_tuple(
    manifest: PublicationManifest,
    dependency: Dependency,
) -> None:
    """실제 publication manifest의 state identity를 dependency와 대조한다."""
    actual = (
        manifest.publication_key,
        manifest.logical_dttm,
        manifest.revision_no,
        manifest.artifact_set_sha256,
        manifest.input_fingerprint_sha256,
    )
    expected = (
        dependency.publication_key,
        dependency.logical_dttm,
        dependency.revision_no,
        dependency.artifact_set_sha256,
        dependency.input_fingerprint_sha256,
    )
    if actual != expected:
        raise ContractViolation(
            f"실제 manifest 내용이 {dependency.publication_key} dependency tuple과 다릅니다."
        )


def _representative_row_count(
    spec: PublicationSpec,
    target_row_counts: Mapping[str, int],
) -> int:
    """registry가 지정한 대표 target의 row count를 반환한다."""
    actual_targets = set(target_row_counts)
    expected_targets = set(spec.target_tables)
    if actual_targets != expected_targets:
        raise ContractViolation(
            f"{spec.publication_key} target_row_counts key가 registry와 다릅니다."
        )
    for target, count in target_row_counts.items():
        _require_nfc_nonblank(target, "target row count key")
        _require_nonnegative_integer(count, f"target row count {target}")
    return target_row_counts[spec.representative_target]


def _artifact_set_document(artifact_set: ArtifactSet) -> dict[str, Any]:
    """artifact set dataclass를 정확한 JSON object로 바꾼다."""
    return {
        "artifacts": [
            _artifact_document(artifact) for artifact in artifact_set.artifacts
        ],
        "schema_version": artifact_set.schema_version,
    }


def _artifact_document(artifact: Artifact) -> dict[str, Any]:
    """artifact dataclass를 정확한 JSON object로 바꾼다."""
    return {
        "byte_sha256": artifact.byte_sha256,
        "role": artifact.role,
        "row_count": artifact.row_count,
        "uri": artifact.uri,
    }


def _input_fingerprint_document(fingerprint: InputFingerprint) -> dict[str, Any]:
    """input fingerprint dataclass를 정확한 JSON object로 바꾼다."""
    return {
        "dependencies": [
            _dependency_document(dependency) for dependency in fingerprint.dependencies
        ],
        "input_artifacts": [
            _input_artifact_document(artifact)
            for artifact in fingerprint.input_artifacts
        ],
        "parameters": [
            _parameter_document(parameter) for parameter in fingerprint.parameters
        ],
        "schema_version": fingerprint.schema_version,
    }


def _dependency_document(dependency: Dependency) -> dict[str, Any]:
    """dependency dataclass를 정확한 JSON object로 바꾼다."""
    return {
        "artifact_set_sha256": dependency.artifact_set_sha256,
        "input_fingerprint_sha256": dependency.input_fingerprint_sha256,
        "logical_dttm": format_utc_dttm(dependency.logical_dttm),
        "manifest_uri": dependency.manifest_uri,
        "publication_key": dependency.publication_key,
        "revision_no": dependency.revision_no,
    }


def _input_artifact_document(artifact: InputArtifact) -> dict[str, Any]:
    """input artifact dataclass를 정확한 JSON object로 바꾼다."""
    return {
        "byte_sha256": artifact.byte_sha256,
        "role": artifact.role,
        "uri": artifact.uri,
    }


def _parameter_document(parameter: Parameter) -> dict[str, Any]:
    """parameter dataclass를 정확한 JSON object로 바꾼다."""
    return {"name": parameter.name, "value": parameter.value}


def _id_set_document(id_set: IdSet) -> dict[str, Any]:
    """ID set dataclass를 정확한 JSON object로 바꾼다."""
    return {"ids": list(id_set.ids), "schema_version": id_set.schema_version}


def _publication_manifest_document(manifest: PublicationManifest) -> dict[str, Any]:
    """publication manifest dataclass를 정확한 13-key JSON object로 바꾼다."""
    return {
        "artifact_set_sha256": manifest.artifact_set_sha256,
        "artifacts": [_artifact_document(artifact) for artifact in manifest.artifacts],
        "input_fingerprint_schema": manifest.input_fingerprint_schema,
        "input_fingerprint_sha256": manifest.input_fingerprint_sha256,
        "input_fingerprint_uri": manifest.input_fingerprint_uri,
        "logical_dttm": format_utc_dttm(manifest.logical_dttm),
        "publication_key": manifest.publication_key,
        "published_row_cnt": manifest.published_row_cnt,
        "publisher_version": manifest.publisher_version,
        "revision_no": manifest.revision_no,
        "schema_version": manifest.schema_version,
        "target_row_counts": dict(manifest.target_row_counts),
        "target_schema_version": manifest.target_schema_version,
    }


def _parse_artifact(value: Any) -> Artifact:
    """JSON 값을 exact-key Artifact로 파싱한다."""
    document = _require_exact_object(value, _ARTIFACT_KEYS, "artifact")
    return Artifact(
        byte_sha256=_require_string(document["byte_sha256"], "artifact byte_sha256"),
        role=_require_string(document["role"], "artifact role"),
        row_count=_require_nonnegative_integer(
            document["row_count"], "artifact row_count"
        ),
        uri=_require_string(document["uri"], "artifact URI"),
    )


def _parse_dependency(value: Any) -> Dependency:
    """JSON 값을 exact-key Dependency로 파싱한다."""
    document = _require_exact_object(value, _DEPENDENCY_KEYS, "dependency")
    return Dependency(
        artifact_set_sha256=_require_string(
            document["artifact_set_sha256"],
            "dependency artifact_set_sha256",
        ),
        input_fingerprint_sha256=_require_string(
            document["input_fingerprint_sha256"],
            "dependency input_fingerprint_sha256",
        ),
        logical_dttm=parse_utc_dttm(
            _require_string(document["logical_dttm"], "dependency logical_dttm")
        ),
        manifest_uri=_require_string(
            document["manifest_uri"], "dependency manifest_uri"
        ),
        publication_key=_require_string(
            document["publication_key"],
            "dependency publication_key",
        ),
        revision_no=_require_nonnegative_integer(
            document["revision_no"],
            "dependency revision_no",
        ),
    )


def _parse_input_artifact(value: Any) -> InputArtifact:
    """JSON 값을 exact-key InputArtifact로 파싱한다."""
    document = _require_exact_object(value, _INPUT_ARTIFACT_KEYS, "input artifact")
    return InputArtifact(
        byte_sha256=_require_string(
            document["byte_sha256"],
            "input artifact byte_sha256",
        ),
        role=_require_string(document["role"], "input artifact role"),
        uri=_require_string(document["uri"], "input artifact URI"),
    )


def _parse_parameter(value: Any) -> Parameter:
    """JSON 값을 exact-key Parameter로 파싱한다."""
    document = _require_exact_object(value, _PARAMETER_KEYS, "parameter")
    return Parameter(
        name=_require_string(document["name"], "parameter name"),
        value=_require_string(document["value"], "parameter value"),
    )


def _parse_object(
    payload: bytes,
    expected_keys: frozenset[str],
    name: str,
) -> dict[str, Any]:
    """canonical bytes의 root를 exact-key object로 파싱한다."""
    value = parse_canonical_json(payload)
    return _require_exact_object(value, expected_keys, name)


def _require_exact_object(
    value: Any,
    expected_keys: frozenset[str],
    name: str,
) -> dict[str, Any]:
    """값이 정확한 key 집합을 가진 JSON object인지 확인한다."""
    document = _require_object(value, name)
    actual_keys = frozenset(document)
    if actual_keys != expected_keys:
        missing = sorted(expected_keys.difference(actual_keys))
        extra = sorted(actual_keys.difference(expected_keys))
        raise ContractViolation(
            f"{name} key가 정확하지 않습니다: missing={missing}, extra={extra}"
        )
    return document


def _require_object(value: Any, name: str) -> dict[str, Any]:
    """값이 JSON object인지 확인한다."""
    if type(value) is not dict:
        raise ContractViolation(f"{name}은 JSON object여야 합니다.")
    return cast(dict[str, Any], value)


def _require_array(value: Any, name: str) -> list[Any]:
    """값이 JSON array인지 확인한다."""
    if type(value) is not list:
        raise ContractViolation(f"{name}은 JSON array여야 합니다.")
    return cast(list[Any], value)


def _require_string(value: Any, name: str) -> str:
    """값이 NFC 문자열인지 확인하고 반환한다."""
    if type(value) is not str:
        raise ContractViolation(f"{name}은 문자열이어야 합니다.")
    _require_nfc_string(value, name)
    return value


def _require_exact_value(value: Any, expected: str, name: str) -> None:
    """문자열 field가 contract 고정값과 정확히 같은지 확인한다."""
    if value != expected:
        raise ContractViolation(f"{name}은 정확히 {expected!r}이어야 합니다.")


def _require_instances(
    values: tuple[Any, ...], expected_type: type[Any], name: str
) -> None:
    """tuple의 모든 값이 기대 dataclass 인스턴스인지 확인한다."""
    if any(type(value) is not expected_type for value in values):
        raise ContractViolation(
            f"모든 {name} 값은 {expected_type.__name__}이어야 합니다."
        )


def _utc_dttm(value: datetime) -> datetime:
    """aware datetime을 contract의 UTC instant로 정규화한다."""
    format_utc_dttm(value)
    return value.astimezone(UTC)
