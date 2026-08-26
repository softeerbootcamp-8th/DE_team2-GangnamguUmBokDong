"""Inference CLI가 Gold serving plan에서 필요한 exact 입력만 안전하게 연다."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any, cast
from urllib.parse import urlsplit

from .gold_publication import (
    ContractViolation,
    Dependency,
    IdSet,
    InputArtifact,
    ImmutableObjectStore,
    parse_canonical_json,
    parse_id_set,
    parse_utc_dttm,
    sha256_hex,
    validate_sha256_hex,
)
from .inference_snapshot import ServingPlanRef, ServingReleaseRef
from .model_snapshot import IdSetArtifactRef

LEGACY_SERVING_PLAN_SCHEMA_VERSION = "gold-serving-plan-v2"
"""Exact shared identity가 없어 새 inference를 금지하는 legacy version이다."""

SERVING_PLAN_SCHEMA_VERSION = "gold-serving-plan-v3"
"""Inference input extractor가 허용하는 serving plan schema version이다."""

_PLAN_KEYS = frozenset(
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
        "serving_release",
        "source_lookbacks",
        "station_dependency",
        "station_master_enriched",
    }
)
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


@dataclass(frozen=True, slots=True)
class VerifiedServingPlanInferenceInputs:
    """Exact plan과 expected ID bytes에서 검증된 inference 호출 입력이다."""

    logical_dttm: datetime
    object_base_uri: str
    station_dependency: Dependency
    serving_plan: ServingPlanRef
    expected_sta_ids_ref: IdSetArtifactRef
    expected_sta_ids: IdSet
    serving_release: ServingReleaseRef
    station_master_enriched: InputArtifact

    def __post_init__(self) -> None:
        """Plan-derived typed 값의 exact class와 anchor 결합을 검증한다."""
        if type(self.station_dependency) is not Dependency:
            raise ContractViolation("station dependency 타입이 잘못됐습니다.")
        if type(self.serving_plan) is not ServingPlanRef:
            raise ContractViolation("serving plan ref 타입이 잘못됐습니다.")
        if type(self.expected_sta_ids_ref) is not IdSetArtifactRef:
            raise ContractViolation("expected station ID ref 타입이 잘못됐습니다.")
        if type(self.expected_sta_ids) is not IdSet:
            raise ContractViolation("expected station ID set 타입이 잘못됐습니다.")
        if type(self.serving_release) is not ServingReleaseRef:
            raise ContractViolation("serving release ref 타입이 잘못됐습니다.")
        if type(self.station_master_enriched) is not InputArtifact:
            raise ContractViolation("station_master_enriched ref 타입이 잘못됐습니다.")
        if self.station_dependency.logical_dttm != self.logical_dttm:
            raise ContractViolation(
                "serving plan logical과 station dependency logical이 다릅니다."
            )


def read_serving_plan_inference_inputs(
    object_store: ImmutableObjectStore,
    *,
    plan_uri: str,
    plan_sha256: str,
) -> VerifiedServingPlanInferenceInputs:
    """Plan과 expected ID set actual bytes를 URI·SHA로 exact-read한다."""
    validate_sha256_hex(plan_sha256)
    plan_ref = ServingPlanRef(byte_sha256=plan_sha256, uri=plan_uri)
    payload = object_store.read_bytes(
        plan_uri,
        plan_sha256,
        require_canonical_json=True,
    )
    if sha256_hex(payload) != plan_sha256:
        raise ContractViolation("serving plan actual bytes SHA가 argument와 다릅니다.")
    parsed = parse_canonical_json(payload)
    if type(parsed) is not dict:
        raise ContractViolation("serving plan은 JSON object여야 합니다.")
    version = _string(parsed.get("schema_version"), "schema_version")
    if version == LEGACY_SERVING_PLAN_SCHEMA_VERSION:
        raise ContractViolation(
            "v2 serving plan에는 exact serving release와 station_master_enriched가 "
            "없습니다. 같은 logical tick의 prepare를 다시 실행해야 합니다."
        )
    if version != SERVING_PLAN_SCHEMA_VERSION:
        raise ContractViolation("serving plan schema_version이 다릅니다.")
    document = _exact_object(parsed, _PLAN_KEYS, "serving plan")
    logical = parse_utc_dttm(_string(document["logical_dttm"], "logical_dttm"))
    dependency = _parse_dependency(document["station_dependency"])
    expected_ref = _parse_id_set_ref(document["expected_sta_ids"])
    object_base_uri = _string(document["object_base_uri"], "object_base_uri")
    serving_release = _parse_serving_release_ref(document["serving_release"])
    station_master_enriched = _parse_input_artifact(
        document["station_master_enriched"],
        "station_master_enriched",
    )
    _require_under_object_base(plan_uri, object_base_uri, "serving plan")
    _require_under_object_base(expected_ref.uri, object_base_uri, "expected ID set")
    _require_under_object_base(
        dependency.manifest_uri,
        object_base_uri,
        "station dependency manifest",
    )
    _require_same_bucket(serving_release.uri, object_base_uri, "serving release")
    _require_enriched_master_ref(station_master_enriched, object_base_uri)
    expected_payload = object_store.read_bytes(
        expected_ref.uri,
        expected_ref.byte_sha256,
        require_canonical_json=True,
    )
    expected = parse_id_set(expected_payload)
    if (
        expected.sha256 != expected_ref.byte_sha256
        or len(expected.ids) != expected_ref.id_count
    ):
        raise ContractViolation(
            "serving plan expected station ID actual bytes가 ref와 다릅니다."
        )
    return VerifiedServingPlanInferenceInputs(
        logical_dttm=logical,
        object_base_uri=object_base_uri,
        station_dependency=dependency,
        serving_plan=plan_ref,
        expected_sta_ids_ref=expected_ref,
        expected_sta_ids=expected,
        serving_release=serving_release,
        station_master_enriched=station_master_enriched,
    )


def _parse_id_set_ref(value: Any) -> IdSetArtifactRef:
    """Expected station ID reference exact object를 파싱한다."""
    document = _exact_object(value, _ID_SET_REF_KEYS, "expected_sta_ids")
    return IdSetArtifactRef(
        byte_sha256=_string(document["byte_sha256"], "expected byte_sha256"),
        id_count=_nonnegative_int(document["id_count"], "expected id_count"),
        schema_version=_string(document["schema_version"], "expected schema_version"),
        uri=_string(document["uri"], "expected uri"),
    )


def _parse_dependency(value: Any) -> Dependency:
    """Station dependency exact object를 파싱한다."""
    document = _exact_object(value, _DEPENDENCY_KEYS, "station dependency")
    return Dependency(
        artifact_set_sha256=_string(
            document["artifact_set_sha256"], "dependency artifact_set_sha256"
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
            document["publication_key"], "dependency publication_key"
        ),
        revision_no=_nonnegative_int(document["revision_no"], "dependency revision_no"),
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


def _exact_object(value: Any, keys: frozenset[str], name: str) -> dict[str, Any]:
    """JSON object의 exact key 집합을 검증한다."""
    if type(value) is not dict or frozenset(value) != keys:
        raise ContractViolation(f"{name} key 집합이 잘못됐습니다.")
    return cast(dict[str, Any], value)


def _string(value: Any, name: str) -> str:
    """Nonblank exact string을 반환한다."""
    if type(value) is not str or not value or value != value.strip():
        raise ContractViolation(f"{name}은 nonblank exact string이어야 합니다.")
    return value


def _nonnegative_int(value: Any, name: str) -> int:
    """Bool을 제외한 nonnegative exact integer를 반환한다."""
    if type(value) is not int or value < 0:
        raise ContractViolation(f"{name}은 nonnegative exact integer여야 합니다.")
    return value


def _require_under_object_base(uri: str, base_uri: str, name: str) -> None:
    """Exact S3 ref가 plan object base의 같은 bucket·prefix 아래인지 검증한다."""
    reference = urlsplit(uri)
    base = urlsplit(base_uri)
    base_key = base.path.lstrip("/").rstrip("/")
    reference_key = reference.path.lstrip("/")
    if (
        base.scheme != "s3"
        or not base.netloc
        or not base_key
        or base.query
        or base.fragment
        or reference.scheme != "s3"
        or reference.netloc != base.netloc
        or not reference_key.startswith(f"{base_key}/")
        or reference.query
        or reference.fragment
    ):
        raise ContractViolation(f"{name} URI가 serving plan object base 밖입니다.")


def _require_same_bucket(uri: str, base_uri: str, name: str) -> None:
    """Exact ref와 serving plan base가 같은 S3 bucket인지 검증한다."""
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
    base_uri: str,
) -> None:
    """Enriched master를 same-bucket Silver Parquet ref로 제한한다."""
    _require_same_bucket(reference.uri, base_uri, "station_master_enriched")
    key = urlsplit(reference.uri).path.lstrip("/")
    if not key.startswith("silver/station_master_enriched/") or not key.endswith(
        ".parquet"
    ):
        raise ContractViolation("station_master_enriched URI 경로가 잘못됐습니다.")


__all__ = [
    "LEGACY_SERVING_PLAN_SCHEMA_VERSION",
    "SERVING_PLAN_SCHEMA_VERSION",
    "VerifiedServingPlanInferenceInputs",
    "read_serving_plan_inference_inputs",
]
