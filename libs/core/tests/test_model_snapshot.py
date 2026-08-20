"""Immutable model snapshot의 exact canonical byte와 bundle 경계를 검증한다."""

from __future__ import annotations

import json
from dataclasses import replace

import pytest
from core.gold_publication.canonical import canonical_json_bytes, sha256_hex
from core.gold_publication.contract import build_id_set
from core.gold_publication.errors import CanonicalParseError
from core.model_snapshot import (
    MODEL_ARTIFACT_ROLES,
    MODEL_BUNDLE_IDENTITY_SCHEMA_VERSION,
    MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
    SERVING_FEATURE_PROFILE_KEYS,
    STATION_CROSSWALK_SCHEMA_VERSION,
    IdSetArtifactRef,
    ModelArtifact,
    ModelKind,
    ModelSnapshotContractError,
    ModelSnapshotManifest,
    StationCrosswalk,
    StationCrosswalkEntry,
    build_id_set_artifact_ref,
    build_model_snapshot_manifest,
    build_model_support_sta_ids,
    build_station_crosswalk,
    canonical_station_categories_bytes,
    derive_model_support_sta_ids,
    effective_contract_version_from_profile,
    extract_serving_feature_contract_bytes,
    model_bundle_version,
    model_manifest_input_artifact,
    parse_model_snapshot_manifest,
    parse_station_categories,
    parse_station_crosswalk,
    validate_content_addressed_s3_uri,
    validate_model_effective_contract_binding,
)

_SHA_BY_ROLE = {
    role: f"{index:x}" * 64 for index, role in enumerate(MODEL_ARTIFACT_ROLES, 1)
}
_EXTENSION_BY_ROLE = {
    role: "txt" if role.startswith("booster_") else "json"
    for role in MODEL_ARTIFACT_ROLES
}
SUPPORT_SHA256 = "f" * 64
EFFECTIVE_CONTRACT_VERSION = "sha256:" + "a" * 64


def _artifact(role: str) -> ModelArtifact:
    """Role별 고정 SHA와 content-addressed fixture artifact를 만든다."""
    checksum = _SHA_BY_ROLE[role]
    extension = _EXTENSION_BY_ROLE[role]
    return ModelArtifact(
        byte_sha256=checksum,
        role=role,
        uri=f"s3://fixture/models/rental/{role}/sha256={checksum}.{extension}",
    )


def _support(id_count: int = 2) -> IdSetArtifactRef:
    """테스트용 immutable Gold ID-set ref를 만든다."""
    return IdSetArtifactRef(
        byte_sha256=SUPPORT_SHA256,
        id_count=id_count,
        schema_version="gold-id-set-v1",
        uri=f"s3://fixture/models/support/sha256={SUPPORT_SHA256}.json",
    )


def _manifest(**overrides: object) -> ModelSnapshotManifest:
    """테스트용 정상 rental model snapshot manifest를 만든다."""
    fields: dict[str, object] = {
        "model_kind": ModelKind.RENTAL,
        "effective_contract_version": EFFECTIVE_CONTRACT_VERSION,
        "artifacts": tuple(_artifact(role) for role in reversed(MODEL_ARTIFACT_ROLES)),
        "support_sta_ids": _support(),
    }
    fields.update(overrides)
    return build_model_snapshot_manifest(**fields)  # type: ignore[arg-type]


def _effective_profile_payload(**overrides: object) -> bytes:
    """Training-only 값도 포함한 full effective profile JSON을 만든다."""
    profile: dict[str, object] = {
        "ROLLING_TICK_MINUTES": 5,
        "ROLLING_WINDOW_MINUTES": 60,
        "ROLLING_EMBARGO_MINUTES": 30,
        "TARGET_HORIZON_MINUTES": 60,
        "GRID_TICK_MINUTES": 5,
        "TRAIN_ANCHOR_TICK_MINUTES": 20,
        "HORIZON_COUNT": 12,
        "TRAIN_LOOKBACK_MONTHS": 12,
        "LGB_PARAMS_COMMON": {"learning_rate": 0.05, "num_leaves": 63},
    }
    profile.update(overrides)
    return json.dumps(profile, sort_keys=True, separators=(",", ":")).encode()


def test_model_manifest_has_stable_exact_canonical_bytes() -> None:
    """Model bundle의 exact role, field와 canonical byte 표현을 golden 고정한다."""
    manifest = _manifest()

    assert manifest.canonical_bytes == (
        b'{"artifacts":['
        b'{"byte_sha256":"1111111111111111111111111111111111111111111111111111111111111111",'
        b'"role":"booster_poisson","uri":"s3://fixture/models/rental/booster_poisson/'
        b'sha256=1111111111111111111111111111111111111111111111111111111111111111.txt"},'
        b'{"byte_sha256":"2222222222222222222222222222222222222222222222222222222222222222",'
        b'"role":"booster_q10","uri":"s3://fixture/models/rental/booster_q10/'
        b'sha256=2222222222222222222222222222222222222222222222222222222222222222.txt"},'
        b'{"byte_sha256":"3333333333333333333333333333333333333333333333333333333333333333",'
        b'"role":"booster_q50","uri":"s3://fixture/models/rental/booster_q50/'
        b'sha256=3333333333333333333333333333333333333333333333333333333333333333.txt"},'
        b'{"byte_sha256":"4444444444444444444444444444444444444444444444444444444444444444",'
        b'"role":"booster_q90","uri":"s3://fixture/models/rental/booster_q90/'
        b'sha256=4444444444444444444444444444444444444444444444444444444444444444.txt"},'
        b'{"byte_sha256":"5555555555555555555555555555555555555555555555555555555555555555",'
        b'"role":"conformal_correction","uri":"s3://fixture/models/rental/conformal_correction/'
        b'sha256=5555555555555555555555555555555555555555555555555555555555555555.json"},'
        b'{"byte_sha256":"6666666666666666666666666666666666666666666666666666666666666666",'
        b'"role":"effective_profile","uri":"s3://fixture/models/rental/effective_profile/'
        b'sha256=6666666666666666666666666666666666666666666666666666666666666666.json"},'
        b'{"byte_sha256":"7777777777777777777777777777777777777777777777777777777777777777",'
        b'"role":"metrics","uri":"s3://fixture/models/rental/metrics/'
        b'sha256=7777777777777777777777777777777777777777777777777777777777777777.json"},'
        b'{"byte_sha256":"8888888888888888888888888888888888888888888888888888888888888888",'
        b'"role":"station_categories","uri":"s3://fixture/models/rental/station_categories/'
        b'sha256=8888888888888888888888888888888888888888888888888888888888888888.json"},'
        b'{"byte_sha256":"9999999999999999999999999999999999999999999999999999999999999999",'
        b'"role":"station_crosswalk","uri":"s3://fixture/models/rental/station_crosswalk/'
        b'sha256=9999999999999999999999999999999999999999999999999999999999999999.json"}],'
        b'"effective_contract_version":"sha256:'
        b'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",'
        b'"model_kind":"rental","model_version":"sha256:'
        b'92250dd0e8d9bddf48628608f511227906d097157f23f9f4f833a656f9001f6b",'
        b'"schema_version":"ml-model-snapshot-manifest-v1",'
        b'"support_sta_ids":{"byte_sha256":"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",'
        b'"id_count":2,"schema_version":"gold-id-set-v1",'
        b'"uri":"s3://fixture/models/support/'
        b'sha256=ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff.json"}}'
    )
    assert parse_model_snapshot_manifest(manifest.canonical_bytes) == manifest


def test_builder_accepts_exact_return_kind_and_sorts_roles() -> None:
    """Builder가 return bundle도 허용하고 artifact를 exact role 순으로 정렬한다."""
    manifest = _manifest(
        model_kind=ModelKind.RETURN,
    )

    assert manifest.model_kind is ModelKind.RETURN
    assert tuple(value.role for value in manifest.artifacts) == MODEL_ARTIFACT_ROLES


@pytest.mark.parametrize(
    ("artifacts", "message"),
    [
        (tuple(_artifact(role) for role in MODEL_ARTIFACT_ROLES[:-1]), "exact 집합"),
        (
            tuple(_artifact(role) for role in MODEL_ARTIFACT_ROLES)
            + (_artifact("booster_poisson"),),
            "exact 집합",
        ),
        (
            tuple(_artifact(role) for role in reversed(MODEL_ARTIFACT_ROLES)),
            "exact 집합",
        ),
    ],
)
def test_typed_manifest_rejects_missing_duplicate_or_unsorted_roles(
    artifacts: tuple[ModelArtifact, ...],
    message: str,
) -> None:
    """Direct typed constructor는 exact sorted role tuple 외에는 받지 않는다."""
    with pytest.raises(ModelSnapshotContractError, match=message):
        ModelSnapshotManifest(
            schema_version=MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            model_kind=ModelKind.RENTAL,
            model_version="sha256:" + "0" * 64,
            effective_contract_version=EFFECTIVE_CONTRACT_VERSION,
            artifacts=artifacts,
            support_sta_ids=_support(),
        )


def test_manifest_rejects_duplicate_artifact_uri_and_support_alias() -> None:
    """서로 다른 의미의 role과 support가 같은 immutable URI를 재사용하지 못한다."""
    artifacts = list(_manifest().artifacts)
    artifacts[1] = replace(
        artifacts[1],
        uri=artifacts[0].uri,
        byte_sha256=artifacts[0].byte_sha256,
    )
    with pytest.raises(ModelSnapshotContractError, match="URI는 중복"):
        ModelSnapshotManifest(
            MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            ModelKind.RENTAL,
            "sha256:" + "0" * 64,
            EFFECTIVE_CONTRACT_VERSION,
            tuple(artifacts),
            _support(),
        )

    json_artifact = _manifest().artifacts[5]
    support = replace(
        _support(),
        byte_sha256=json_artifact.byte_sha256,
        uri=json_artifact.uri,
    )
    with pytest.raises(ModelSnapshotContractError, match="support ID set URI"):
        ModelSnapshotManifest(
            MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            ModelKind.RENTAL,
            "sha256:" + "0" * 64,
            EFFECTIVE_CONTRACT_VERSION,
            _manifest().artifacts,
            support,
        )


@pytest.mark.parametrize(
    ("uri", "checksum", "extension"),
    [
        ("https://fixture/x/sha256=" + "a" * 64 + ".json", "a" * 64, "json"),
        ("s3://fixture/x/current.json", "a" * 64, "json"),
        ("s3://fixture/x/sha256=" + "b" * 64 + ".json", "a" * 64, "json"),
        ("s3://fixture/x/sha256=" + "a" * 64 + ".txt", "a" * 64, "json"),
        ("s3://fixture/x/sha256=" + "a" * 64 + ".json?version=1", "a" * 64, "json"),
    ],
)
def test_content_addressed_uri_rejects_mutable_or_mismatched_objects(
    uri: str,
    checksum: str,
    extension: str,
) -> None:
    """Scheme, filename digest, 확장자 또는 query가 틀린 object를 거부한다."""
    with pytest.raises(ModelSnapshotContractError):
        validate_content_addressed_s3_uri(
            uri,
            checksum,
            expected_extension=extension,
        )


def test_id_set_ref_is_built_from_actual_gold_id_set_bytes() -> None:
    """Support ref의 digest와 count가 canonical gold-id-set-v1에서 파생된다."""
    id_set = build_id_set(("ST-2", "ST-1"))
    uri = f"s3://fixture/support/sha256={id_set.sha256}.json"

    reference = build_id_set_artifact_ref(id_set, uri)

    assert reference.byte_sha256 == id_set.sha256
    assert reference.id_count == 2
    assert reference.schema_version == "gold-id-set-v1"


def test_parser_rejects_unknown_field_and_noncanonical_bytes() -> None:
    """Parser가 schema 확장과 canonical이 아닌 JSON 표현을 fail closed한다."""
    payload = _manifest().canonical_bytes
    document = json.loads(payload)
    document["extra"] = "not-v1"

    with pytest.raises(ModelSnapshotContractError, match="extra"):
        parse_model_snapshot_manifest(canonical_json_bytes(document))
    with pytest.raises(CanonicalParseError):
        parse_model_snapshot_manifest(payload.replace(b'":', b'": ', 1))


def test_exact_builtin_and_dataclass_types_reject_subclasses() -> None:
    """Bool, string/tuple/dataclass subclass가 exact typed contract를 우회하지 못한다."""

    class StringSubclass(str):
        """Exact string 검증용 subclass다."""

    class ArtifactSubclass(ModelArtifact):
        """Exact artifact 검증용 subclass다."""

    with pytest.raises(ModelSnapshotContractError):
        IdSetArtifactRef(SUPPORT_SHA256, True, "gold-id-set-v1", _support().uri)  # type: ignore[arg-type]
    with pytest.raises(ModelSnapshotContractError):
        replace(
            _manifest(),
            model_version=StringSubclass(_manifest().model_version),
        )

    values = list(_manifest().artifacts)
    first = values[0]
    values[0] = ArtifactSubclass(first.byte_sha256, first.role, first.uri)
    with pytest.raises(ModelSnapshotContractError, match="exact ModelArtifact"):
        ModelSnapshotManifest(
            MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION,
            ModelKind.RENTAL,
            "sha256:" + "0" * 64,
            EFFECTIVE_CONTRACT_VERSION,
            tuple(values),
            _support(),
        )


def test_model_manifest_becomes_exact_gold_input_artifact() -> None:
    """Canonical manifest bytes가 raw bundle이 아닌 Gold model role의 authority가 된다."""
    manifest = _manifest()
    uri = f"s3://fixture/model-manifests/sha256={manifest.sha256}.json"

    artifact = model_manifest_input_artifact(manifest, uri)

    assert artifact.role == "rental_model_manifest"
    assert artifact.byte_sha256 == manifest.sha256
    assert artifact.uri == uri


def test_schema_and_role_constants_are_disk_contract() -> None:
    """Model manifest schema와 inference-required role 집합을 회귀 고정한다."""
    assert MODEL_SNAPSHOT_MANIFEST_SCHEMA_VERSION == "ml-model-snapshot-manifest-v1"
    assert MODEL_BUNDLE_IDENTITY_SCHEMA_VERSION == "ml-model-bundle-identity-v1"
    assert MODEL_ARTIFACT_ROLES == (
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


def test_model_version_is_derived_from_exact_bundle_identity() -> None:
    """Model version이 임의 label이 아니라 versionless bundle bytes에 결합된다."""
    manifest = _manifest()

    assert manifest.model_version == model_bundle_version(
        model_kind=manifest.model_kind,
        effective_contract_version=manifest.effective_contract_version,
        artifacts=manifest.artifacts,
        support_sta_ids=manifest.support_sta_ids,
    )
    with pytest.raises(ModelSnapshotContractError, match="bundle identity"):
        replace(manifest, model_version="sha256:" + "0" * 64)


def test_station_crosswalk_has_stable_bytes_and_exact_one_to_one_mapping() -> None:
    """Station crosswalk schema, sort와 양방향 unique mapping을 고정한다."""
    crosswalk = build_station_crosswalk(
        (
            StationCrosswalkEntry(20, "ST-20"),
            StationCrosswalkEntry(10, "ST-10"),
            StationCrosswalkEntry(30, "ST-30"),
        )
    )

    assert crosswalk.canonical_bytes == (
        b'{"entries":[{"sta_id":"ST-10","station_no":10},'
        b'{"sta_id":"ST-20","station_no":20},'
        b'{"sta_id":"ST-30","station_no":30}],'
        b'"schema_version":"ml-station-crosswalk-v1"}'
    )
    assert parse_station_crosswalk(crosswalk.canonical_bytes) == crosswalk
    assert STATION_CROSSWALK_SCHEMA_VERSION == "ml-station-crosswalk-v1"

    with pytest.raises(ModelSnapshotContractError, match="station_no는 중복"):
        StationCrosswalk(
            STATION_CROSSWALK_SCHEMA_VERSION,
            (
                StationCrosswalkEntry(10, "ST-10"),
                StationCrosswalkEntry(10, "ST-11"),
            ),
        )
    with pytest.raises(ModelSnapshotContractError, match="여러 station_no"):
        StationCrosswalk(
            STATION_CROSSWALK_SCHEMA_VERSION,
            (
                StationCrosswalkEntry(10, "ST-10"),
                StationCrosswalkEntry(11, "ST-10"),
            ),
        )


def test_support_id_set_is_derived_only_from_pinned_categories_and_crosswalk() -> None:
    """Model support Gold ID set을 category∩crosswalk bytes에서 재현한다."""
    categories_payload = canonical_station_categories_bytes((20, 10))
    crosswalk = build_station_crosswalk(
        (
            StationCrosswalkEntry(10, "ST-10"),
            StationCrosswalkEntry(20, "ST-20"),
            StationCrosswalkEntry(30, "ST-30"),
        )
    )
    support = build_model_support_sta_ids(
        parse_station_categories(categories_payload),
        crosswalk,
    )
    artifacts = list(_manifest().artifacts)
    for role, payload in (
        ("station_categories", categories_payload),
        ("station_crosswalk", crosswalk.canonical_bytes),
    ):
        index = MODEL_ARTIFACT_ROLES.index(role)
        checksum = sha256_hex(payload)
        artifacts[index] = ModelArtifact(
            checksum,
            role,
            f"s3://fixture/models/rental/{role}/sha256={checksum}.json",
        )
    support_ref = IdSetArtifactRef(
        support.sha256,
        len(support.ids),
        support.schema_version,
        f"s3://fixture/support/sha256={support.sha256}.json",
    )
    manifest = build_model_snapshot_manifest(
        model_kind=ModelKind.RENTAL,
        effective_contract_version=EFFECTIVE_CONTRACT_VERSION,
        artifacts=artifacts,
        support_sta_ids=support_ref,
    )

    assert support.ids == ("ST-10", "ST-20")
    assert (
        derive_model_support_sta_ids(
            manifest,
            categories_payload,
            crosswalk.canonical_bytes,
        )
        == support
    )

    with pytest.raises(ModelSnapshotContractError, match="mapping이 없습니다"):
        build_model_support_sta_ids(
            (10, 99),
            crosswalk,
        )
    with pytest.raises(ModelSnapshotContractError, match="exact tuple"):
        build_model_support_sta_ids([10], crosswalk)  # type: ignore[arg-type]
    with pytest.raises(ModelSnapshotContractError, match="중복"):
        canonical_station_categories_bytes((10, 10))


@pytest.mark.parametrize(
    "sta_id",
    (
        "ST-",
        "ST-station",
        "station-10",
        "ST-１０",
        "ST-10A",
    ),
)
def test_station_crosswalk_rejects_ids_outside_gold_ddl_pattern(sta_id: str) -> None:
    """Crosswalk ID는 Gold DDL의 exact ``^ST-[0-9]+$`` 계약을 따라야 한다."""
    with pytest.raises(ModelSnapshotContractError, match="ASCII 숫자"):
        StationCrosswalkEntry(10, sta_id)


def test_support_derivation_rejects_payload_or_support_ref_mismatch() -> None:
    """Current master lookup이나 임의 support digest로 pinned provenance를 바꾸지 못한다."""
    with pytest.raises(ModelSnapshotContractError, match="payload SHA-256"):
        derive_model_support_sta_ids(
            _manifest(),
            canonical_station_categories_bytes((10,)),
            build_station_crosswalk(
                (StationCrosswalkEntry(10, "ST-10"),)
            ).canonical_bytes,
        )


def test_effective_contract_uses_serving_subset_not_full_profile_bytes() -> None:
    """Training/LGB 차이를 제외한 7-key subset으로 pair contract를 정한다."""
    rental_profile = _effective_profile_payload()
    return_profile = _effective_profile_payload(
        TRAIN_LOOKBACK_MONTHS=6,
        LGB_PARAMS_COMMON={"learning_rate": 0.1, "num_leaves": 31},
    )
    assert rental_profile != return_profile
    assert extract_serving_feature_contract_bytes(rental_profile) == (
        extract_serving_feature_contract_bytes(return_profile)
    )
    version = effective_contract_version_from_profile(rental_profile)
    assert version == effective_contract_version_from_profile(return_profile)
    assert tuple(SERVING_FEATURE_PROFILE_KEYS) == (
        "ROLLING_TICK_MINUTES",
        "ROLLING_WINDOW_MINUTES",
        "ROLLING_EMBARGO_MINUTES",
        "TARGET_HORIZON_MINUTES",
        "GRID_TICK_MINUTES",
        "TRAIN_ANCHOR_TICK_MINUTES",
        "HORIZON_COUNT",
    )

    artifacts = list(_manifest().artifacts)
    index = MODEL_ARTIFACT_ROLES.index("effective_profile")
    checksum = sha256_hex(rental_profile)
    artifacts[index] = ModelArtifact(
        checksum,
        "effective_profile",
        f"s3://fixture/models/rental/effective_profile/sha256={checksum}.json",
    )
    manifest = build_model_snapshot_manifest(
        model_kind=ModelKind.RENTAL,
        effective_contract_version=version,
        artifacts=artifacts,
        support_sta_ids=_support(),
    )
    assert validate_model_effective_contract_binding(
        manifest,
        rental_profile,
    ) == extract_serving_feature_contract_bytes(rental_profile)

    wrong_contract_manifest = build_model_snapshot_manifest(
        model_kind=ModelKind.RENTAL,
        effective_contract_version="sha256:" + "0" * 64,
        artifacts=artifacts,
        support_sta_ids=_support(),
    )
    with pytest.raises(ModelSnapshotContractError, match="7-key canonical subset"):
        validate_model_effective_contract_binding(
            wrong_contract_manifest,
            rental_profile,
        )

    changed = _effective_profile_payload(TARGET_HORIZON_MINUTES=30)
    assert effective_contract_version_from_profile(changed) != version
    with pytest.raises(ModelSnapshotContractError, match="payload SHA-256"):
        validate_model_effective_contract_binding(manifest, changed)
