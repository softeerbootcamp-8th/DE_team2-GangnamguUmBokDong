"""Gold input fingerprint의 hash·manifest·조건부 결합을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from core.gold_publication.canonical import sha256_hex
from core.gold_publication.contract import (
    Artifact,
    Dependency,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PublicationManifest,
    build_artifact_set,
    build_id_set,
    build_input_fingerprint,
    build_publication_manifest,
    get_publication_spec,
    validate_id_set_parameter,
    validate_input_fingerprint,
    validate_linked_dependency_manifests,
    validate_route_urgency_dependencies,
    validate_station_conditional_inputs,
    validate_station_stock_release,
)
from core.gold_publication.documents import (
    StationRealtimeWindow,
    build_station_realtime_window_set,
)
from core.gold_publication.errors import ContractViolation

_LOGICAL_DTTM = datetime(2026, 8, 20, tzinfo=UTC)
_ID_SET_SHA256 = "a080d2f47ea7c4d0f5d27704264ed23d5a93ec525dd12544812f81b3519fa52f"


def test_hash_parameters_bind_route_coverage_and_expected_id_set() -> None:
    """hash parameter가 lowercase 형식과 실제 canonical document digest에 묶인다."""
    demand = _fingerprint("station_demand_forecast")
    validate_id_set_parameter(
        "station_demand_forecast", demand, build_id_set(("ST-2", "ST-1"))
    )
    with pytest.raises(ContractViolation, match="gold-id-set-v1"):
        validate_id_set_parameter(
            "station_demand_forecast", demand, build_id_set(("ST-1",))
        )

    class FakeIdSet:
        """임의 sha256 attribute만 가진 잘못된 ID-set 증거다."""

        sha256 = _ID_SET_SHA256

    with pytest.raises(ContractViolation, match="IdSet"):
        validate_id_set_parameter(
            "station_demand_forecast",
            demand,
            FakeIdSet(),  # type: ignore[arg-type]
        )

    route = _fingerprint("rebalance_route")
    coverage = next(
        artifact
        for artifact in route.input_artifacts
        if artifact.role == "route_coverage"
    )
    parameters = tuple(
        Parameter(parameter.name, "f" * 64)
        if parameter.name == "route_coverage_sha256"
        else parameter
        for parameter in route.parameters
    )
    assert coverage.byte_sha256 != "f" * 64
    with pytest.raises(ContractViolation, match="route_coverage_sha256"):
        build_input_fingerprint(
            "rebalance_route", route.dependencies, route.input_artifacts, parameters
        )


def test_expected_id_parameter_rejects_non_hash_string() -> None:
    """expected station ID digest에는 임의 문자열을 사용할 수 없다."""
    fingerprint = _fingerprint("station_demand_forecast")
    parameters = tuple(
        Parameter(parameter.name, "not-a-hash")
        if parameter.name == "expected_sta_id_sha256"
        else parameter
        for parameter in fingerprint.parameters
    )
    with pytest.raises(ContractViolation, match="lowercase"):
        build_input_fingerprint(
            "station_demand_forecast",
            fingerprint.dependencies,
            fingerprint.input_artifacts,
            parameters,
        )


def test_fingerprint_revalidation_rejects_mutated_nested_document() -> None:
    """frozen 자식 dataclass를 강제 변조해도 validator가 다시 거부한다."""
    fingerprint = _fingerprint("event:cultural_event")
    object.__setattr__(fingerprint.parameters[0], "value", 1)

    with pytest.raises(ContractViolation, match="문자열"):
        validate_input_fingerprint("event:cultural_event", fingerprint)


@pytest.mark.parametrize(
    ("previous_state_exists", "relocation_applied", "optional_roles", "message"),
    [
        (True, False, (), "previous_projection"),
        (False, True, (), "relocation_approval"),
        (
            False,
            False,
            ("station_previous_projection",),
            "previous_projection",
        ),
        (
            False,
            False,
            ("station_relocation_approval",),
            "relocation_approval",
        ),
    ],
)
def test_station_conditional_roles_require_actual_context(
    previous_state_exists: bool,
    relocation_applied: bool,
    optional_roles: tuple[str, ...],
    message: str,
) -> None:
    """station optional role의 단순 0..1을 state·실제 반영 조건으로 좁힌다."""
    fingerprint = _fingerprint("station", optional_roles=optional_roles)
    with pytest.raises(ContractViolation, match=message):
        validate_station_conditional_inputs(
            fingerprint,
            previous_state_exists=previous_state_exists,
            relocation_applied=relocation_applied,
        )


def test_linked_manifest_actual_bytes_match_dependency_tuple() -> None:
    """actual publication manifest bytes의 SHA와 5개 state field를 모두 대조한다."""
    urgency_manifest = _manifest("station_urgency", "urgency")
    urgency_dependency = _dependency_from_manifest(
        urgency_manifest, "s3://fixture/urgency-manifest.json"
    )
    route = _fingerprint(
        "rebalance_route", dependency_overrides={"station_urgency": urgency_dependency}
    )
    route = _replace_input_artifact(
        "rebalance_route",
        route,
        "urgency_publication_manifest",
        urgency_manifest.sha256,
        urgency_dependency.manifest_uri,
    )

    parsed = validate_linked_dependency_manifests(
        "rebalance_route",
        route,
        {"urgency_publication_manifest": urgency_manifest.canonical_bytes},
    )
    assert parsed["urgency_publication_manifest"] == urgency_manifest

    with pytest.raises(ContractViolation, match="bytes SHA"):
        validate_linked_dependency_manifests(
            "rebalance_route",
            route,
            {"urgency_publication_manifest": urgency_manifest.canonical_bytes + b" "},
        )

    wrong_dependency = Dependency(
        artifact_set_sha256=urgency_dependency.artifact_set_sha256,
        input_fingerprint_sha256=urgency_dependency.input_fingerprint_sha256,
        logical_dttm=urgency_dependency.logical_dttm,
        manifest_uri=urgency_dependency.manifest_uri,
        publication_key=urgency_dependency.publication_key,
        revision_no=urgency_dependency.revision_no + 1,
    )
    wrong_route = _fingerprint(
        "rebalance_route", dependency_overrides={"station_urgency": wrong_dependency}
    )
    wrong_route = _replace_input_artifact(
        "rebalance_route",
        wrong_route,
        "urgency_publication_manifest",
        urgency_manifest.sha256,
        wrong_dependency.manifest_uri,
    )
    with pytest.raises(ContractViolation, match="dependency tuple"):
        validate_linked_dependency_manifests(
            "rebalance_route",
            wrong_route,
            {"urgency_publication_manifest": urgency_manifest.canonical_bytes},
        )


def test_route_rejects_urgency_with_stale_nested_dependency() -> None:
    """route는 urgency 계산이 읽은 station·demand·stock tuple을 그대로 요구한다."""
    urgency = _fingerprint("station_urgency")
    route = _fingerprint(
        "rebalance_route",
        dependency_overrides={
            dependency.publication_key: dependency
            for dependency in urgency.dependencies
        },
    )
    validate_route_urgency_dependencies(route, urgency)

    changed_station = _dependency("station", salt="corrected")
    stale_route = _fingerprint(
        "rebalance_route",
        dependency_overrides={
            **{
                dependency.publication_key: dependency
                for dependency in urgency.dependencies
            },
            "station": changed_station,
        },
    )
    with pytest.raises(ContractViolation, match="station"):
        validate_route_urgency_dependencies(stale_route, urgency)


def test_station_and_stock_share_window_set_candidate_manifest() -> None:
    """station window-set의 첫 manifest가 같은 release의 stock input과 같아야 한다."""
    candidate = StationRealtimeWindow(
        sha256_hex(b"realtime-manifest"),
        _LOGICAL_DTTM,
        0,
        "s3://fixture/realtime-manifest.json",
    )
    window_set = build_station_realtime_window_set(
        (candidate,), expected_candidate=candidate
    )
    station = _replace_input_artifact(
        "station",
        _fingerprint("station"),
        "station_realtime_window_set",
        window_set.sha256,
        "s3://fixture/window-set.json",
    )
    stock = _replace_input_artifact(
        "station_stock",
        _fingerprint("station_stock"),
        "bike_station_realtime_manifest",
        candidate.byte_sha256,
        candidate.uri,
    )

    validate_station_stock_release(station, stock, window_set)

    mismatched_stock = _replace_input_artifact(
        "station_stock",
        stock,
        "bike_station_realtime_manifest",
        sha256_hex(b"other-manifest"),
        "s3://fixture/other-manifest.json",
    )
    with pytest.raises(ContractViolation, match="첫 candidate"):
        validate_station_stock_release(station, mismatched_stock, window_set)


def _fingerprint(
    publication_key: str,
    *,
    dependency_overrides: dict[str, Dependency] | None = None,
    optional_roles: tuple[str, ...] = (),
) -> InputFingerprint:
    """registry를 만족하는 결정적인 input fingerprint fixture를 만든다."""
    spec = get_publication_spec(publication_key)
    overrides = dependency_overrides or {}
    dependencies = tuple(
        overrides.get(key, _dependency(key)) for key in spec.dependencies
    )
    dependency_by_key = {
        dependency.publication_key: dependency for dependency in dependencies
    }
    manifest_bindings = {
        "demand_publication_manifest": "station_demand_forecast",
        "stock_publication_manifest": "station_stock",
        "urgency_publication_manifest": "station_urgency",
    }
    artifacts: list[InputArtifact] = []
    for cardinality in spec.input_roles:
        if cardinality.minimum == 0 and cardinality.role not in optional_roles:
            continue
        dependency_key = manifest_bindings.get(cardinality.role)
        uri = (
            dependency_by_key[dependency_key].manifest_uri
            if dependency_key is not None
            else f"s3://fixture/{publication_key}/{cardinality.role}.json"
        )
        artifacts.append(
            InputArtifact(
                sha256_hex(f"{publication_key}:{cardinality.role}".encode()),
                cardinality.role,
                uri,
            )
        )
    parameters = []
    for name in spec.parameter_names:
        if name == "expected_sta_id_sha256":
            value = _ID_SET_SHA256
        elif name == "route_coverage_sha256":
            value = next(
                artifact.byte_sha256
                for artifact in artifacts
                if artifact.role == "route_coverage"
            )
        else:
            value = f"fixture-{name}"
        parameters.append(Parameter(name, value))
    return build_input_fingerprint(publication_key, dependencies, artifacts, parameters)


def _dependency(publication_key: str, *, salt: str = "base") -> Dependency:
    """고정된 publication state dependency fixture를 만든다."""
    return Dependency(
        sha256_hex(f"{salt}:{publication_key}:artifacts".encode()),
        sha256_hex(f"{salt}:{publication_key}:inputs".encode()),
        _LOGICAL_DTTM,
        f"s3://fixture/{salt}/{publication_key}-manifest.json",
        publication_key,
        0,
    )


def _manifest(publication_key: str, salt: str) -> PublicationManifest:
    """실제 canonical bytes를 가진 valid publication manifest를 만든다."""
    spec = get_publication_spec(publication_key)
    fingerprint = _fingerprint(publication_key)
    target_counts = {target: 1 for target in spec.target_tables}
    artifact_set = build_artifact_set(
        Artifact(
            sha256_hex(f"{salt}:{role}:output".encode()),
            role,
            target_counts[target],
            f"s3://fixture/{salt}/{role}.parquet",
        )
        for role, target in spec.output_targets
    )
    return build_publication_manifest(
        publication_key=publication_key,
        artifact_set=artifact_set,
        input_fingerprint=fingerprint,
        input_fingerprint_uri=f"s3://fixture/{salt}/fingerprint.json",
        logical_dttm=_LOGICAL_DTTM,
        publisher_version="fixture-v1",
        revision_no=0,
        target_row_counts=target_counts,
    )


def _dependency_from_manifest(
    manifest: PublicationManifest,
    manifest_uri: str,
) -> Dependency:
    """manifest의 state identity로 dependency를 만든다."""
    return Dependency(
        manifest.artifact_set_sha256,
        manifest.input_fingerprint_sha256,
        manifest.logical_dttm,
        manifest_uri,
        manifest.publication_key,
        manifest.revision_no,
    )


def _replace_input_artifact(
    publication_key: str,
    fingerprint: InputFingerprint,
    role: str,
    byte_sha256: str,
    uri: str,
) -> InputFingerprint:
    """한 input role의 실제 bytes identity를 바꿔 fingerprint를 다시 만든다."""
    artifacts = tuple(
        InputArtifact(byte_sha256, role, uri) if artifact.role == role else artifact
        for artifact in fingerprint.input_artifacts
    )
    return build_input_fingerprint(
        publication_key,
        fingerprint.dependencies,
        artifacts,
        fingerprint.parameters,
    )
