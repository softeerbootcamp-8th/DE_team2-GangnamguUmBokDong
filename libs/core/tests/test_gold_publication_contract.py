"""Gold publication v1 typed document와 registry 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from core.gold_publication.canonical import canonical_json_bytes
from core.gold_publication.contract import (
    EMPTY_ARTIFACT_SET_SHA256,
    PUBLICATION_REGISTRY,
    Artifact,
    ArtifactSet,
    Dependency,
    EmptyPolicy,
    IdSet,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PublicationManifest,
    build_artifact_set,
    build_id_set,
    build_input_fingerprint,
    build_publication_manifest,
    parse_artifact_set,
    parse_id_set,
    parse_input_fingerprint,
    parse_publication_manifest,
)
from core.gold_publication.errors import ContractViolation

_UTC_1555 = datetime(2026, 8, 19, 15, 55, tzinfo=UTC)
_UTC_1600 = datetime(2026, 8, 19, 16, 0, tzinfo=UTC)

_ROUTE_ARTIFACT_SET_BYTES = (
    b'{"artifacts":[{"byte_sha256":"cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",'
    b'"role":"route_stops","row_count":1,"uri":"s3://fixture/route-stops.parquet"},'
    b'{"byte_sha256":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",'
    b'"role":"routes","row_count":1,"uri":"s3://fixture/routes.parquet"}],'
    b'"schema_version":"gold-artifact-set-v1"}'
)
_ROUTE_ARTIFACT_SET_SHA256 = (
    "576eec2c53f1be8985ce531f512f4f4014fe05879d1f53714128dd774d8abf87"
)
_ROUTE_INPUT_SHA256 = "28fb19c7b43c003042c2564d62b8e735e9ac2df4852e6b0591dee05d6a10dbf0"
_ID_SET_BYTES = b'{"ids":["ST-1","ST-2"],"schema_version":"gold-id-set-v1"}'
_ID_SET_SHA256 = "a080d2f47ea7c4d0f5d27704264ed23d5a93ec525dd12544812f81b3519fa52f"
_ROUTE_MANIFEST_SHA256 = (
    "003d0cbcfe90537d1fd9562bf36e62e00f80391e1b6954e777f14985de5d8545"
)


def _dependency(
    publication_key: str,
    artifact_hash: str,
    input_hash: str,
    logical_dttm: datetime,
    manifest_uri: str,
) -> Dependency:
    """테스트용 publication dependency를 만든다."""
    return Dependency(
        artifact_set_sha256=artifact_hash,
        input_fingerprint_sha256=input_hash,
        logical_dttm=logical_dttm,
        manifest_uri=manifest_uri,
        publication_key=publication_key,
        revision_no=0,
    )


def _route_artifact_set() -> ArtifactSet:
    """SSOT route artifact set 회귀 fixture를 만든다."""
    return build_artifact_set(
        (
            Artifact("d" * 64, "routes", 1, "s3://fixture/routes.parquet"),
            Artifact("c" * 64, "route_stops", 1, "s3://fixture/route-stops.parquet"),
        )
    )


def _route_input_fingerprint() -> InputFingerprint:
    """SSOT route input fingerprint 회귀 fixture를 만든다."""
    dependencies = (
        _dependency(
            "station_urgency",
            "5" * 64,
            "6" * 64,
            _UTC_1600,
            "s3://fixture/urgency-publication.json",
        ),
        _dependency(
            "station_stock",
            "9" * 64,
            "a" * 64,
            _UTC_1600,
            "s3://fixture/stock-publication.json",
        ),
        _dependency(
            "station_demand_forecast",
            "7" * 64,
            "8" * 64,
            _UTC_1600,
            "s3://fixture/demand-publication.json",
        ),
        _dependency(
            "station",
            "3" * 64,
            "4" * 64,
            _UTC_1555,
            "s3://fixture/station-publication.json",
        ),
        _dependency(
            "dispatch_center",
            "1" * 64,
            "2" * 64,
            _UTC_1555,
            "s3://fixture/dispatch-center-publication.json",
        ),
    )
    artifacts = (
        InputArtifact(
            "b" * 64,
            "urgency_publication_manifest",
            "s3://fixture/urgency-publication.json",
        ),
        InputArtifact(
            "13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b",
            "route_coverage",
            "s3://fixture/route-coverage.json",
        ),
    )
    parameters = (
        Parameter("max_routes_per_center", "3"),
        Parameter("max_stops_per_route", "8"),
        Parameter("rebalance_policy_config", "{}"),
        Parameter("truck_capacity_config_version", "truck-capacity-v1"),
        Parameter("truck_capacity", "20"),
        Parameter(
            "route_coverage_sha256",
            "13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b",
        ),
        Parameter("route_work_unit_config_version", "route-work-unit-v1"),
        Parameter("route_algorithm_version", "route-v2"),
    )
    return build_input_fingerprint(
        "rebalance_route",
        dependencies,
        artifacts,
        parameters,
    )


def _station_fingerprint(*optional_roles: InputArtifact) -> InputFingerprint:
    """station conditional role 검증용 fingerprint를 만든다."""
    return build_input_fingerprint(
        "station",
        (
            _dependency(
                "weather_grid",
                "1" * 64,
                "2" * 64,
                _UTC_1555,
                "s3://fixture/weather-grid-publication.json",
            ),
            _dependency(
                "dispatch_center",
                "3" * 64,
                "4" * 64,
                _UTC_1555,
                "s3://fixture/dispatch-center-publication.json",
            ),
        ),
        (
            InputArtifact(
                "5" * 64, "station_realtime_window_set", "s3://fixture/windows.json"
            ),
            InputArtifact(
                "6" * 64,
                "bike_station_master_manifest",
                "s3://fixture/master.json",
            ),
            *optional_roles,
        ),
        (
            Parameter("station_policy_version", "station-v1"),
            Parameter("grid_conversion_version", "lcc-v1"),
            Parameter("center_assignment_version", "nearest-v1"),
        ),
    )


def _event_fingerprint() -> InputFingerprint:
    """EMPTY 허용 event fingerprint를 만든다."""
    return build_input_fingerprint(
        "event:cultural_event",
        (),
        (
            InputArtifact(
                "1" * 64,
                "cultural_event_manifest",
                "s3://fixture/cultural-event.json",
            ),
        ),
        (
            Parameter("event_policy_version", "event-v1"),
            Parameter("event_identity_version", "identity-v1"),
        ),
    )


def _demand_fingerprint() -> InputFingerprint:
    """조건부 EMPTY demand fingerprint를 만든다."""
    return build_input_fingerprint(
        "station_demand_forecast",
        (
            _dependency(
                "station",
                "1" * 64,
                "2" * 64,
                _UTC_1600,
                "s3://fixture/station.json",
            ),
        ),
        (
            InputArtifact(
                "3" * 64, "inference_output", "s3://fixture/inference.parquet"
            ),
            InputArtifact(
                "4" * 64, "rental_model_manifest", "s3://fixture/rental.json"
            ),
            InputArtifact(
                "5" * 64, "return_model_manifest", "s3://fixture/return.json"
            ),
        ),
        (
            Parameter("rounding_mode", "roundTiesToEven"),
            Parameter("quantile_policy_decision", "{}"),
            Parameter("horizon_count", "12"),
            Parameter("expected_sta_id_sha256", _ID_SET_SHA256),
        ),
    )


def test_artifact_set_matches_document_regression_vector() -> None:
    """artifact set의 bytes·SHA와 EMPTY SHA를 SSOT 회귀값으로 고정한다."""
    artifact_set = _route_artifact_set()

    assert artifact_set.canonical_bytes == _ROUTE_ARTIFACT_SET_BYTES
    assert artifact_set.sha256 == _ROUTE_ARTIFACT_SET_SHA256
    assert parse_artifact_set(_ROUTE_ARTIFACT_SET_BYTES) == artifact_set
    assert build_artifact_set(()).sha256 == EMPTY_ARTIFACT_SET_SHA256


def test_input_fingerprint_matches_document_regression_vector() -> None:
    """route input fingerprint의 canonical bytes SHA를 SSOT 회귀값으로 고정한다."""
    fingerprint = _route_input_fingerprint()

    assert fingerprint.sha256 == _ROUTE_INPUT_SHA256
    assert (
        parse_input_fingerprint(fingerprint.canonical_bytes, "rebalance_route")
        == fingerprint
    )


def test_id_set_matches_document_regression_vector() -> None:
    """ID set builder의 UTF-8 정렬과 SSOT bytes·SHA를 고정한다."""
    id_set = build_id_set(("ST-2", "ST-1"))

    assert id_set.canonical_bytes == _ID_SET_BYTES
    assert id_set.sha256 == _ID_SET_SHA256
    assert parse_id_set(_ID_SET_BYTES) == id_set

    with pytest.raises(ContractViolation, match="scalar"):
        build_id_set("ST-1")


def test_manifest_has_typed_fields_and_matches_regression_vector() -> None:
    """13-key manifest builder의 typed field와 SSOT bytes SHA를 고정한다."""
    manifest = build_publication_manifest(
        publication_key="rebalance_route",
        artifact_set=_route_artifact_set(),
        input_fingerprint=_route_input_fingerprint(),
        input_fingerprint_uri="s3://fixture/route-input-fingerprint.json",
        logical_dttm=_UTC_1600,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"rebalance_route_stop": 1, "rebalance_route": 1},
    )

    assert manifest.logical_dttm == _UTC_1600
    assert manifest.publication_key == "rebalance_route"
    assert manifest.revision_no == 0
    assert manifest.published_row_cnt == 1
    assert manifest.sha256 == _ROUTE_MANIFEST_SHA256
    assert parse_publication_manifest(manifest.canonical_bytes) == manifest
    assert len(cast_manifest_document(manifest)) == 13


def cast_manifest_document(manifest: PublicationManifest) -> dict[str, object]:
    """테스트에서 manifest canonical bytes를 JSON object로 되돌린다."""
    from core.gold_publication.canonical import parse_canonical_json

    value = parse_canonical_json(manifest.canonical_bytes)
    assert isinstance(value, dict)
    return value


def test_registry_matches_all_ten_ssot_publication_keys() -> None:
    """10개 publication key의 exact dependency·입출력·대표 target·EMPTY를 고정한다."""
    expected = {
        "weather_grid": (
            (),
            ("weather_grid_seed",),
            ("expected_grid_count", "grid_seed_version"),
            ("weather_grid",),
            ("weather_grid",),
            "weather_grid",
            EmptyPolicy.FORBIDDEN,
        ),
        "dispatch_center": (
            (),
            ("dispatch_center_seed",),
            ("center_seed_version", "expected_center_count"),
            ("dispatch_center",),
            ("dispatch_center",),
            "dispatch_center",
            EmptyPolicy.FORBIDDEN,
        ),
        "station": (
            ("dispatch_center", "weather_grid"),
            (
                "bike_station_master_manifest",
                "station_previous_projection",
                "station_realtime_window_set",
                "station_relocation_approval",
            ),
            (
                "center_assignment_version",
                "grid_conversion_version",
                "station_policy_version",
            ),
            ("station",),
            ("station",),
            "station",
            EmptyPolicy.FORBIDDEN,
        ),
        "station_stock": (
            (),
            ("bike_station_realtime_manifest",),
            ("station_stock_policy_version",),
            ("station_stock",),
            ("station_stock",),
            "station_stock",
            EmptyPolicy.FORBIDDEN,
        ),
        "station_demand_forecast": (
            ("station",),
            ("inference_output", "rental_model_manifest", "return_model_manifest"),
            (
                "expected_sta_id_sha256",
                "horizon_count",
                "quantile_policy_decision",
                "rounding_mode",
            ),
            ("station_demand_forecast",),
            ("station_demand_forecast",),
            "station_demand_forecast",
            EmptyPolicy.CONDITIONAL,
        ),
        "weather_forecast": (
            ("station", "weather_grid"),
            ("short_term_manifest", "ultra_short_manifest"),
            ("forecast_hour_count", "resolver_version"),
            ("weather_forecast",),
            ("weather_forecast",),
            "weather_forecast",
            EmptyPolicy.CONDITIONAL,
        ),
        "event:cultural_event": (
            (),
            ("cultural_event_manifest",),
            ("event_identity_version", "event_policy_version"),
            ("event_cultural_event",),
            ("event",),
            "event",
            EmptyPolicy.ALLOWED,
        ),
        "event:performance_event": (
            (),
            ("performance_event_manifest", "stadium_coordinate_seed"),
            ("event_policy_version", "stadium_coordinate_version"),
            ("event_performance_event",),
            ("event",),
            "event",
            EmptyPolicy.ALLOWED,
        ),
        "station_urgency": (
            ("station", "station_demand_forecast", "station_stock"),
            (
                "demand_publication_manifest",
                "stock_history_manifest_01",
                "stock_history_manifest_02",
                "stock_history_manifest_03",
                "stock_history_manifest_04",
                "stock_history_manifest_05",
                "stock_publication_manifest",
                "urgency_output",
            ),
            (
                "expected_sta_id_sha256",
                "quantile_policy_decision",
                "rebalance_policy_config",
                "scoring_config_version",
                "stock_window_count",
            ),
            ("station_urgency",),
            ("station_urgency",),
            "station_urgency",
            EmptyPolicy.CONDITIONAL,
        ),
        "rebalance_route": (
            (
                "dispatch_center",
                "station",
                "station_demand_forecast",
                "station_stock",
                "station_urgency",
            ),
            ("route_coverage", "urgency_publication_manifest"),
            (
                "max_routes_per_center",
                "max_stops_per_route",
                "rebalance_policy_config",
                "route_algorithm_version",
                "route_coverage_sha256",
                "route_work_unit_config_version",
                "truck_capacity",
                "truck_capacity_config_version",
            ),
            ("route_stops", "routes"),
            ("rebalance_route", "rebalance_route_stop"),
            "rebalance_route",
            EmptyPolicy.ALLOWED,
        ),
    }

    assert set(PUBLICATION_REGISTRY) == set(expected)
    for key, expected_contract in expected.items():
        spec = PUBLICATION_REGISTRY[key]
        actual = (
            spec.dependencies,
            tuple(cardinality.role for cardinality in spec.input_roles),
            spec.parameter_names,
            spec.output_roles,
            spec.target_tables,
            spec.representative_target,
            spec.empty_policy,
        )
        assert actual == expected_contract


def test_station_conditional_roles_allow_zero_or_one_each() -> None:
    """station의 previous projection과 relocation approval만 0..1개 허용한다."""
    without_optional = _station_fingerprint()
    with_optional = _station_fingerprint(
        InputArtifact(
            "7" * 64,
            "station_previous_projection",
            "s3://fixture/previous.parquet",
        ),
        InputArtifact(
            "8" * 64,
            "station_relocation_approval",
            "s3://fixture/approval.json",
        ),
    )

    assert len(without_optional.input_artifacts) == 2
    assert len(with_optional.input_artifacts) == 4
    cardinalities = {
        role.role: (role.minimum, role.maximum, role.condition)
        for role in PUBLICATION_REGISTRY["station"].input_roles
    }
    assert cardinalities["station_previous_projection"][:2] == (0, 1)
    assert cardinalities["station_relocation_approval"][:2] == (0, 1)


def test_station_conditional_role_rejects_two_artifacts() -> None:
    """조건부 station role도 한 publication에 두 번 나오면 거부한다."""
    with pytest.raises(ContractViolation, match="cardinality"):
        _station_fingerprint(
            InputArtifact(
                "7" * 64,
                "station_previous_projection",
                "s3://fixture/previous-a.parquet",
            ),
            InputArtifact(
                "8" * 64,
                "station_previous_projection",
                "s3://fixture/previous-b.parquet",
            ),
        )


def test_fingerprint_rejects_missing_extra_and_duplicate_registry_inputs() -> None:
    """registry에 없는 role과 누락·중복 parameter를 모두 거부한다."""
    with pytest.raises(ContractViolation, match="허용되지 않은"):
        build_input_fingerprint(
            "weather_grid",
            (),
            (
                InputArtifact("1" * 64, "weather_grid_seed", "s3://fixture/grid.yaml"),
                InputArtifact("2" * 64, "extra", "s3://fixture/extra.json"),
            ),
            (
                Parameter("expected_grid_count", "34"),
                Parameter("grid_seed_version", "grid-v1"),
            ),
        )

    with pytest.raises(ContractViolation, match="parameter"):
        build_input_fingerprint(
            "weather_grid",
            (),
            (InputArtifact("1" * 64, "weather_grid_seed", "s3://fixture/grid.yaml"),),
            (Parameter("expected_grid_count", "34"),),
        )

    with pytest.raises(ContractViolation, match="중복 parameter"):
        build_input_fingerprint(
            "weather_grid",
            (),
            (InputArtifact("1" * 64, "weather_grid_seed", "s3://fixture/grid.yaml"),),
            (
                Parameter("expected_grid_count", "34"),
                Parameter("expected_grid_count", "34"),
                Parameter("grid_seed_version", "grid-v1"),
            ),
        )


def test_dependency_manifest_role_uri_must_match_dependency() -> None:
    """input manifest URI와 동명 dependency manifest_uri가 다르면 거부한다."""
    route = _route_input_fingerprint()
    mismatched = tuple(
        InputArtifact(artifact.byte_sha256, artifact.role, "s3://fixture/wrong.json")
        if artifact.role == "urgency_publication_manifest"
        else artifact
        for artifact in route.input_artifacts
    )

    with pytest.raises(ContractViolation, match="manifest_uri"):
        build_input_fingerprint(
            "rebalance_route",
            route.dependencies,
            mismatched,
            route.parameters,
        )


def test_builders_reject_duplicate_unsorted_and_non_nfc_values() -> None:
    """ID 중복과 raw 배열 역순 및 non-NFC 문자열을 fail-closed로 거부한다."""
    with pytest.raises(ContractViolation, match="중복 ID"):
        build_id_set(("ST-1", "ST-1"))

    unsorted = canonical_json_bytes(
        {"ids": ["ST-2", "ST-1"], "schema_version": "gold-id-set-v1"}
    )
    with pytest.raises(ContractViolation, match="오름차순"):
        parse_id_set(unsorted)

    with pytest.raises(ContractViolation, match="NFC"):
        Artifact("1" * 64, "e\u0301", 1, "s3://fixture/output.parquet")

    with pytest.raises(ContractViolation, match="noncharacter"):
        Artifact("1" * 64, "\ufdd0", 1, "s3://fixture/output.parquet")

    class EvilString(str):
        """정렬 bytes를 override하는 테스트용 문자열이다."""

        def encode(self, *_args: object, **_kwargs: object) -> bytes:
            """실제 문자열과 다른 정렬 bytes를 반환한다."""
            return b"z"

    class EvilInteger(int):
        """문자열 표현을 override하는 테스트용 integer다."""

        def __str__(self) -> str:
            """integer가 아닌 JSON token을 반환한다."""
            return "1.0"

    with pytest.raises(ContractViolation, match="문자열"):
        Artifact("1" * 64, EvilString("role"), 1, "s3://fixture/output.parquet")
    with pytest.raises(ContractViolation, match="integer"):
        Artifact("1" * 64, "role", EvilInteger(1), "s3://fixture/output.parquet")


def test_parsers_reject_extra_keys_and_noncanonical_bytes() -> None:
    """exact key가 아닌 문서와 canonical하지 않은 원문 bytes를 거부한다."""
    extra_key = canonical_json_bytes(
        {"artifacts": [], "extra": None, "schema_version": "gold-artifact-set-v1"}
    )
    with pytest.raises(ContractViolation, match="extra"):
        parse_artifact_set(extra_key)

    with pytest.raises(ContractViolation):
        parse_artifact_set(b'{"schema_version":"gold-artifact-set-v1", "artifacts":[]}')


def test_manifest_uses_representative_route_header_count() -> None:
    """route published_row_cnt는 stop 수가 아니라 header target 수를 따른다."""
    artifact_set = build_artifact_set(
        (
            Artifact("1" * 64, "route_stops", 7, "s3://fixture/stops.parquet"),
            Artifact("2" * 64, "routes", 2, "s3://fixture/routes.parquet"),
        )
    )
    manifest = build_publication_manifest(
        publication_key="rebalance_route",
        artifact_set=artifact_set,
        input_fingerprint=_route_input_fingerprint(),
        input_fingerprint_uri="s3://fixture/route-input.json",
        logical_dttm=_UTC_1600,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"rebalance_route": 2, "rebalance_route_stop": 7},
    )

    assert manifest.published_row_cnt == 2


def test_empty_policy_allows_forbids_and_requires_conditional_proof() -> None:
    """event·seed·demand의 allowed·forbidden·conditional EMPTY를 구분한다."""
    empty_artifacts = build_artifact_set(())
    event_manifest = build_publication_manifest(
        publication_key="event:cultural_event",
        artifact_set=empty_artifacts,
        input_fingerprint=_event_fingerprint(),
        input_fingerprint_uri="s3://fixture/event-input.json",
        logical_dttm=_UTC_1600,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"event": 0},
    )
    assert event_manifest.published_row_cnt == 0
    assert event_manifest.artifact_set_sha256 == EMPTY_ARTIFACT_SET_SHA256

    weather_input = build_input_fingerprint(
        "weather_grid",
        (),
        (InputArtifact("1" * 64, "weather_grid_seed", "s3://fixture/grid.yaml"),),
        (
            Parameter("expected_grid_count", "34"),
            Parameter("grid_seed_version", "grid-v1"),
        ),
    )
    with pytest.raises(ContractViolation, match="EMPTY를 금지"):
        build_publication_manifest(
            publication_key="weather_grid",
            artifact_set=empty_artifacts,
            input_fingerprint=weather_input,
            input_fingerprint_uri="s3://fixture/grid-input.json",
            logical_dttm=_UTC_1600,
            publisher_version="gold-publisher-v1",
            revision_no=0,
            target_row_counts={"weather_grid": 0},
        )

    with pytest.raises(ContractViolation, match="근거 확인"):
        build_publication_manifest(
            publication_key="station_demand_forecast",
            artifact_set=empty_artifacts,
            input_fingerprint=_demand_fingerprint(),
            input_fingerprint_uri="s3://fixture/demand-input.json",
            logical_dttm=_UTC_1600,
            publisher_version="gold-publisher-v1",
            revision_no=0,
            target_row_counts={"station_demand_forecast": 0},
        )

    demand_manifest = build_publication_manifest(
        publication_key="station_demand_forecast",
        artifact_set=empty_artifacts,
        input_fingerprint=_demand_fingerprint(),
        input_fingerprint_uri="s3://fixture/demand-input.json",
        logical_dttm=_UTC_1600,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"station_demand_forecast": 0},
        conditional_empty_proven=True,
    )
    assert demand_manifest.published_row_cnt == 0


def test_manifest_rejects_hash_count_and_target_contract_drift() -> None:
    """embedded artifact hash·행 수·target key drift를 모두 거부한다."""
    artifact_set = _route_artifact_set()
    common = {
        "artifacts": artifact_set.artifacts,
        "input_fingerprint_schema": "gold-input-fingerprint-v1",
        "input_fingerprint_sha256": _ROUTE_INPUT_SHA256,
        "input_fingerprint_uri": "s3://fixture/input.json",
        "logical_dttm": _UTC_1600,
        "publication_key": "rebalance_route",
        "publisher_version": "gold-publisher-v1",
        "revision_no": 0,
        "schema_version": "gold-publication-manifest-v1",
        "target_schema_version": "gold-postgis-v1",
    }

    with pytest.raises(ContractViolation, match="embedded artifacts"):
        PublicationManifest(
            artifact_set_sha256="0" * 64,
            published_row_cnt=1,
            target_row_counts={"rebalance_route": 1, "rebalance_route_stop": 1},
            **common,
        )

    with pytest.raises(ContractViolation, match="대표 target"):
        PublicationManifest(
            artifact_set_sha256=artifact_set.sha256,
            published_row_cnt=2,
            target_row_counts={"rebalance_route": 1, "rebalance_route_stop": 1},
            **common,
        )

    with pytest.raises(ContractViolation, match="target_row_counts key"):
        PublicationManifest(
            artifact_set_sha256=artifact_set.sha256,
            published_row_cnt=1,
            target_row_counts={"rebalance_route": 1},
            **common,
        )


def test_manifest_zero_representative_requires_all_targets_and_artifacts_empty() -> (
    None
):
    """route header가 0이면 stop과 artifact도 모두 EMPTY여야 한다."""
    artifact_set = build_artifact_set(
        (
            Artifact("1" * 64, "route_stops", 1, "s3://fixture/stops.parquet"),
            Artifact("2" * 64, "routes", 0, "s3://fixture/routes.parquet"),
        )
    )

    with pytest.raises(ContractViolation, match="EMPTY"):
        build_publication_manifest(
            publication_key="rebalance_route",
            artifact_set=artifact_set,
            input_fingerprint=_route_input_fingerprint(),
            input_fingerprint_uri="s3://fixture/route-input.json",
            logical_dttm=_UTC_1600,
            publisher_version="gold-publisher-v1",
            revision_no=0,
            target_row_counts={"rebalance_route": 0, "rebalance_route_stop": 1},
        )


def test_raw_manifest_parser_rejects_missing_thirteenth_field() -> None:
    """publication manifest에서 13개 중 하나라도 빠지면 거부한다."""
    manifest = build_publication_manifest(
        publication_key="rebalance_route",
        artifact_set=_route_artifact_set(),
        input_fingerprint=_route_input_fingerprint(),
        input_fingerprint_uri="s3://fixture/route-input-fingerprint.json",
        logical_dttm=_UTC_1600,
        publisher_version="gold-publisher-v1",
        revision_no=0,
        target_row_counts={"rebalance_route": 1, "rebalance_route_stop": 1},
    )
    document = cast_manifest_document(manifest)
    document.pop("target_schema_version")

    with pytest.raises(ContractViolation, match="missing"):
        parse_publication_manifest(canonical_json_bytes(document))


def test_direct_typed_documents_reject_mutable_or_unsorted_arrays() -> None:
    """직접 dataclass 생성도 mutable array와 contract 역순을 허용하지 않는다."""
    with pytest.raises(ContractViolation, match="tuple"):
        ArtifactSet("gold-artifact-set-v1", [])  # type: ignore[arg-type]

    with pytest.raises(ContractViolation, match="오름차순"):
        IdSet("gold-id-set-v1", ("ST-2", "ST-1"))
