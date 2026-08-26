"""Serving plan canonical contract와 activation gate를 검증한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace
from typing import Any

import pytest
from core.gold_publication import (
    ContractViolation,
    Dependency,
    ImmutablePutOutcome,
    InputArtifact,
    build_id_set,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from core.inference_snapshot import (
    ImmutableInputRef,
    InferenceSnapshotStatus,
    ServingReleaseRef,
)
from core.model_snapshot import IdSetArtifactRef, build_id_set_artifact_ref
from core.serving_plan_input import read_serving_plan_inference_inputs
from gold.serving_plan import (
    LEGACY_SERVING_PLAN_SCHEMA_VERSION,
    PreparedManifestRef,
    ServingPlan,
    ServingPlanArtifact,
    SourceLookbacks,
    _activation_ready_station_ids,
    _inference_expected_station_ids,
    _store_id_set,
    _validate_shared_identity_binding,
    parse_serving_plan,
)
from gold.station import StationProjection, StationRecord

_LOGICAL = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)


def test_expected_ids_isolate_small_known_quality_gap_but_reject_large_gap() -> None:
    """1% 이내 결측은 제외하고 이를 넘는 품질 저하는 plan 실패로 처리한다."""
    candidates = tuple(f"ST-{number:03d}" for number in range(100))

    expected = _inference_expected_station_ids(
        candidates, candidates, candidates, candidates[:-1]
    )

    assert expected == candidates[:-1]
    with pytest.raises(ContractViolation, match="제외율이 기준을 초과"):
        _inference_expected_station_ids(
            candidates, candidates, candidates, candidates[:-2]
        )
_SHA_A = "a" * 64
_SHA_B = "b" * 64
_SHA_C = "c" * 64


class _MemoryStore:
    """Plan unit test가 쓰는 exact immutable byte store다."""

    def __init__(self) -> None:
        """빈 object mapping을 만든다."""
        self.objects: dict[str, bytes] = {}

    def put_once(
        self,
        uri: str,
        payload: bytes,
        *,
        expected_sha256: str | None = None,
        require_canonical_json: bool = False,
    ) -> ImmutablePutOutcome:
        """같은 URI·bytes 재시도만 허용한다."""
        if expected_sha256 is not None:
            assert sha256_hex(payload) == expected_sha256
        if require_canonical_json:
            assert canonical_json_bytes(parse_canonical_json(payload)) == payload
        previous = self.objects.setdefault(uri, payload)
        assert previous == payload
        return (
            ImmutablePutOutcome.CREATED
            if previous is payload
            else ImmutablePutOutcome.ALREADY_EXISTS
        )

    def read_bytes(
        self,
        uri: str,
        expected_sha256: str,
        *,
        require_canonical_json: bool = False,
    ) -> bytes:
        """URI·SHA가 같은 exact bytes를 반환한다."""
        payload = self.objects[uri]
        assert sha256_hex(payload) == expected_sha256
        if require_canonical_json:
            assert canonical_json_bytes(parse_canonical_json(payload)) == payload
        return payload


def _id_ref(name: str, values: tuple[str, ...]) -> IdSetArtifactRef:
    """Content-addressed test ID set reference를 만든다."""
    id_set = build_id_set(values)
    return build_id_set_artifact_ref(
        id_set,
        f"s3://fixture/gold/{name}/sha256={id_set.sha256}.json",
    )


def _dependency() -> Dependency:
    """Incoming station prepared dependency를 만든다."""
    return Dependency(
        artifact_set_sha256=_SHA_A,
        input_fingerprint_sha256=_SHA_B,
        logical_dttm=_LOGICAL,
        manifest_uri=f"s3://fixture/gold/station/publication-{_SHA_C}.json",
        publication_key="station",
        revision_no=2,
    )


def _plan() -> ServingPlan:
    """Canonical round-trip용 최소 clean-bootstrap plan을 만든다."""
    activation = _id_ref("activation", ("ST-1", "ST-2"))
    expected = _id_ref("expected", ("ST-1",))
    rental = _id_ref("rental", ("ST-1", "ST-3"))
    returned = _id_ref("return", ("ST-1", "ST-4"))
    return ServingPlan(
        logical_dttm=_LOGICAL,
        object_base_uri="s3://fixture/gold",
        station_dependency=_dependency(),
        activation_ready_sta_ids=activation,
        expected_sta_ids=expected,
        inference_eligible_sta_ids=activation,
        rental_support_sta_ids=rental,
        return_support_sta_ids=returned,
        prepared_publications=(
            PreparedManifestRef(
                "station",
                f"s3://fixture/station/publication-{_SHA_A}.json",
                _SHA_A,
            ),
            PreparedManifestRef(
                "station_stock",
                f"s3://fixture/stock/publication-{_SHA_B}.json",
                _SHA_B,
            ),
            PreparedManifestRef(
                "weather_forecast",
                f"s3://fixture/weather/publication-{_SHA_C}.json",
                _SHA_C,
            ),
        ),
        prior_states=(),
        source_lookbacks=SourceLookbacks(
            master=timedelta(hours=48),
            realtime=timedelta(hours=24),
            short_term=timedelta(hours=3),
            ultra_short=timedelta(minutes=30),
        ),
        serving_release=ServingReleaseRef(
            byte_sha256=_SHA_C,
            effective_contract_version=f"sha256:{_SHA_A}",
            release_version=f"sha256:{_SHA_B}",
            uri=f"s3://fixture/models/serving-release/manifests/sha256={_SHA_C}.json",
        ),
        station_master_enriched=InputArtifact(
            byte_sha256=_SHA_B,
            role="station_master_enriched",
            uri=(
                "s3://fixture/silver/station_master_enriched/"
                "dt=2026-08-20/hh=00/0005.parquet"
            ),
        ),
    )


def _station(
    station_id: str,
    grid_id: str,
    *,
    active: bool,
) -> StationRecord:
    """Activation gate test용 station record를 만든다."""
    return StationRecord(
        sta_id=station_id,
        sta_nm=f"대여소 {station_id}",
        sta_addr="서울시 강남구",
        hold_cnt=20,
        longitude=127.0473,
        latitude=37.5172,
        sta_point_source_cd="bike_station_master",
        weather_grid_id=grid_id,
        dispatch_center_id="gangnam",
        master_base_dttm=_LOGICAL - timedelta(days=1),
        last_seen_dttm=_LOGICAL,
        is_active=active,
    )


def test_serving_plan_canonical_round_trip_and_exact_keys() -> None:
    """Plan bytes가 deterministic round-trip하고 unknown key를 거부한다."""
    plan = _plan()

    assert parse_serving_plan(plan.canonical_bytes) == plan
    document = parse_canonical_json(plan.canonical_bytes)
    assert isinstance(document, dict)
    document["unknown"] = True
    with pytest.raises(ContractViolation, match="key가 정확하지"):
        parse_serving_plan(canonical_json_bytes(document))


def test_serving_plan_artifact_exposes_inference_compatible_exact_ref() -> None:
    """Stored plan URI를 inference ServingPlanRef의 sha256= 형식에 고정한다."""
    plan = _plan()
    uri = f"s3://fixture/gold/serving-plan/plans/sha256={plan.sha256}.json"
    artifact = ServingPlanArtifact(plan, uri, plan.sha256)

    assert artifact.serving_plan_ref.byte_sha256 == plan.sha256
    assert artifact.serving_plan_ref.uri == uri
    with pytest.raises(ContractViolation):
        ServingPlanArtifact(
            plan,
            f"s3://fixture/gold/serving-plan-{plan.sha256}.json",
            plan.sha256,
        )


def test_core_inference_extractor_matches_loader_canonical_plan() -> None:
    """Loader actual canonical plan과 core inference subset parser drift를 막는다."""
    store = _MemoryStore()
    plan = _plan()
    plan_uri = f"s3://fixture/gold/serving-plan/plans/sha256={plan.sha256}.json"
    expected = build_id_set(("ST-1",))
    store.put_once(
        plan.expected_sta_ids.uri,
        expected.canonical_bytes,
        expected_sha256=expected.sha256,
        require_canonical_json=True,
    )
    store.put_once(
        plan_uri,
        plan.canonical_bytes,
        expected_sha256=plan.sha256,
        require_canonical_json=True,
    )

    extracted = read_serving_plan_inference_inputs(
        store,  # type: ignore[arg-type]
        plan_uri=plan_uri,
        plan_sha256=plan.sha256,
    )

    assert extracted.logical_dttm == plan.logical_dttm
    assert extracted.object_base_uri == plan.object_base_uri
    assert extracted.station_dependency == plan.station_dependency
    assert extracted.expected_sta_ids_ref == plan.expected_sta_ids
    assert extracted.expected_sta_ids == expected
    assert extracted.serving_release == plan.serving_release
    assert extracted.station_master_enriched == plan.station_master_enriched
    assert (
        extracted.serving_plan
        == ServingPlanArtifact(
            plan,
            plan_uri,
            plan.sha256,
        ).serving_plan_ref
    )


def test_core_inference_extractor_rejects_cross_bucket_expected_ref() -> None:
    """Plan bytes가 다른 bucket의 expected ID authority를 주입하지 못하게 한다."""
    store = _MemoryStore()
    document = parse_canonical_json(_plan().canonical_bytes)
    assert isinstance(document, dict)
    expected = document["expected_sta_ids"]
    assert isinstance(expected, dict)
    expected["uri"] = expected["uri"].replace("s3://fixture/", "s3://other/")
    payload = canonical_json_bytes(document)
    digest = sha256_hex(payload)
    plan_uri = f"s3://fixture/gold/serving-plan/plans/sha256={digest}.json"
    store.put_once(
        plan_uri,
        payload,
        expected_sha256=digest,
        require_canonical_json=True,
    )

    with pytest.raises(ContractViolation, match="object base 밖"):
        read_serving_plan_inference_inputs(
            store,  # type: ignore[arg-type]
            plan_uri=plan_uri,
            plan_sha256=digest,
        )


def test_legacy_v2_plan_replays_in_finalize_but_cannot_start_new_inference() -> None:
    """기존 v2 finalize 호환은 유지하고 신규 unpinned inference는 거부한다."""
    store = _MemoryStore()
    legacy = replace(
        _plan(),
        schema_version=LEGACY_SERVING_PLAN_SCHEMA_VERSION,
        serving_release=None,
        station_master_enriched=None,
    )
    plan_uri = f"s3://fixture/gold/serving-plan/plans/sha256={legacy.sha256}.json"
    store.put_once(
        plan_uri,
        legacy.canonical_bytes,
        expected_sha256=legacy.sha256,
        require_canonical_json=True,
    )

    assert parse_serving_plan(legacy.canonical_bytes) == legacy
    _validate_shared_identity_binding(legacy, object())
    with pytest.raises(ContractViolation, match="prepare를 다시 실행"):
        read_serving_plan_inference_inputs(
            store,  # type: ignore[arg-type]
            plan_uri=plan_uri,
            plan_sha256=legacy.sha256,
        )


def test_v3_finalize_rejects_release_and_enriched_master_drift() -> None:
    """Plan 뒤 shared identity가 바뀐 inference manifest를 fail-closed한다."""
    plan = _plan()
    assert plan.serving_release is not None
    assert plan.station_master_enriched is not None
    wrong_release = replace(plan.serving_release, byte_sha256="d" * 64, uri=(
        "s3://fixture/models/serving-release/manifests/"
        + "sha256="
        + "d" * 64
        + ".json"
    ))
    with pytest.raises(ContractViolation, match="serving release"):
        _validate_shared_identity_binding(
            plan,
            SimpleNamespace(
                serving_release=wrong_release,
                status=InferenceSnapshotStatus.SUCCEEDED,
                inputs=(),
            ),
        )

    with pytest.raises(ContractViolation, match="station_master_enriched bytes"):
        _validate_shared_identity_binding(
            plan,
            SimpleNamespace(
                serving_release=plan.serving_release,
                status=InferenceSnapshotStatus.SUCCEEDED,
                inputs=(
                    ImmutableInputRef(
                        byte_sha256="d" * 64,
                        role="station_master_enriched",
                        uri=(
                            "s3://fixture/gold/inference/inputs/"
                            "station-master/sha256="
                            + "d" * 64
                            + ".parquet"
                        ),
                    ),
                ),
            ),
        )


def test_plan_requires_same_anchor_station_dependency_and_prepared_order() -> None:
    """Inference station tuple과 prepared key 순서가 흔들리면 plan을 거부한다."""
    values = _plan()
    with pytest.raises(ContractViolation, match="같은 anchor"):
        ServingPlan(
            logical_dttm=_LOGICAL + timedelta(minutes=5),
            object_base_uri=values.object_base_uri,
            station_dependency=values.station_dependency,
            activation_ready_sta_ids=values.activation_ready_sta_ids,
            expected_sta_ids=values.expected_sta_ids,
            inference_eligible_sta_ids=values.inference_eligible_sta_ids,
            rental_support_sta_ids=values.rental_support_sta_ids,
            return_support_sta_ids=values.return_support_sta_ids,
            prepared_publications=values.prepared_publications,
            prior_states=(),
            source_lookbacks=values.source_lookbacks,
            serving_release=values.serving_release,
            station_master_enriched=values.station_master_enriched,
        )
    with pytest.raises(ContractViolation, match="key 순서"):
        ServingPlan(
            logical_dttm=values.logical_dttm,
            object_base_uri=values.object_base_uri,
            station_dependency=values.station_dependency,
            activation_ready_sta_ids=values.activation_ready_sta_ids,
            expected_sta_ids=values.expected_sta_ids,
            inference_eligible_sta_ids=values.inference_eligible_sta_ids,
            rental_support_sta_ids=values.rental_support_sta_ids,
            return_support_sta_ids=values.return_support_sta_ids,
            prepared_publications=tuple(reversed(values.prepared_publications)),
            prior_states=(),
            source_lookbacks=values.source_lookbacks,
            serving_release=values.serving_release,
            station_master_enriched=values.station_master_enriched,
        )


def test_plan_id_set_uses_model_contract_content_addressed_filename() -> None:
    """Inference expected ID set을 model contract가 받는 sha256= 이름으로 저장한다."""
    store = _MemoryStore()
    reference = _store_id_set(
        store,  # type: ignore[arg-type]
        object_base_uri="s3://fixture/gold",
        name="expected-sta-ids",
        values=("ST-1", "ST-2"),
    )

    assert reference.uri.endswith(f"/sha256={reference.byte_sha256}.json")
    assert reference.id_count == 2
    assert store.read_bytes(
        reference.uri,
        reference.byte_sha256,
        require_canonical_json=True,
    )


def test_activation_gate_requires_complete_weather_and_current_stock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Candidate는 singleton 13h weather와 current stock이 모두 있어야 준비된다."""
    projection = StationProjection(
        records=(
            _station("ST-1", "61_126", active=True),
            _station("ST-2", "62_126", active=False),
            _station("ST-3", "63_126", active=False),
            _station("ST-4", "64_126", active=False),
        ),
        relocation_applied=False,
        lkg_retained_count=0,
    )

    def resolve(*_args: Any, active_grids: tuple[str, ...], **_kwargs: Any) -> Any:
        """62_126만 complete이고 63_126은 13h 한 칸이 부족하다고 흉내 낸다."""
        if active_grids == ("63_126",):
            raise ContractViolation(
                "weather active grid×13시간 coverage가 완전하지 않습니다"
            )
        return object()

    monkeypatch.setattr(
        "gold.serving_plan._weather_projection_from_artifacts",
        resolve,
    )
    ready = _activation_ready_station_ids(
        object(),  # type: ignore[arg-type]
        provisional=projection,
        short_term_artifact=object(),  # type: ignore[arg-type]
        ultra_short_artifact=object(),  # type: ignore[arg-type]
        short_input=object(),  # type: ignore[arg-type]
        ultra_input=object(),  # type: ignore[arg-type]
        anchor=_LOGICAL,
        current_stock_ids=("ST-1", "ST-2", "ST-3"),
    )

    assert ready == ("ST-1", "ST-2")
