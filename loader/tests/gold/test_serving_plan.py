"""Serving plan canonical contract와 activation gate를 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from core.gold_publication import (
    ContractViolation,
    Dependency,
    ImmutablePutOutcome,
    build_id_set,
    canonical_json_bytes,
    parse_canonical_json,
    sha256_hex,
)
from core.model_snapshot import IdSetArtifactRef, build_id_set_artifact_ref
from gold.serving_plan import (
    PreparedManifestRef,
    ServingPlan,
    SourceLookbacks,
    _activation_ready_station_ids,
    _store_id_set,
    parse_serving_plan,
)
from gold.station import StationProjection, StationRecord

_LOGICAL = datetime(2026, 8, 20, 0, 5, tzinfo=UTC)
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
        f"s3://fixture/{name}/sha256={id_set.sha256}.json",
    )


def _dependency() -> Dependency:
    """Incoming station prepared dependency를 만든다."""
    return Dependency(
        artifact_set_sha256=_SHA_A,
        input_fingerprint_sha256=_SHA_B,
        logical_dttm=_LOGICAL,
        manifest_uri=f"s3://fixture/station/publication-{_SHA_C}.json",
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
            rental_support_sta_ids=values.rental_support_sta_ids,
            return_support_sta_ids=values.return_support_sta_ids,
            prepared_publications=values.prepared_publications,
            prior_states=(),
            source_lookbacks=values.source_lookbacks,
        )
    with pytest.raises(ContractViolation, match="key 순서"):
        ServingPlan(
            logical_dttm=values.logical_dttm,
            object_base_uri=values.object_base_uri,
            station_dependency=values.station_dependency,
            activation_ready_sta_ids=values.activation_ready_sta_ids,
            expected_sta_ids=values.expected_sta_ids,
            rental_support_sta_ids=values.rental_support_sta_ids,
            return_support_sta_ids=values.return_support_sta_ids,
            prepared_publications=tuple(reversed(values.prepared_publications)),
            prior_states=(),
            source_lookbacks=values.source_lookbacks,
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


def test_activation_gate_only_marks_inactive_station_with_complete_weather(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """기존 active는 gate 밖이고 candidate는 singleton 13h coverage로만 준비된다."""
    projection = StationProjection(
        records=(
            _station("ST-1", "61_126", active=True),
            _station("ST-2", "62_126", active=False),
            _station("ST-3", "63_126", active=False),
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
    )

    assert ready == ("ST-2",)
