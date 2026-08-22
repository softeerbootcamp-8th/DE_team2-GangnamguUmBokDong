"""Gold 재배치 경로의 결정적 순수 planner와 coverage projection을 제공한다."""

from __future__ import annotations

import math
import re
import unicodedata
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any
from uuid import UUID

import pyarrow as pa
from core.gold_publication import (
    Artifact,
    ArtifactSet,
    ContractViolation,
    ImmutableObjectStore,
    InputArtifact,
    InputFingerprint,
    Parameter,
    PreparedPublication,
    PublicationManifest,
    RouteCoverageDocument,
    RouteCoverageStop,
    VerifiedPublicationEvidence,
    build_artifact_set,
    build_id_set,
    build_input_fingerprint,
    build_route_coverage,
    build_route_coverage_route,
    parse_input_fingerprint,
    parse_publication_manifest,
    parse_route_coverage,
    route_uuid_v5,
    sha256_hex,
    validate_route_urgency_dependencies,
)
from psycopg import Connection, Cursor
from psycopg.pq import TransactionStatus
from psycopg.rows import tuple_row

from .common import (
    OutputObject,
    PublicationExecution,
    build_prepared_publication,
    content_addressed_uri,
    materialize_publication,
    parquet_bytes,
    publish_verified,
    read_parquet_bytes,
    store_input_payload,
)
from .state import (
    PublicationStateRecord,
    load_dependencies,
    load_publication_state,
    read_state_manifest,
)

ROUTE_ALGORITHM_VERSION = "route-v2"
TRUCK_CAPACITY = 20
TRUCK_CAPACITY_CONFIG_VERSION = "truck-capacity-v1"
INITIAL_TRUCK_LOAD = 0
MAX_STOPS_PER_ROUTE = 8
MAX_ROUTES_PER_CENTER = 3
ROUTE_WORK_UNIT_CONFIG_VERSION = "route-work-unit-v1"
ROUTE_PUBLISHER_VERSION = "gold-route-publisher-v1"
_MAX_DATABASE_REVISION = 2_147_483_647

_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_URGENCY_ACTIONS = {"normal", "supply_needed", "retrieval_needed"}
_ROUTE_ACTIONS = {"pickup", "dropoff"}
_ROUTE_STATUSES = {"proposed", "dispatched", "completed"}
_POSTGRES_INTEGER_MAX = 2_147_483_647
_URGENCY_OUTPUT_SCHEMA = pa.schema(
    (
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("base_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("urgency_score", pa.float64(), nullable=False),
        pa.field("critical_remaining_min", pa.int32(), nullable=False),
        pa.field("rebalance_need_type_cd", pa.string(), nullable=False),
        pa.field("bike_qty", pa.int32(), nullable=False),
    )
)
_ROUTES_SCHEMA = pa.schema(
    (
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("dispatch_center_id", pa.string(), nullable=False),
        pa.field("route_status_cd", pa.string(), nullable=False),
        pa.field("proposed_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
        pa.field("dispatched_dttm", pa.timestamp("us", tz="UTC"), nullable=True),
        pa.field("completed_dttm", pa.timestamp("us", tz="UTC"), nullable=True),
    )
)
_ROUTE_STOPS_SCHEMA = pa.schema(
    (
        pa.field("route_id", pa.string(), nullable=False),
        pa.field("visit_no", pa.int16(), nullable=False),
        pa.field("sta_id", pa.string(), nullable=False),
        pa.field("route_action_type_cd", pa.string(), nullable=False),
        pa.field("bike_cnt", pa.int32(), nullable=False),
    )
)


@dataclass(frozen=True, slots=True)
class DispatchCenterTopology:
    """현재 Gold dispatch_center의 경로 계산용 필드를 표현한다."""

    dispatch_center_id: str
    longitude: float
    latitude: float
    is_active: bool

    def __post_init__(self) -> None:
        """센터 ID·Point·활성 상태 타입을 검증한다."""
        _nonblank_text(self.dispatch_center_id, "dispatch_center_id")
        _point(self.longitude, self.latitude, "dispatch center")
        if type(self.is_active) is not bool:
            raise ContractViolation("dispatch center is_active는 bool이어야 합니다.")


@dataclass(frozen=True, slots=True)
class StationRouteTopology:
    """현재 Gold station의 경로 계산용 Point와 물리화된 센터 FK를 표현한다."""

    sta_id: str
    dispatch_center_id: str
    longitude: float
    latitude: float
    is_active: bool

    def __post_init__(self) -> None:
        """station ID·센터 FK·Point·활성 상태 타입을 검증한다."""
        _station_id(self.sta_id)
        _nonblank_text(self.dispatch_center_id, "station dispatch_center_id")
        _point(self.longitude, self.latitude, "station")
        if type(self.is_active) is not bool:
            raise ContractViolation("station is_active는 bool이어야 합니다.")


@dataclass(frozen=True, slots=True)
class RouteUrgencyInput:
    """urgency artifact에서 route-v2가 소비하는 최소 typed 필드를 표현한다."""

    sta_id: str
    urgency_score: float
    action_type: str
    bike_qty: int

    def __post_init__(self) -> None:
        """station·점수·판단 코드·이동 수량 타입을 검증한다."""
        _station_id(self.sta_id)
        score = _bounded_float(self.urgency_score, "urgency_score", 0.0, 100.0)
        object.__setattr__(self, "urgency_score", score)
        if (
            type(self.action_type) is not str
            or self.action_type not in _URGENCY_ACTIONS
        ):
            raise ContractViolation("urgency action_type이 SSOT allowlist에 없습니다.")
        if type(self.bike_qty) is not int:
            raise ContractViolation("urgency bike_qty는 integer여야 합니다.")


@dataclass(frozen=True, slots=True)
class ExistingRouteStop:
    """현재 DB route aggregate에서 coverage로 투영할 stop을 표현한다."""

    visit_no: int
    sta_id: str
    action: str
    bike_cnt: int

    def __post_init__(self) -> None:
        """연속 순서 검증 전 stop의 DDL 필드를 검증한다."""
        if type(self.visit_no) is not int or not 1 <= self.visit_no <= 32767:
            raise ContractViolation(
                "route stop visit_no는 SMALLINT 범위의 양수여야 합니다."
            )
        _station_id(self.sta_id)
        if type(self.action) is not str or self.action not in _ROUTE_ACTIONS:
            raise ContractViolation("route stop action이 SSOT allowlist에 없습니다.")
        if type(self.bike_cnt) is not int or self.bike_cnt <= 0:
            raise ContractViolation("route stop bike_cnt는 양수 integer여야 합니다.")


@dataclass(frozen=True, slots=True)
class ExistingRoute:
    """coverage 선택 전 현재 proposed·dispatched·completed aggregate를 표현한다."""

    route_id: str
    route_status_cd: str
    dispatched_dttm: datetime | None
    completed_dttm: datetime | None
    stops: tuple[ExistingRouteStop, ...]

    def __post_init__(self) -> None:
        """route UUID·lifecycle 조합·연속 stop을 검증한다."""
        _canonical_uuid(self.route_id, "route_id")
        if (
            type(self.route_status_cd) is not str
            or self.route_status_cd not in _ROUTE_STATUSES
        ):
            raise ContractViolation("route_status_cd가 SSOT allowlist에 없습니다.")
        if type(self.stops) is not tuple or any(
            type(stop) is not ExistingRouteStop for stop in self.stops
        ):
            raise ContractViolation(
                "existing route stops는 ExistingRouteStop tuple이어야 합니다."
            )
        if not self.stops:
            raise ContractViolation(
                "existing route에는 stop이 하나 이상 있어야 합니다."
            )
        if tuple(stop.visit_no for stop in self.stops) != tuple(
            range(1, len(self.stops) + 1)
        ):
            raise ContractViolation("existing route stop visit_no는 1..N이어야 합니다.")
        _validate_existing_running_load(self.stops)
        dispatched = _optional_utc_dttm(self.dispatched_dttm, "dispatched_dttm")
        completed = _optional_utc_dttm(self.completed_dttm, "completed_dttm")
        object.__setattr__(self, "dispatched_dttm", dispatched)
        object.__setattr__(self, "completed_dttm", completed)
        if self.route_status_cd == "proposed":
            if dispatched is not None or completed is not None:
                raise ContractViolation(
                    "proposed route의 lifecycle 일시는 null이어야 합니다."
                )
        elif self.route_status_cd == "dispatched":
            if dispatched is None or completed is not None:
                raise ContractViolation(
                    "dispatched route에는 dispatched_dttm만 필요합니다."
                )
        else:
            if dispatched is None or completed is None:
                raise ContractViolation(
                    "completed route에는 두 lifecycle 일시가 필요합니다."
                )
            if completed < dispatched:
                raise ContractViolation(
                    "completed_dttm은 dispatched_dttm보다 빠를 수 없습니다."
                )


@dataclass(frozen=True, slots=True)
class RebalanceRoute:
    """새 publication이 제안하는 rebalance_route 행을 표현한다."""

    route_id: str
    dispatch_center_id: str
    route_status_cd: str
    proposed_dttm: datetime
    dispatched_dttm: datetime | None
    completed_dttm: datetime | None

    def __post_init__(self) -> None:
        """새 route가 UUID·센터·proposed lifecycle 계약을 지키는지 검증한다."""
        _canonical_uuid(self.route_id, "route_id")
        _nonblank_text(self.dispatch_center_id, "dispatch_center_id")
        proposed = _utc_dttm(self.proposed_dttm, "proposed_dttm")
        object.__setattr__(self, "proposed_dttm", proposed)
        if self.route_status_cd != "proposed":
            raise ContractViolation("새 route는 proposed 상태여야 합니다.")
        if self.dispatched_dttm is not None or self.completed_dttm is not None:
            raise ContractViolation(
                "새 proposed route의 lifecycle 일시는 null이어야 합니다."
            )


@dataclass(frozen=True, slots=True)
class RebalanceRouteStop:
    """새 publication이 제안하는 rebalance_route_stop 행을 표현한다."""

    route_id: str
    visit_no: int
    sta_id: str
    route_action_type_cd: str
    bike_cnt: int

    def __post_init__(self) -> None:
        """stop UUID·방문 순서·station·작업·수량을 검증한다."""
        _canonical_uuid(self.route_id, "route stop route_id")
        if type(self.visit_no) is not int or not 1 <= self.visit_no <= 32767:
            raise ContractViolation("visit_no는 SMALLINT 범위의 양수여야 합니다.")
        _station_id(self.sta_id)
        if (
            type(self.route_action_type_cd) is not str
            or self.route_action_type_cd not in _ROUTE_ACTIONS
        ):
            raise ContractViolation("route_action_type_cd가 SSOT allowlist에 없습니다.")
        if type(self.bike_cnt) is not int or self.bike_cnt <= 0:
            raise ContractViolation("bike_cnt는 양수 integer여야 합니다.")


@dataclass(frozen=True, slots=True)
class RebalanceRoutePlan:
    """한 batch에서 원자 게시할 proposed header·stop aggregate를 표현한다."""

    routes: tuple[RebalanceRoute, ...]
    route_stops: tuple[RebalanceRouteStop, ...]

    def __post_init__(self) -> None:
        """aggregate ID 집합·정렬·연속 stop·적재량을 검증한다."""
        if type(self.routes) is not tuple or any(
            type(route) is not RebalanceRoute for route in self.routes
        ):
            raise ContractViolation("routes는 RebalanceRoute tuple이어야 합니다.")
        if type(self.route_stops) is not tuple or any(
            type(stop) is not RebalanceRouteStop for stop in self.route_stops
        ):
            raise ContractViolation(
                "route_stops는 RebalanceRouteStop tuple이어야 합니다."
            )
        route_ids = tuple(route.route_id for route in self.routes)
        if len(route_ids) != len(set(route_ids)):
            raise ContractViolation("새 route aggregate에 중복 route_id가 있습니다.")
        center_ids = tuple(route.dispatch_center_id for route in self.routes)
        if center_ids != tuple(sorted(center_ids, key=_utf8_key)):
            raise ContractViolation("route는 dispatch_center_id UTF-8 순이어야 합니다.")
        if not self.routes:
            if self.route_stops:
                raise ContractViolation(
                    "EMPTY route aggregate에는 stop이 없어야 합니다."
                )
            return
        proposal_times = {route.proposed_dttm for route in self.routes}
        if len(proposal_times) != 1:
            raise ContractViolation(
                "한 route aggregate의 proposed_dttm은 같아야 합니다."
            )
        stops_by_route: dict[str, list[RebalanceRouteStop]] = {
            route_id: [] for route_id in route_ids
        }
        observed_route_order: list[str] = []
        for stop in self.route_stops:
            if stop.route_id not in stops_by_route:
                raise ContractViolation("header가 없는 route stop이 있습니다.")
            if not observed_route_order or observed_route_order[-1] != stop.route_id:
                observed_route_order.append(stop.route_id)
            stops_by_route[stop.route_id].append(stop)
        if tuple(observed_route_order) != route_ids:
            raise ContractViolation(
                "route stops가 header 생성 순서로 묶여 있지 않습니다."
            )
        for route_id in route_ids:
            stops = stops_by_route[route_id]
            if not stops:
                raise ContractViolation("각 route에는 stop이 하나 이상 있어야 합니다.")
            if tuple(stop.visit_no for stop in stops) != tuple(
                range(1, len(stops) + 1)
            ):
                raise ContractViolation("route stop visit_no는 1..N이어야 합니다.")
            _validate_running_load(stops)


@dataclass(frozen=True, slots=True)
class RouteParquetArtifacts:
    """route header와 stop의 fixed-schema Parquet bytes를 묶는다."""

    routes: bytes
    route_stops: bytes

    def __post_init__(self) -> None:
        """두 artifact가 완전한 bytes인지 검증한다."""
        if type(self.routes) is not bytes or type(self.route_stops) is not bytes:
            raise ContractViolation("route Parquet artifacts는 bytes여야 합니다.")


@dataclass(frozen=True, slots=True)
class RouteUrgencySnapshot:
    """검증된 urgency publication과 route 계산용 typed 행을 묶는다."""

    manifest: PublicationManifest
    input_fingerprint: InputFingerprint
    manifest_input: InputArtifact
    records: tuple[RouteUrgencyInput, ...]

    def __post_init__(self) -> None:
        """Manifest·fingerprint·input role·record 타입의 결합을 검증한다."""
        if (
            type(self.manifest) is not PublicationManifest
            or self.manifest.publication_key != "station_urgency"
        ):
            raise ContractViolation(
                "route urgency snapshot에는 station_urgency manifest가 필요합니다."
            )
        if type(self.input_fingerprint) is not InputFingerprint:
            raise ContractViolation("urgency input fingerprint 타입이 잘못됐습니다.")
        if (
            type(self.manifest_input) is not InputArtifact
            or self.manifest_input.role != "urgency_publication_manifest"
        ):
            raise ContractViolation(
                "urgency publication manifest input role이 잘못됐습니다."
            )
        if type(self.records) is not tuple or any(
            type(record) is not RouteUrgencyInput for record in self.records
        ):
            raise ContractViolation("route urgency records 타입이 잘못됐습니다.")


@dataclass(frozen=True, slots=True)
class RouteDatabaseSnapshot:
    """한 route 계산이 고정한 topology와 terminal coverage를 표현한다."""

    dispatch_centers: tuple[DispatchCenterTopology, ...]
    stations: tuple[StationRouteTopology, ...]
    route_coverage: RouteCoverageDocument

    def __post_init__(self) -> None:
        """DB snapshot의 exact tuple과 canonical coverage 타입을 검증한다."""
        _index_centers(self.dispatch_centers)
        _index_stations(self.stations)
        if type(self.route_coverage) is not RouteCoverageDocument:
            raise ContractViolation("route DB snapshot coverage 타입이 잘못됐습니다.")


@dataclass(frozen=True, slots=True)
class _RoutePublicationCandidate:
    """Immutable write 전 revision별 route output과 hash preview를 보관한다."""

    logical_dttm: datetime
    revision_no: int
    plan: RebalanceRoutePlan
    outputs: tuple[OutputObject, ...]
    artifact_set: ArtifactSet
    input_fingerprint: InputFingerprint

    def __post_init__(self) -> None:
        """Candidate revision·plan·output·canonical document 타입을 검증한다."""
        object.__setattr__(
            self,
            "logical_dttm",
            _utc_dttm(self.logical_dttm, "candidate logical_dttm"),
        )
        _nonnegative_integer(self.revision_no, "candidate revision_no")
        if type(self.plan) is not RebalanceRoutePlan:
            raise ContractViolation("route candidate plan 타입이 잘못됐습니다.")
        if type(self.outputs) is not tuple or any(
            type(output) is not OutputObject for output in self.outputs
        ):
            raise ContractViolation("route candidate outputs 타입이 잘못됐습니다.")
        if type(self.artifact_set) is not ArtifactSet:
            raise ContractViolation("route candidate artifact set 타입이 잘못됐습니다.")
        if type(self.input_fingerprint) is not InputFingerprint:
            raise ContractViolation("route candidate fingerprint 타입이 잘못됐습니다.")


@dataclass(frozen=True, slots=True)
class _Candidate:
    """coverage를 차감한 현재 센터별 route 후보를 표현한다."""

    sta_id: str
    action: str
    urgency_score: float
    remaining_qty: int
    longitude: float
    latitude: float


@dataclass(frozen=True, slots=True)
class _SelectedStop:
    """한 route에 배정된 station과 실제 작업 수량을 표현한다."""

    candidate: _Candidate
    bike_cnt: int


def build_current_route_coverage(
    *,
    stock_anchor_dttm: datetime,
    routes: tuple[ExistingRoute, ...],
) -> RouteCoverageDocument:
    """dispatched 전부와 stock anchor 뒤 completed route의 canonical coverage를 만든다."""
    anchor = _utc_dttm(stock_anchor_dttm, "stock_anchor_dttm")
    if type(routes) is not tuple or any(
        type(route) is not ExistingRoute for route in routes
    ):
        raise ContractViolation("existing routes는 ExistingRoute tuple이어야 합니다.")
    route_ids = tuple(route.route_id for route in routes)
    if len(route_ids) != len(set(route_ids)):
        raise ContractViolation("existing routes에 중복 route_id가 있습니다.")
    covered = []
    for route in routes:
        if route.route_status_cd == "proposed":
            continue
        if route.route_status_cd == "completed" and route.completed_dttm <= anchor:
            continue
        covered.append(
            build_route_coverage_route(
                completed_dttm=route.completed_dttm,
                dispatched_dttm=route.dispatched_dttm,
                route_id=route.route_id,
                status=route.route_status_cd,
                stops=(
                    RouteCoverageStop(
                        action=stop.action,
                        bike_cnt=stop.bike_cnt,
                        sta_id=stop.sta_id,
                        visit_no=stop.visit_no,
                    )
                    for stop in route.stops
                ),
            )
        )
    return build_route_coverage(stock_anchor_dttm=anchor, routes=covered)


def plan_rebalance_routes(
    *,
    logical_dttm: datetime,
    revision_no: int,
    dispatch_centers: tuple[DispatchCenterTopology, ...],
    stations: tuple[StationRouteTopology, ...],
    urgency: tuple[RouteUrgencyInput, ...],
    route_coverage: RouteCoverageDocument,
    max_stops_per_route: int = MAX_STOPS_PER_ROUTE,
) -> RebalanceRoutePlan:
    """route-v2 완결 작업·coverage·용량 계약으로 proposed aggregate를 계산한다.

    ``normal``과 ``bike_qty<=0`` 행은 route 후보가 아니므로 이 순수 planner는
    topology 존재를 요구하지 않는다. urgency 기대 집합의 완전성은 publication
    manifest를 검증하는 publisher 경계가 담당한다.
    """
    logical = _utc_dttm(logical_dttm, "logical_dttm")
    _nonnegative_integer(revision_no, "revision_no")
    if type(max_stops_per_route) is not int or not 2 <= max_stops_per_route <= 32767:
        raise ContractViolation("max_stops_per_route는 2..32767 integer여야 합니다.")
    if type(route_coverage) is not RouteCoverageDocument:
        raise ContractViolation("route_coverage는 RouteCoverageDocument여야 합니다.")
    if route_coverage.stock_anchor_dttm != logical:
        raise ContractViolation(
            "route coverage stock anchor가 urgency logical_dttm과 다릅니다."
        )
    centers_by_id = _index_centers(dispatch_centers)
    stations_by_id = _index_stations(stations)
    urgency_rows = _validate_urgency(urgency)
    coverage_qty = _coverage_quantities(route_coverage)
    candidates_by_center = _build_candidates(
        urgency_rows,
        stations_by_id=stations_by_id,
        centers_by_id=centers_by_id,
        coverage_qty=coverage_qty,
    )
    routes: list[RebalanceRoute] = []
    route_stops: list[RebalanceRouteStop] = []
    for center_id in sorted(centers_by_id, key=_utf8_key):
        center = centers_by_id[center_id]
        if not center.is_active:
            continue
        candidates = candidates_by_center.get(center_id, ())
        pickups = [
            candidate for candidate in candidates if candidate.action == "pickup"
        ]
        dropoffs = [
            candidate for candidate in candidates if candidate.action == "dropoff"
        ]
        ordinal = 1
        while (
            pickups
            and dropoffs
            and ordinal <= MAX_ROUTES_PER_CENTER
        ):
            anchor_candidate = pickups[0]
            route_pickups = _rank_for_route(
                pickups,
                anchor=anchor_candidate,
                keep_anchor_first=True,
            )
            route_dropoffs = _rank_for_route(
                dropoffs,
                anchor=anchor_candidate,
            )
            split = _choose_balanced_stop_split(
                route_pickups,
                route_dropoffs,
                max_stops_per_route=max_stops_per_route,
            )
            if split is None:
                break
            pickup_stop_limit, dropoff_stop_limit, transfer_qty = split
            selected_pickups, pickup_remainder = _take_by_priority(
                route_pickups,
                transfer_qty,
                stop_limit=pickup_stop_limit,
            )
            selected_dropoffs, dropoff_remainder = _take_by_priority(
                route_dropoffs,
                transfer_qty,
                stop_limit=dropoff_stop_limit,
            )
            pickups = sorted(pickup_remainder, key=_candidate_priority)
            dropoffs = sorted(dropoff_remainder, key=_candidate_priority)
            picked_qty = sum(stop.bike_cnt for stop in selected_pickups)
            dropped_qty = sum(stop.bike_cnt for stop in selected_dropoffs)
            if (
                picked_qty != transfer_qty
                or dropped_qty != transfer_qty
                or len(selected_pickups) + len(selected_dropoffs) > max_stops_per_route
            ):
                raise ContractViolation(
                    "route-v2 작업 단위 선택이 완결 수량·stop 제한을 위반했습니다."
                )
            route_id = str(route_uuid_v5(center_id, logical, revision_no, ordinal))
            routes.append(
                RebalanceRoute(
                    route_id=route_id,
                    dispatch_center_id=center_id,
                    route_status_cd="proposed",
                    proposed_dttm=logical,
                    dispatched_dttm=None,
                    completed_dttm=None,
                )
            )
            ordered_stops = _nearest_stops(
                selected_pickups,
                selected_dropoffs,
                start=(center.longitude, center.latitude),
            )
            route_stops.extend(
                RebalanceRouteStop(
                    route_id=route_id,
                    visit_no=visit_no,
                    sta_id=stop.candidate.sta_id,
                    route_action_type_cd=stop.candidate.action,
                    bike_cnt=stop.bike_cnt,
                )
                for visit_no, stop in enumerate(ordered_stops, start=1)
            )
            ordinal += 1
    return RebalanceRoutePlan(tuple(routes), tuple(route_stops))


def route_plan_to_parquet(plan: RebalanceRoutePlan) -> RouteParquetArtifacts:
    """route aggregate를 target-column fixed-schema Parquet 두 개로 직렬화한다."""
    if type(plan) is not RebalanceRoutePlan:
        raise ContractViolation("plan은 RebalanceRoutePlan이어야 합니다.")
    if not plan.routes:
        raise ContractViolation(
            "정상 EMPTY route publication은 Parquet artifact를 만들지 않습니다."
        )
    routes_table = pa.Table.from_pylist(
        [
            {
                "route_id": route.route_id,
                "dispatch_center_id": route.dispatch_center_id,
                "route_status_cd": route.route_status_cd,
                "proposed_dttm": route.proposed_dttm,
                "dispatched_dttm": route.dispatched_dttm,
                "completed_dttm": route.completed_dttm,
            }
            for route in plan.routes
        ],
        schema=_ROUTES_SCHEMA,
    )
    stops_table = pa.Table.from_pylist(
        [
            {
                "route_id": stop.route_id,
                "visit_no": stop.visit_no,
                "sta_id": stop.sta_id,
                "route_action_type_cd": stop.route_action_type_cd,
                "bike_cnt": stop.bike_cnt,
            }
            for stop in plan.route_stops
        ],
        schema=_ROUTE_STOPS_SCHEMA,
    )
    return RouteParquetArtifacts(
        routes=parquet_bytes(routes_table),
        route_stops=parquet_bytes(stops_table),
    )


def route_plan_from_parquet(
    routes_payload: bytes,
    route_stops_payload: bytes,
    *,
    expected_plan: RebalanceRoutePlan,
) -> RebalanceRoutePlan:
    """두 Parquet artifact를 locked input으로 재계산한 plan과 exact 검증한다."""
    if type(expected_plan) is not RebalanceRoutePlan:
        raise ContractViolation("expected_plan은 RebalanceRoutePlan이어야 합니다.")
    routes_table = read_parquet_bytes(routes_payload)
    stops_table = read_parquet_bytes(route_stops_payload)
    if not routes_table.schema.equals(_ROUTES_SCHEMA, check_metadata=False):
        raise ContractViolation("routes output Parquet schema가 exact 계약과 다릅니다.")
    if not stops_table.schema.equals(_ROUTE_STOPS_SCHEMA, check_metadata=False):
        raise ContractViolation(
            "route_stops output Parquet schema가 exact 계약과 다릅니다."
        )
    if routes_table.num_rows == 0 and stops_table.num_rows == 0:
        raise ContractViolation(
            "정상 EMPTY route publication에는 Parquet artifact가 없어야 합니다."
        )
    routes = tuple(RebalanceRoute(**row) for row in routes_table.to_pylist())
    stops = tuple(RebalanceRouteStop(**row) for row in stops_table.to_pylist())
    actual = RebalanceRoutePlan(routes, stops)
    if actual != expected_plan:
        raise ContractViolation(
            "route artifacts가 locked topology·anchor·revision 재계산 결과와 다릅니다."
        )
    return actual


def publish_rebalance_route(
    connection: Connection[Any],
    object_store: ImmutableObjectStore,
    *,
    urgency_manifest_uri: str,
    urgency_manifest_sha256: str,
    object_base_uri: str,
    publisher_version: str = ROUTE_PUBLISHER_VERSION,
) -> PublicationExecution:
    """Urgency authority에서 proposed route aggregate를 계산해 원자 게시한다.

    실제 urgency manifest·output·nested fingerprint를 exact-read하고, 현재 Gold
    dependency·topology·terminal coverage를 고정한다. 같은 logical correction은 현재
    revision plan을 메모리에서 먼저 비교한 뒤 필요할 때만 다음 revision UUID로 다시
    계산하므로 tentative output object를 남기지 않는다.
    """
    urgency_snapshot = _read_route_urgency_snapshot(
        object_store,
        manifest_uri=urgency_manifest_uri,
        manifest_sha256=urgency_manifest_sha256,
    )
    urgency_state = load_publication_state(connection, "station_urgency")
    if urgency_state is None:
        raise ContractViolation("station_urgency publication state가 없습니다.")
    state_manifest = read_state_manifest(object_store, urgency_state)
    if (
        urgency_state.manifest_uri != urgency_manifest_uri
        or state_manifest != urgency_snapshot.manifest
        or urgency_snapshot.manifest.sha256 != urgency_manifest_sha256
    ):
        raise ContractViolation(
            "요청한 urgency manifest가 현재 Gold station_urgency state와 다릅니다."
        )

    dependencies = load_dependencies(
        connection,
        (
            "dispatch_center",
            "station",
            "station_demand_forecast",
            "station_stock",
            "station_urgency",
        ),
    )
    urgency_dependency = next(
        dependency
        for dependency in dependencies
        if dependency.publication_key == "station_urgency"
    )
    if urgency_dependency != urgency_state.dependency:
        raise ContractViolation(
            "urgency state가 route dependency를 읽는 동안 변경됐습니다."
        )

    logical_dttm = urgency_snapshot.manifest.logical_dttm
    database_snapshot = _load_route_database_snapshot(connection, logical_dttm)
    coverage_payload = database_snapshot.route_coverage.canonical_bytes
    coverage_input = InputArtifact(
        byte_sha256=database_snapshot.route_coverage.sha256,
        role="route_coverage",
        uri=content_addressed_uri(
            object_base_uri,
            publication_key="rebalance_route",
            category="inputs",
            name="route_coverage",
            payload=coverage_payload,
            suffix="json",
        ),
    )
    input_fingerprint = build_input_fingerprint(
        "rebalance_route",
        dependencies,
        (coverage_input, urgency_snapshot.manifest_input),
        (
            Parameter("route_algorithm_version", ROUTE_ALGORITHM_VERSION),
            Parameter(
                "route_coverage_sha256",
                database_snapshot.route_coverage.sha256,
            ),
            Parameter("truck_capacity", str(TRUCK_CAPACITY)),
            Parameter(
                "truck_capacity_config_version",
                TRUCK_CAPACITY_CONFIG_VERSION,
            ),
            Parameter("max_stops_per_route", str(MAX_STOPS_PER_ROUTE)),
            Parameter("max_routes_per_center", str(MAX_ROUTES_PER_CENTER)),
            Parameter(
                "route_work_unit_config_version",
                ROUTE_WORK_UNIT_CONFIG_VERSION,
            ),
        ),
    )
    validate_route_urgency_dependencies(
        input_fingerprint,
        urgency_snapshot.input_fingerprint,
    )

    current = load_publication_state(connection, "rebalance_route")
    tentative_revision = (
        current.revision_no
        if current is not None and current.logical_dttm == logical_dttm
        else 0
    )
    candidate = _build_route_candidate(
        revision_no=tentative_revision,
        logical_dttm=logical_dttm,
        database_snapshot=database_snapshot,
        urgency_snapshot=urgency_snapshot,
        input_fingerprint=input_fingerprint,
        object_base_uri=object_base_uri,
    )
    revision_no = _choose_route_revision(candidate, current)
    if revision_no != tentative_revision:
        candidate = _build_route_candidate(
            revision_no=revision_no,
            logical_dttm=logical_dttm,
            database_snapshot=database_snapshot,
            urgency_snapshot=urgency_snapshot,
            input_fingerprint=input_fingerprint,
            object_base_uri=object_base_uri,
        )
    if _candidate_is_exact_replay(candidate, current):
        current_plan = _load_current_proposed_plan(connection, candidate.plan)
        if current_plan != candidate.plan:
            raise ContractViolation(
                "rebalance_route state는 replay지만 current proposed aggregate가 다릅니다."
            )

    stored_coverage = store_input_payload(
        object_store,
        base_uri=object_base_uri,
        publication_key="rebalance_route",
        role="route_coverage",
        payload=coverage_payload,
        suffix="json",
        require_canonical_json=True,
    )
    if stored_coverage != coverage_input:
        raise ContractViolation("stored route coverage identity가 preview와 다릅니다.")
    materials = materialize_publication(
        object_store,
        base_uri=object_base_uri,
        publication_key="rebalance_route",
        dependencies=dependencies,
        input_artifacts=(coverage_input, urgency_snapshot.manifest_input),
        parameters=input_fingerprint.parameters,
        outputs=candidate.outputs,
    )
    if (
        materials.artifact_set != candidate.artifact_set
        or materials.input_fingerprint != candidate.input_fingerprint
    ):
        raise ContractViolation(
            "route immutable materialization이 revision preview와 다릅니다."
        )
    prepared = build_prepared_publication(
        base_uri=object_base_uri,
        publication_key="rebalance_route",
        logical_dttm=logical_dttm,
        publisher_version=publisher_version,
        revision_no=revision_no,
        target_row_counts={
            "rebalance_route": len(candidate.plan.routes),
            "rebalance_route_stop": len(candidate.plan.route_stops),
        },
        materials=materials,
    )

    def validate_staging(
        publication: PreparedPublication,
        payloads: Mapping[str, bytes],
    ) -> Mapping[str, tuple[datetime, ...]]:
        """Actual urgency·coverage·output bytes에서 같은 revision plan을 재증명한다."""
        actual_urgency = _read_route_urgency_snapshot_from_verified_inputs(
            object_store,
            manifest_input=urgency_snapshot.manifest_input,
            payloads=payloads,
        )
        actual_coverage = parse_route_coverage(payloads[coverage_input.uri])
        if actual_coverage != database_snapshot.route_coverage:
            raise ContractViolation(
                "verified route coverage bytes가 준비한 DB projection과 다릅니다."
            )
        validate_route_urgency_dependencies(
            publication.input_fingerprint,
            actual_urgency.input_fingerprint,
        )
        expected = _plan_from_snapshots(
            logical_dttm=publication.manifest.logical_dttm,
            revision_no=publication.manifest.revision_no,
            database_snapshot=database_snapshot,
            urgency_snapshot=actual_urgency,
        )
        if expected != candidate.plan:
            raise ContractViolation(
                "verified urgency actual bytes가 준비한 route plan과 다릅니다."
            )
        _validate_route_output_artifacts(publication, payloads, expected)
        return {
            "proposed_dttm": tuple(route.proposed_dttm for route in expected.routes)
        }

    def validate_locked(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Topology·route lock 안 DB snapshot과 coverage로 plan을 다시 계산한다."""
        _require_route_evidence(evidence)
        locked_snapshot = _route_database_snapshot_locked(cursor, logical_dttm)
        if locked_snapshot != database_snapshot:
            raise ContractViolation(
                "route staging 이후 topology 또는 terminal coverage가 바뀌었습니다."
            )
        locked_plan = _plan_from_snapshots(
            logical_dttm=logical_dttm,
            revision_no=revision_no,
            database_snapshot=locked_snapshot,
            urgency_snapshot=urgency_snapshot,
        )
        if locked_plan != candidate.plan:
            raise ContractViolation(
                "locked topology·coverage route 재계산 결과가 staging과 다릅니다."
            )

    def mutate_targets(
        cursor: Cursor[tuple[Any, ...]],
        evidence: tuple[VerifiedPublicationEvidence, ...],
    ) -> None:
        """Proposed aggregate만 full reconcile하고 terminal route는 보존한다."""
        _require_route_evidence(evidence)
        _reconcile_route_plan(cursor, candidate.plan)

    return publish_verified(
        connection,
        ((prepared, validate_staging),),
        object_store,
        mutate_targets,
        validate_locked=validate_locked,
    )


def _build_route_candidate(
    *,
    revision_no: int,
    logical_dttm: datetime,
    database_snapshot: RouteDatabaseSnapshot,
    urgency_snapshot: RouteUrgencySnapshot,
    input_fingerprint: InputFingerprint,
    object_base_uri: str,
) -> _RoutePublicationCandidate:
    """Revision UUID를 포함한 plan과 output artifact-set을 write 없이 preview한다."""
    plan = _plan_from_snapshots(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        database_snapshot=database_snapshot,
        urgency_snapshot=urgency_snapshot,
    )
    outputs = _route_output_objects(plan)
    artifacts = tuple(
        Artifact(
            byte_sha256=sha256_hex(output.payload),
            role=output.role,
            row_count=output.row_count,
            uri=content_addressed_uri(
                object_base_uri,
                publication_key="rebalance_route",
                category="outputs",
                name=output.role,
                payload=output.payload,
                suffix=output.suffix,
            ),
        )
        for output in outputs
    )
    return _RoutePublicationCandidate(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        plan=plan,
        outputs=outputs,
        artifact_set=build_artifact_set(artifacts),
        input_fingerprint=input_fingerprint,
    )


def _choose_route_revision(
    candidate: _RoutePublicationCandidate,
    current: PublicationStateRecord | None,
) -> int:
    """현재 revision preview가 replay인지 판정하고 correction revision을 반환한다."""
    if current is None or current.logical_dttm != candidate.logical_dttm:
        return 0
    if _candidate_is_exact_replay(candidate, current):
        return current.revision_no
    if current.revision_no == _MAX_DATABASE_REVISION:
        raise ContractViolation(
            "rebalance_route correction revision이 INTEGER 한계에 도달했습니다."
        )
    return current.revision_no + 1


def _candidate_is_exact_replay(
    candidate: _RoutePublicationCandidate,
    current: PublicationStateRecord | None,
) -> bool:
    """Candidate canonical content가 같은 logical current state인지 반환한다."""
    if current is None or current.logical_dttm != candidate.logical_dttm:
        return False
    return (
        candidate.revision_no == current.revision_no
        and candidate.artifact_set.sha256 == current.artifact_set_sha256
        and candidate.input_fingerprint.sha256 == current.input_fingerprint_sha256
        and len(candidate.plan.routes) == current.published_row_cnt
    )


def _route_output_objects(plan: RebalanceRoutePlan) -> tuple[OutputObject, ...]:
    """Nonempty route plan을 두 output으로 만들고 EMPTY는 artifact 없이 둔다."""
    if not plan.routes:
        return ()
    artifacts = route_plan_to_parquet(plan)
    return (
        OutputObject("routes", artifacts.routes, len(plan.routes)),
        OutputObject("route_stops", artifacts.route_stops, len(plan.route_stops)),
    )


def _plan_from_snapshots(
    *,
    logical_dttm: datetime,
    revision_no: int,
    database_snapshot: RouteDatabaseSnapshot,
    urgency_snapshot: RouteUrgencySnapshot,
) -> RebalanceRoutePlan:
    """Typed urgency와 DB snapshot을 pure route-v2 planner 입력으로 바꾼다."""
    return plan_rebalance_routes(
        logical_dttm=logical_dttm,
        revision_no=revision_no,
        dispatch_centers=database_snapshot.dispatch_centers,
        stations=database_snapshot.stations,
        urgency=urgency_snapshot.records,
        route_coverage=database_snapshot.route_coverage,
    )


def _read_route_urgency_snapshot(
    object_store: ImmutableObjectStore,
    *,
    manifest_uri: str,
    manifest_sha256: str,
) -> RouteUrgencySnapshot:
    """URI·SHA로 urgency manifest와 nested fingerprint·output actual bytes를 읽는다."""
    manifest_payload = object_store.read_bytes(
        manifest_uri,
        manifest_sha256,
        require_canonical_json=True,
    )
    if sha256_hex(manifest_payload) != manifest_sha256:
        raise ContractViolation(
            "urgency manifest actual bytes SHA가 요청값과 다릅니다."
        )
    manifest = parse_publication_manifest(manifest_payload)
    fingerprint_payload = object_store.read_bytes(
        manifest.input_fingerprint_uri,
        manifest.input_fingerprint_sha256,
        require_canonical_json=True,
    )
    return _build_route_urgency_snapshot(
        object_store,
        manifest_uri=manifest_uri,
        expected_manifest_sha256=manifest_sha256,
        manifest_payload=manifest_payload,
        fingerprint_payload=fingerprint_payload,
    )


def _read_route_urgency_snapshot_from_verified_inputs(
    object_store: ImmutableObjectStore,
    *,
    manifest_input: InputArtifact,
    payloads: Mapping[str, bytes],
) -> RouteUrgencySnapshot:
    """공통 verifier가 읽은 manifest·nested fingerprint bytes에서 snapshot을 재구성한다."""
    try:
        manifest_payload = payloads[manifest_input.uri]
    except KeyError as exc:
        raise ContractViolation(
            "route verifier payload에 urgency manifest가 없습니다."
        ) from exc
    manifest = parse_publication_manifest(manifest_payload)
    try:
        fingerprint_payload = payloads[manifest.input_fingerprint_uri]
    except KeyError as exc:
        raise ContractViolation(
            "route verifier payload에 nested urgency fingerprint가 없습니다."
        ) from exc
    return _build_route_urgency_snapshot(
        object_store,
        manifest_uri=manifest_input.uri,
        expected_manifest_sha256=manifest_input.byte_sha256,
        manifest_payload=manifest_payload,
        fingerprint_payload=fingerprint_payload,
    )


def _build_route_urgency_snapshot(
    object_store: ImmutableObjectStore,
    *,
    manifest_uri: str,
    expected_manifest_sha256: str,
    manifest_payload: bytes,
    fingerprint_payload: bytes,
) -> RouteUrgencySnapshot:
    """Urgency wire documents와 output Parquet을 typed route input으로 결합한다."""
    if sha256_hex(manifest_payload) != expected_manifest_sha256:
        raise ContractViolation(
            "urgency manifest payload SHA가 input identity와 다릅니다."
        )
    manifest = parse_publication_manifest(manifest_payload)
    if manifest.publication_key != "station_urgency":
        raise ContractViolation("route input manifest가 station_urgency가 아닙니다.")
    if manifest.sha256 != expected_manifest_sha256:
        raise ContractViolation("urgency canonical manifest SHA가 요청값과 다릅니다.")
    if sha256_hex(fingerprint_payload) != manifest.input_fingerprint_sha256:
        raise ContractViolation(
            "urgency nested fingerprint actual bytes SHA가 manifest와 다릅니다."
        )
    fingerprint = parse_input_fingerprint(
        fingerprint_payload,
        "station_urgency",
    )
    parameters = {
        parameter.name: parameter.value for parameter in fingerprint.parameters
    }

    if manifest.published_row_cnt == 0:
        records: tuple[RouteUrgencyInput, ...] = ()
    else:
        if len(manifest.artifacts) != 1:
            raise ContractViolation(
                "nonempty urgency manifest에는 output artifact 하나가 필요합니다."
            )
        artifact = manifest.artifacts[0]
        if artifact.role != "station_urgency":
            raise ContractViolation("urgency manifest output role이 잘못됐습니다.")
        output_payload = object_store.read_bytes(
            artifact.uri,
            artifact.byte_sha256,
        )
        if sha256_hex(output_payload) != artifact.byte_sha256:
            raise ContractViolation("urgency output actual bytes SHA가 다릅니다.")
        table = read_parquet_bytes(output_payload)
        if "sta_id" not in table.column_names:
            raise ContractViolation("urgency output Parquet에 sta_id가 없습니다.")
        expected_ids = tuple(table.column("sta_id").to_pylist())
        records = _route_urgency_records_from_parquet(
            output_payload,
            expected_base_dttm=manifest.logical_dttm,
            expected_sta_ids=expected_ids,
        )
        if artifact.row_count != len(records):
            raise ContractViolation(
                "urgency output physical row count가 manifest artifact와 다릅니다."
            )
    expected_ids = tuple(record.sta_id for record in records)
    if parameters["expected_sta_id_sha256"] != build_id_set(expected_ids).sha256:
        raise ContractViolation(
            "urgency output station 집합이 nested expected_sta_id_sha256과 다릅니다."
        )
    if manifest.published_row_cnt != len(records):
        raise ContractViolation(
            "urgency manifest published_row_cnt가 actual output과 다릅니다."
        )
    return RouteUrgencySnapshot(
        manifest=manifest,
        input_fingerprint=fingerprint,
        manifest_input=InputArtifact(
            byte_sha256=expected_manifest_sha256,
            role="urgency_publication_manifest",
            uri=manifest_uri,
        ),
        records=records,
    )


def _route_urgency_records_from_parquet(
    payload: bytes,
    *,
    expected_base_dttm: datetime,
    expected_sta_ids: tuple[str, ...],
) -> tuple[RouteUrgencyInput, ...]:
    """Route가 소비하는 urgency output exact schema·anchor·ID 집합을 검증한다."""
    table = read_parquet_bytes(payload)
    if not table.schema.equals(_URGENCY_OUTPUT_SCHEMA, check_metadata=False):
        raise ContractViolation(
            "urgency output Parquet schema가 exact 계약과 다릅니다."
        )
    expected_base = _utc_dttm(expected_base_dttm, "urgency expected base_dttm")
    canonical_ids = tuple(sorted(expected_sta_ids, key=_utf8_key))
    if expected_sta_ids != canonical_ids or len(expected_sta_ids) != len(
        set(expected_sta_ids)
    ):
        raise ContractViolation(
            "urgency output sta_id는 중복 없이 UTF-8 순이어야 합니다."
        )
    records: list[RouteUrgencyInput] = []
    actual_ids: list[str] = []
    for row in table.to_pylist():
        station_id = _station_id(row["sta_id"])
        if _utc_dttm(row["base_dttm"], "urgency base_dttm") != expected_base:
            raise ContractViolation(
                "urgency output row base_dttm이 manifest logical_dttm과 다릅니다."
            )
        _postgres_nonnegative_integer(
            row["critical_remaining_min"],
            "urgency critical_remaining_min",
        )
        _postgres_nonnegative_integer(row["bike_qty"], "urgency bike_qty")
        actual_ids.append(station_id)
        records.append(
            RouteUrgencyInput(
                sta_id=station_id,
                urgency_score=row["urgency_score"],
                action_type=row["rebalance_need_type_cd"],
                bike_qty=row["bike_qty"],
            )
        )
    if tuple(actual_ids) != expected_sta_ids:
        raise ContractViolation(
            "urgency output actual station 순서·집합이 기대값과 다릅니다."
        )
    return tuple(records)


def _load_route_database_snapshot(
    connection: Connection[Any],
    logical_dttm: datetime,
) -> RouteDatabaseSnapshot:
    """짧은 transaction에서 현재 topology와 terminal coverage를 함께 읽는다."""
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "route DB snapshot loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return _route_database_snapshot_locked(cursor, logical_dttm)


def _route_database_snapshot_locked(
    cursor: Cursor[tuple[Any, ...]],
    logical_dttm: datetime,
) -> RouteDatabaseSnapshot:
    """현재 cursor snapshot에서 topology와 canonical route coverage를 읽는다."""
    logical = _utc_dttm(logical_dttm, "route logical_dttm")
    cursor.execute(
        """
        SELECT dispatch_center_id,
               ST_X(dispatch_center_point),
               ST_Y(dispatch_center_point),
               is_active
          FROM dispatch_center
         ORDER BY dispatch_center_id COLLATE "C"
        """
    )
    centers = tuple(DispatchCenterTopology(*row) for row in cursor.fetchall())
    cursor.execute(
        """
        SELECT sta_id,
               dispatch_center_id,
               ST_X(sta_point),
               ST_Y(sta_point),
               is_active
          FROM station
         ORDER BY sta_id COLLATE "C"
        """
    )
    stations = tuple(StationRouteTopology(*row) for row in cursor.fetchall())
    coverage = _route_coverage_locked(cursor, logical)
    return RouteDatabaseSnapshot(centers, stations, coverage)


def _route_coverage_locked(
    cursor: Cursor[tuple[Any, ...]],
    stock_anchor_dttm: datetime,
) -> RouteCoverageDocument:
    """DB dispatched 전체와 stock anchor 뒤 completed aggregate를 canonicalize한다."""
    anchor = _utc_dttm(stock_anchor_dttm, "route stock anchor")
    cursor.execute(
        """
        SELECT route_id::TEXT,
               route_status_cd,
               dispatched_dttm,
               completed_dttm
          FROM rebalance_route
         WHERE route_status_cd = 'dispatched'
            OR (route_status_cd = 'completed' AND completed_dttm > %s)
         ORDER BY route_id::TEXT COLLATE "C"
        """,
        (anchor,),
    )
    headers = tuple(cursor.fetchall())
    route_ids = tuple(row[0] for row in headers)
    stops_by_route: dict[str, list[ExistingRouteStop]] = {
        route_id: [] for route_id in route_ids
    }
    if route_ids:
        cursor.execute(
            """
            SELECT route_id::TEXT,
                   visit_no,
                   sta_id,
                   route_action_type_cd,
                   bike_cnt
              FROM rebalance_route_stop
             WHERE route_id = ANY(%s::UUID[])
             ORDER BY route_id::TEXT COLLATE "C", visit_no
            """,
            (list(route_ids),),
        )
        for route_id, visit_no, sta_id, action, bike_cnt in cursor.fetchall():
            stops_by_route[route_id].append(
                ExistingRouteStop(visit_no, sta_id, action, bike_cnt)
            )
    routes = tuple(
        ExistingRoute(
            route_id=route_id,
            route_status_cd=status,
            dispatched_dttm=dispatched_dttm,
            completed_dttm=completed_dttm,
            stops=tuple(stops_by_route[route_id]),
        )
        for route_id, status, dispatched_dttm, completed_dttm in headers
    )
    return build_current_route_coverage(stock_anchor_dttm=anchor, routes=routes)


def _load_current_proposed_plan(
    connection: Connection[Any],
    expected_plan: RebalanceRoutePlan,
) -> RebalanceRoutePlan:
    """Replay drift 검사용 current proposed aggregate를 짧게 읽는다."""
    if type(expected_plan) is not RebalanceRoutePlan:
        raise ContractViolation("expected route plan 타입이 잘못됐습니다.")
    if connection.info.transaction_status is not TransactionStatus.IDLE:
        raise ContractViolation(
            "proposed route loader는 transaction이 시작되지 않은 연결이 필요합니다."
        )
    with connection.transaction(), connection.cursor(row_factory=tuple_row) as cursor:
        return _current_proposed_plan_locked(
            cursor,
            expected_route_order=tuple(
                route.route_id for route in expected_plan.routes
            ),
        )


def _current_proposed_plan_locked(
    cursor: Cursor[tuple[Any, ...]],
    *,
    expected_route_order: tuple[str, ...] | None = None,
) -> RebalanceRoutePlan:
    """현재 proposed header와 stop을 deterministic aggregate로 읽는다."""
    cursor.execute(
        """
        SELECT route_id::TEXT,
               dispatch_center_id,
               route_status_cd,
               proposed_dttm,
               dispatched_dttm,
               completed_dttm
          FROM rebalance_route
         WHERE route_status_cd = 'proposed'
         ORDER BY dispatch_center_id COLLATE "C", route_id::TEXT COLLATE "C"
        """
    )
    routes = tuple(RebalanceRoute(*row) for row in cursor.fetchall())
    if expected_route_order is not None:
        expected_ids = tuple(
            _canonical_uuid(route_id, "expected route_id")
            for route_id in expected_route_order
        )
        routes_by_id = {route.route_id: route for route in routes}
        if set(routes_by_id) == set(expected_ids):
            routes = tuple(routes_by_id[route_id] for route_id in expected_ids)
    route_ids = tuple(route.route_id for route in routes)
    stops: tuple[RebalanceRouteStop, ...] = ()
    if route_ids:
        cursor.execute(
            """
            SELECT route_id::TEXT,
                   visit_no,
                   sta_id,
                   route_action_type_cd,
                   bike_cnt
              FROM rebalance_route_stop
             WHERE route_id = ANY(%s::UUID[])
             ORDER BY array_position(%s::UUID[], route_id), visit_no
            """,
            (list(route_ids), list(route_ids)),
        )
        stops = tuple(RebalanceRouteStop(*row) for row in cursor.fetchall())
    return RebalanceRoutePlan(routes, stops)


def _validate_route_output_artifacts(
    publication: PreparedPublication,
    payloads: Mapping[str, bytes],
    expected_plan: RebalanceRoutePlan,
) -> None:
    """Route output actual Parquet 두 개 또는 artifact 없는 EMPTY를 검증한다."""
    artifacts = {artifact.role: artifact for artifact in publication.manifest.artifacts}
    if not expected_plan.routes:
        if artifacts:
            raise ContractViolation(
                "EMPTY route publication에 output artifact가 있습니다."
            )
        return
    if set(artifacts) != {"routes", "route_stops"}:
        raise ContractViolation(
            "nonempty route publication에는 routes와 route_stops artifact가 필요합니다."
        )
    actual = route_plan_from_parquet(
        payloads[artifacts["routes"].uri],
        payloads[artifacts["route_stops"].uri],
        expected_plan=expected_plan,
    )
    if actual != expected_plan:
        raise ContractViolation("route output actual bytes가 expected plan과 다릅니다.")


def _require_route_evidence(
    evidence: tuple[VerifiedPublicationEvidence, ...],
) -> VerifiedPublicationEvidence:
    """Callback evidence가 rebalance_route 하나인지 검증한다."""
    if len(evidence) != 1 or evidence[0].manifest.publication_key != "rebalance_route":
        raise ContractViolation("rebalance_route publication evidence가 잘못됐습니다.")
    return evidence[0]


def _reconcile_route_plan(
    cursor: Cursor[tuple[Any, ...]],
    plan: RebalanceRoutePlan,
) -> None:
    """Proposed aggregate만 교체하고 terminal header·stop metadata를 보존한다."""
    terminal_before = _terminal_route_snapshot_locked(cursor)
    cursor.execute("DELETE FROM rebalance_route WHERE route_status_cd = 'proposed'")
    if plan.routes:
        cursor.executemany(
            """
            INSERT INTO rebalance_route (
                route_id,
                dispatch_center_id,
                route_status_cd,
                proposed_dttm,
                dispatched_dttm,
                completed_dttm
            ) VALUES (%s, %s, %s, %s, %s, %s)
            """,
            [
                (
                    route.route_id,
                    route.dispatch_center_id,
                    route.route_status_cd,
                    route.proposed_dttm,
                    route.dispatched_dttm,
                    route.completed_dttm,
                )
                for route in plan.routes
            ],
        )
        cursor.executemany(
            """
            INSERT INTO rebalance_route_stop (
                route_id,
                visit_no,
                sta_id,
                route_action_type_cd,
                bike_cnt
            ) VALUES (%s, %s, %s, %s, %s)
            """,
            [
                (
                    stop.route_id,
                    stop.visit_no,
                    stop.sta_id,
                    stop.route_action_type_cd,
                    stop.bike_cnt,
                )
                for stop in plan.route_stops
            ],
        )
    if (
        _current_proposed_plan_locked(
            cursor,
            expected_route_order=tuple(route.route_id for route in plan.routes),
        )
        != plan
    ):
        raise ContractViolation(
            "rebalance_route proposed full reconcile readback이 plan과 다릅니다."
        )
    if _terminal_route_snapshot_locked(cursor) != terminal_before:
        raise ContractViolation(
            "rebalance_route reconcile이 dispatched/completed 이력을 변경했습니다."
        )


def _terminal_route_snapshot_locked(
    cursor: Cursor[tuple[Any, ...]],
) -> tuple[tuple[Any, ...], tuple[tuple[Any, ...], ...]]:
    """Terminal route와 stop의 모든 business·metadata 값을 비교용으로 읽는다."""
    cursor.execute(
        """
        SELECT route_id::TEXT,
               dispatch_center_id,
               route_status_cd,
               proposed_dttm,
               dispatched_dttm,
               completed_dttm,
               created_dttm,
               updated_dttm
          FROM rebalance_route
         WHERE route_status_cd IN ('dispatched', 'completed')
         ORDER BY route_id::TEXT COLLATE "C"
        """
    )
    routes = tuple(cursor.fetchall())
    cursor.execute(
        """
        SELECT stop.route_id::TEXT,
               stop.visit_no,
               stop.sta_id,
               stop.route_action_type_cd,
               stop.bike_cnt,
               stop.created_dttm
          FROM rebalance_route_stop AS stop
          JOIN rebalance_route AS route USING (route_id)
         WHERE route.route_status_cd IN ('dispatched', 'completed')
         ORDER BY stop.route_id::TEXT COLLATE "C", stop.visit_no
        """
    )
    return routes, tuple(cursor.fetchall())


def _index_centers(
    centers: tuple[DispatchCenterTopology, ...],
) -> dict[str, DispatchCenterTopology]:
    """센터 tuple을 중복 없는 current-ID index로 바꾼다."""
    if type(centers) is not tuple or any(
        type(center) is not DispatchCenterTopology for center in centers
    ):
        raise ContractViolation(
            "dispatch_centers는 DispatchCenterTopology tuple이어야 합니다."
        )
    indexed = {center.dispatch_center_id: center for center in centers}
    if len(indexed) != len(centers):
        raise ContractViolation("dispatch center topology에 중복 ID가 있습니다.")
    return indexed


def _index_stations(
    stations: tuple[StationRouteTopology, ...],
) -> dict[str, StationRouteTopology]:
    """station tuple을 중복 없는 current-ID index로 바꾼다."""
    if type(stations) is not tuple or any(
        type(station) is not StationRouteTopology for station in stations
    ):
        raise ContractViolation("stations는 StationRouteTopology tuple이어야 합니다.")
    indexed = {station.sta_id: station for station in stations}
    if len(indexed) != len(stations):
        raise ContractViolation("station topology에 중복 sta_id가 있습니다.")
    return indexed


def _validate_urgency(
    urgency: tuple[RouteUrgencyInput, ...],
) -> tuple[RouteUrgencyInput, ...]:
    """urgency tuple의 exact type과 station 단일성을 검증한다."""
    if type(urgency) is not tuple or any(
        type(row) is not RouteUrgencyInput for row in urgency
    ):
        raise ContractViolation("urgency는 RouteUrgencyInput tuple이어야 합니다.")
    ids = tuple(row.sta_id for row in urgency)
    if len(ids) != len(set(ids)):
        raise ContractViolation("urgency input에 중복 sta_id가 있습니다.")
    return urgency


def _coverage_quantities(
    coverage: RouteCoverageDocument,
) -> dict[tuple[str, str], int]:
    """canonical coverage stop 수량을 station·action별로 합산한다."""
    quantities: dict[tuple[str, str], int] = {}
    for route in coverage.routes:
        load = INITIAL_TRUCK_LOAD
        for stop in route.stops:
            if stop.action == "pickup":
                load += stop.bike_cnt
            else:
                load -= stop.bike_cnt
            if not INITIAL_TRUCK_LOAD <= load <= TRUCK_CAPACITY:
                raise ContractViolation(
                    "route coverage stop prefix 적재량이 0..20 범위를 벗어납니다."
                )
            key = (stop.sta_id, stop.action)
            quantities[key] = quantities.get(key, 0) + stop.bike_cnt
    return quantities


def _build_candidates(
    urgency: tuple[RouteUrgencyInput, ...],
    *,
    stations_by_id: dict[str, StationRouteTopology],
    centers_by_id: dict[str, DispatchCenterTopology],
    coverage_qty: dict[tuple[str, str], int],
) -> dict[str, tuple[_Candidate, ...]]:
    """현재 station FK와 coverage를 적용한 센터별 결정적 후보를 만든다."""
    candidates: dict[str, list[_Candidate]] = {}
    for row in urgency:
        action = _route_action(row.action_type)
        if action is None or row.bike_qty <= 0:
            continue
        station = stations_by_id.get(row.sta_id)
        if station is None or not station.is_active:
            raise ContractViolation(
                f"actionable urgency station이 current active topology에 없습니다: {row.sta_id}"
            )
        center = centers_by_id.get(station.dispatch_center_id)
        if center is None or not center.is_active:
            raise ContractViolation(
                f"actionable urgency station의 current center가 active가 아닙니다: {row.sta_id}"
            )
        remaining = row.bike_qty - coverage_qty.get((row.sta_id, action), 0)
        if remaining <= 0:
            continue
        candidates.setdefault(station.dispatch_center_id, []).append(
            _Candidate(
                sta_id=row.sta_id,
                action=action,
                urgency_score=row.urgency_score,
                remaining_qty=remaining,
                longitude=station.longitude,
                latitude=station.latitude,
            )
        )
    return {
        center_id: tuple(sorted(rows, key=_candidate_priority))
        for center_id, rows in candidates.items()
    }


def _route_action(action_type: str) -> str | None:
    """urgency 판단 코드를 route 작업으로 exact 변환한다."""
    if action_type == "retrieval_needed":
        return "pickup"
    if action_type == "supply_needed":
        return "dropoff"
    return None


def _candidate_priority(candidate: _Candidate) -> tuple[float, bytes]:
    """후보 선택의 score 내림차순·station ID 오름차순 key를 반환한다."""
    return (-candidate.urgency_score, _utf8_key(candidate.sta_id))


def _rank_for_route(
    candidates: list[_Candidate],
    *,
    anchor: _Candidate,
    keep_anchor_first: bool = False,
) -> list[_Candidate]:
    """최고 긴급 pickup 주변의 처리효율 순으로 한 작업 후보를 정렬한다.

    첫 pickup은 전체 긴급도 순서를 유지한다. 추가 stop은 긴급도가 높고 anchor에
    가까울수록 앞서는 (urgency + 1) / (distance + 1) 점수를 사용한다.
    """

    def route_score(candidate: _Candidate) -> tuple[float, float, float, bytes]:
        """후보의 긴급도·거리 결합 정렬 key를 반환한다."""
        distance = _haversine_km(
            anchor.longitude,
            anchor.latitude,
            candidate.longitude,
            candidate.latitude,
        )
        efficiency = (candidate.urgency_score + 1.0) / (distance + 1.0)
        return (
            -efficiency,
            -candidate.urgency_score,
            distance,
            _utf8_key(candidate.sta_id),
        )

    ranked = sorted(candidates, key=route_score)
    if not keep_anchor_first:
        return ranked
    return [anchor, *(candidate for candidate in ranked if candidate != anchor)]


def _take_by_priority(
    candidates: list[_Candidate],
    limit: int,
    *,
    stop_limit: int | None = None,
) -> tuple[tuple[_SelectedStop, ...], list[_Candidate]]:
    """후보 우선순위를 보존하며 양수 limit까지 수량을 배정한다."""
    if type(limit) is not int or limit < 0:
        raise ContractViolation("route selection limit은 0 이상 integer여야 합니다.")
    if stop_limit is not None and (
        type(stop_limit) is not int or stop_limit < 0
    ):
        raise ContractViolation("route stop limit은 0 이상 integer여야 합니다.")
    if limit == 0 or not candidates:
        return (), list(candidates)
    selected: list[_SelectedStop] = []
    remaining: list[_Candidate] = []
    available = limit
    for candidate in candidates:
        if available == 0 or (
            stop_limit is not None and len(selected) >= stop_limit
        ):
            remaining.append(candidate)
            continue
        quantity = min(candidate.remaining_qty, available)
        selected.append(_SelectedStop(candidate, quantity))
        available -= quantity
        if quantity < candidate.remaining_qty:
            remaining.append(
                _Candidate(
                    sta_id=candidate.sta_id,
                    action=candidate.action,
                    urgency_score=candidate.urgency_score,
                    remaining_qty=candidate.remaining_qty - quantity,
                    longitude=candidate.longitude,
                    latitude=candidate.latitude,
                )
            )
    return tuple(selected), remaining


def _choose_balanced_stop_split(
    pickups: list[_Candidate],
    dropoffs: list[_Candidate],
    *,
    max_stops_per_route: int = MAX_STOPS_PER_ROUTE,
) -> tuple[int, int, int] | None:
    """설정된 대여소 상한으로 가장 많은 수량을 완결할 stop 배분을 고른다.

    pickup과 dropoff에 각각 한 자리를 보장하고, 처리 가능 수량이 같으면 더 적은
    대여소와 더 높은 긴급도 합을 우선한다. 후보 목록은 이미 경로 효율 순으로
    정렬되어 있으므로 각 action의 앞쪽 N개만 비교해도 결정적이다.
    """
    if not pickups or not dropoffs:
        return None
    if type(max_stops_per_route) is not int or not 2 <= max_stops_per_route <= 32767:
        raise ContractViolation("max_stops_per_route는 2..32767 integer여야 합니다.")
    best: tuple[tuple[int, int, float, int, int], tuple[int, int, int]] | None = None
    max_pickup_stops = min(len(pickups), max_stops_per_route - 1)
    for pickup_stop_limit in range(1, max_pickup_stops + 1):
        max_dropoff_stops = min(
            len(dropoffs),
            max_stops_per_route - pickup_stop_limit,
        )
        pickup_candidates = pickups[:pickup_stop_limit]
        pickup_capacity = sum(item.remaining_qty for item in pickup_candidates)
        for dropoff_stop_limit in range(1, max_dropoff_stops + 1):
            dropoff_candidates = dropoffs[:dropoff_stop_limit]
            transfer_qty = min(
                TRUCK_CAPACITY,
                pickup_capacity,
                sum(item.remaining_qty for item in dropoff_candidates),
            )
            if transfer_qty <= 0:
                continue
            stop_count = pickup_stop_limit + dropoff_stop_limit
            urgency_sum = sum(
                item.urgency_score
                for item in (*pickup_candidates, *dropoff_candidates)
            )
            key = (
                transfer_qty,
                -stop_count,
                urgency_sum,
                -pickup_stop_limit,
                -dropoff_stop_limit,
            )
            value = (pickup_stop_limit, dropoff_stop_limit, transfer_qty)
            if best is None or key > best[0]:
                best = (key, value)
    return None if best is None else best[1]


def _nearest_stops(
    pickups: tuple[_SelectedStop, ...],
    dropoffs: tuple[_SelectedStop, ...],
    *,
    start: tuple[float, float],
) -> tuple[_SelectedStop, ...]:
    """센터부터 pickup을 거쳐 dropoff까지 이어지는 최근접 순서를 만든다."""
    pickup_order, pickup_end = _nearest_order(pickups, start=start)
    dropoff_order, _ = _nearest_order(dropoffs, start=pickup_end)
    return pickup_order + dropoff_order


def _nearest_order(
    stops: tuple[_SelectedStop, ...],
    *,
    start: tuple[float, float],
) -> tuple[tuple[_SelectedStop, ...], tuple[float, float]]:
    """현재 Point에서 거리·station ID 동률 순으로 다음 stop을 반복 선택한다."""
    remaining = list(stops)
    ordered: list[_SelectedStop] = []
    current = start
    while remaining:
        nearest = min(
            remaining,
            key=lambda stop: (
                _haversine_km(
                    current[0],
                    current[1],
                    stop.candidate.longitude,
                    stop.candidate.latitude,
                ),
                _utf8_key(stop.candidate.sta_id),
            ),
        )
        ordered.append(nearest)
        remaining.remove(nearest)
        current = (nearest.candidate.longitude, nearest.candidate.latitude)
    return tuple(ordered), current


def _validate_running_load(stops: list[RebalanceRouteStop]) -> None:
    """초기 0부터 모든 stop prefix 적재량이 0..20인지 검증한다."""
    load = INITIAL_TRUCK_LOAD
    for stop in stops:
        if stop.route_action_type_cd == "pickup":
            load += stop.bike_cnt
        else:
            load -= stop.bike_cnt
        if not INITIAL_TRUCK_LOAD <= load <= TRUCK_CAPACITY:
            raise ContractViolation(
                "route stop prefix 적재량이 0..20 범위를 벗어납니다."
            )


def _validate_existing_running_load(stops: tuple[ExistingRouteStop, ...]) -> None:
    """coverage 대상 terminal route도 초기 0의 prefix 적재량을 재검증한다."""
    load = INITIAL_TRUCK_LOAD
    for stop in stops:
        if stop.action == "pickup":
            load += stop.bike_cnt
        else:
            load -= stop.bike_cnt
        if not INITIAL_TRUCK_LOAD <= load <= TRUCK_CAPACITY:
            raise ContractViolation(
                "existing route stop prefix 적재량이 0..20 범위를 벗어납니다."
            )


def _haversine_km(
    longitude_a: float,
    latitude_a: float,
    longitude_b: float,
    latitude_b: float,
) -> float:
    """두 WGS84 좌표 사이의 구면 거리 km를 계산한다."""
    radius_km = 6371.0
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = math.radians(latitude_b - latitude_a)
    delta_lambda = math.radians(longitude_b - longitude_a)
    haversine = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi_a) * math.cos(phi_b) * math.sin(delta_lambda / 2.0) ** 2
    )
    return 2.0 * radius_km * math.asin(math.sqrt(haversine))


def _station_id(value: object) -> str:
    """SSOT station 자연키를 검증해 반환한다."""
    text = _nonblank_text(value, "sta_id")
    if _STATION_ID.fullmatch(text) is None:
        raise ContractViolation("sta_id는 ST-[0-9]+ 형식이어야 합니다.")
    return text


def _nonblank_text(value: object, label: str) -> str:
    """NFC nonblank text를 검증해 반환한다."""
    if type(value) is not str or not value.strip():
        raise ContractViolation(f"{label}는 nonblank string이어야 합니다.")
    if unicodedata.normalize("NFC", value) != value:
        raise ContractViolation(f"{label}는 NFC 문자열이어야 합니다.")
    return value


def _canonical_uuid(value: object, label: str) -> str:
    """lowercase hyphen canonical UUID 문자열을 검증해 반환한다."""
    text = _nonblank_text(value, label)
    try:
        parsed = UUID(text)
    except ValueError as exc:
        raise ContractViolation(f"{label}가 UUID가 아닙니다.") from exc
    if str(parsed) != text:
        raise ContractViolation(f"{label}는 canonical UUID 문자열이어야 합니다.")
    return text


def _point(longitude: object, latitude: object, label: str) -> tuple[float, float]:
    """Gold DDL 서울 안전 범위의 유한 Point 좌표를 검증한다."""
    lon = _bounded_float(longitude, f"{label} longitude", 126.5, 127.5)
    lat = _bounded_float(latitude, f"{label} latitude", 37.0, 38.0)
    return lon, lat


def _bounded_float(value: object, label: str, minimum: float, maximum: float) -> float:
    """bool이 아닌 유한 실수를 닫힌 범위에서 검증한다."""
    if type(value) not in {int, float}:
        raise ContractViolation(f"{label}는 finite number여야 합니다.")
    number = float(value)
    if not math.isfinite(number) or not minimum <= number <= maximum:
        raise ContractViolation(f"{label}가 {minimum}..{maximum} 범위를 벗어났습니다.")
    return number


def _utc_dttm(value: object, label: str) -> datetime:
    """timezone-aware datetime을 UTC로 정규화한다."""
    if type(value) is not datetime or value.tzinfo is None or value.utcoffset() is None:
        raise ContractViolation(f"{label}는 timezone-aware datetime이어야 합니다.")
    return value.astimezone(UTC)


def _optional_utc_dttm(value: object, label: str) -> datetime | None:
    """nullable timezone-aware datetime을 UTC로 정규화한다."""
    if value is None:
        return None
    return _utc_dttm(value, label)


def _nonnegative_integer(value: object, label: str) -> int:
    """bool이 아닌 0 이상 integer를 검증해 반환한다."""
    if type(value) is not int or value < 0:
        raise ContractViolation(f"{label}는 0 이상 integer여야 합니다.")
    return value


def _postgres_nonnegative_integer(value: object, label: str) -> int:
    """값을 target의 비음수 PostgreSQL INTEGER 범위로 검증해 반환한다."""
    number = _nonnegative_integer(value, label)
    if number > _POSTGRES_INTEGER_MAX:
        raise ContractViolation(f"{label}가 PostgreSQL INTEGER 범위를 벗어났습니다.")
    return number


def _utf8_key(value: str) -> bytes:
    """SSOT 문자열 정렬용 UTF-8 bytes key를 반환한다."""
    return value.encode("utf-8")
