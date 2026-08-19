"""Gold 재배치 경로의 결정적 순수 planner와 coverage projection을 제공한다."""

from __future__ import annotations

import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

import pyarrow as pa
from core.gold_publication import (
    ContractViolation,
    RouteCoverageDocument,
    RouteCoverageStop,
    build_route_coverage,
    build_route_coverage_route,
    route_uuid_v5,
)

from .common import parquet_bytes, read_parquet_bytes

ROUTE_ALGORITHM_VERSION = "route-v1"
TRUCK_CAPACITY = 20
TRUCK_CAPACITY_CONFIG_VERSION = "truck-capacity-v1"
INITIAL_TRUCK_LOAD = 0

_STATION_ID = re.compile(r"ST-[0-9]+\Z")
_URGENCY_ACTIONS = {"normal", "supply_needed", "retrieval_needed"}
_ROUTE_ACTIONS = {"pickup", "dropoff"}
_ROUTE_STATUSES = {"proposed", "dispatched", "completed"}
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
    """urgency artifact에서 route-v1이 소비하는 최소 typed 필드를 표현한다."""

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
) -> RebalanceRoutePlan:
    """route-v1 정렬·coverage·용량 계약으로 proposed aggregate를 계산한다.

    ``normal``과 ``bike_qty<=0`` 행은 route 후보가 아니므로 이 순수 planner는
    topology 존재를 요구하지 않는다. urgency 기대 집합의 완전성은 publication
    manifest를 검증하는 publisher 경계가 담당한다.
    """
    logical = _utc_dttm(logical_dttm, "logical_dttm")
    _nonnegative_integer(revision_no, "revision_no")
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
        while pickups:
            selected_pickups, pickups = _take_by_priority(pickups, TRUCK_CAPACITY)
            picked_qty = sum(stop.bike_cnt for stop in selected_pickups)
            selected_dropoffs, dropoffs = _take_by_priority(dropoffs, picked_qty)
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


def _take_by_priority(
    candidates: list[_Candidate],
    limit: int,
) -> tuple[tuple[_SelectedStop, ...], list[_Candidate]]:
    """후보 우선순위를 보존하며 양수 limit까지 수량을 배정한다."""
    if type(limit) is not int or limit < 0:
        raise ContractViolation("route selection limit은 0 이상 integer여야 합니다.")
    if limit == 0 or not candidates:
        return (), list(candidates)
    selected: list[_SelectedStop] = []
    remaining: list[_Candidate] = []
    available = limit
    for candidate in candidates:
        if available == 0:
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


def _nearest_stops(
    pickups: tuple[_SelectedStop, ...],
    dropoffs: tuple[_SelectedStop, ...],
    *,
    start: tuple[float, float],
) -> tuple[_SelectedStop, ...]:
    """센터 기준으로 pickup과 dropoff를 각각 최근접 순서로 만든다."""
    pickup_order, _ = _nearest_order(pickups, start=start)
    dropoff_order, _ = _nearest_order(dropoffs, start=start)
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


def _utf8_key(value: str) -> bytes:
    """SSOT 문자열 정렬용 UTF-8 bytes key를 반환한다."""
    return value.encode("utf-8")
