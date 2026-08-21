"""Gold rebalance route-v2 planner·coverage·Parquet 계약을 검증한다."""

from __future__ import annotations

from collections import Counter
from datetime import UTC, datetime, timedelta
from types import SimpleNamespace

import pyarrow as pa
import pytest
from core.gold_publication import (
    ContractViolation,
    RouteCoverageStop,
    build_route_coverage,
    build_route_coverage_route,
    parse_route_coverage,
    route_uuid_v5,
)

from gold import rebalance_route as route_module
from gold.common import parquet_bytes, read_parquet_bytes
from gold.rebalance_policy import (
    LEGACY_REBALANCE_POLICY,
    RebalancePolicyConfig,
    risk_band_policy,
)
from gold.rebalance_route import (
    INITIAL_TRUCK_LOAD,
    MAX_ROUTES_PER_CENTER,
    MAX_STOPS_PER_ROUTE,
    ROUTE_ALGORITHM_VERSION,
    ROUTE_WORK_UNIT_CONFIG_VERSION,
    TRUCK_CAPACITY,
    TRUCK_CAPACITY_CONFIG_VERSION,
    DispatchCenterTopology,
    ExistingRoute,
    ExistingRouteStop,
    RebalanceRoute,
    RebalanceRoutePlan,
    RebalanceRouteStop,
    RouteUrgencyInput,
    StationRouteTopology,
    build_current_route_coverage,
    plan_rebalance_routes,
    route_plan_from_parquet,
    route_plan_to_parquet,
)
from gold.state import PublicationStateRecord

_UTC_1600 = datetime(2026, 8, 19, 16, 0, tzinfo=UTC)
_CENTER_LONGITUDE = 127.0
_CENTER_LATITUDE = 37.5


def _center(
    center_id: str = "center_a",
    *,
    longitude: float = _CENTER_LONGITUDE,
    latitude: float = _CENTER_LATITUDE,
    is_active: bool = True,
) -> DispatchCenterTopology:
    """테스트용 current dispatch center를 만든다."""
    return DispatchCenterTopology(center_id, longitude, latitude, is_active)


def _station(
    station_id: str,
    *,
    center_id: str = "center_a",
    longitude: float = _CENTER_LONGITUDE,
    latitude: float = _CENTER_LATITUDE,
    is_active: bool = True,
) -> StationRouteTopology:
    """테스트용 current station topology를 만든다."""
    return StationRouteTopology(
        station_id,
        center_id,
        longitude,
        latitude,
        is_active,
    )


def _urgency(
    station_id: str,
    action_type: str,
    bike_qty: int,
    *,
    score: float = 50.0,
) -> RouteUrgencyInput:
    """테스트용 route urgency input을 만든다."""
    return RouteUrgencyInput(station_id, score, action_type, bike_qty)


def _stop(
    station_id: str,
    *,
    action: str = "pickup",
    bike_cnt: int = 1,
    visit_no: int = 1,
) -> ExistingRouteStop:
    """테스트용 existing route stop을 만든다."""
    return ExistingRouteStop(visit_no, station_id, action, bike_cnt)


def _existing_route(
    route_id: str,
    status: str,
    *,
    stops: tuple[ExistingRouteStop, ...] = (_stop("ST-1"),),
    dispatched_dttm: datetime | None = None,
    completed_dttm: datetime | None = None,
) -> ExistingRoute:
    """테스트용 existing route aggregate를 만든다."""
    return ExistingRoute(
        route_id=route_id,
        route_status_cd=status,
        dispatched_dttm=dispatched_dttm,
        completed_dttm=completed_dttm,
        stops=stops,
    )


def _empty_coverage():
    """테스트 anchor의 EMPTY canonical route coverage를 만든다."""
    return build_current_route_coverage(stock_anchor_dttm=_UTC_1600, routes=())


def _plan(
    *,
    centers: tuple[DispatchCenterTopology, ...] = (_center(),),
    stations: tuple[StationRouteTopology, ...],
    urgency: tuple[RouteUrgencyInput, ...],
    revision_no: int = 0,
    coverage=None,
    max_stops_per_route: int = MAX_STOPS_PER_ROUTE,
    policy_config: RebalancePolicyConfig = LEGACY_REBALANCE_POLICY,
    pickup_cooldown_sta_ids: frozenset[str] = frozenset(),
) -> RebalanceRoutePlan:
    """공통 anchor로 pure route planner를 실행한다."""
    return plan_rebalance_routes(
        logical_dttm=_UTC_1600,
        revision_no=revision_no,
        dispatch_centers=centers,
        stations=stations,
        urgency=urgency,
        route_coverage=coverage or _empty_coverage(),
        max_stops_per_route=max_stops_per_route,
        policy_config=policy_config,
        pickup_cooldown_sta_ids=pickup_cooldown_sta_ids,
    )


def test_route_constants_match_publication_contract() -> None:
    """route fingerprint에 들어가는 v3 작업 단위 상수를 SSOT 값으로 고정한다."""
    assert ROUTE_ALGORITHM_VERSION == "route-v3-risk-band"
    assert TRUCK_CAPACITY == 20
    assert TRUCK_CAPACITY_CONFIG_VERSION == "truck-capacity-v1"
    assert INITIAL_TRUCK_LOAD == 0
    assert MAX_STOPS_PER_ROUTE == 5
    assert MAX_ROUTES_PER_CENTER == 3
    assert ROUTE_WORK_UNIT_CONFIG_VERSION == "route-work-unit-v2"


def test_planner_can_override_stop_limit_for_offline_evaluation() -> None:
    """게시 기본값을 바꾸지 않고 순수 planner의 stop 상한을 좁혀 비교한다."""
    stations = tuple(_station(f"ST-{index}") for index in range(1, 9))
    urgency = tuple(
        _urgency(f"ST-{index}", "retrieval_needed", 1) for index in range(1, 5)
    ) + tuple(_urgency(f"ST-{index}", "supply_needed", 1) for index in range(5, 9))

    plan = _plan(
        stations=stations,
        urgency=urgency,
        max_stops_per_route=5,
    )
    route_stop_counts = Counter(stop.route_id for stop in plan.route_stops)

    assert route_stop_counts
    assert max(route_stop_counts.values()) <= 5
    with pytest.raises(ContractViolation, match="max_stops_per_route"):
        _plan(stations=stations, urgency=urgency, max_stops_per_route=1)


def test_empty_plan_excludes_normal_and_nonpositive_quantities() -> None:
    """후보 아닌 urgency는 topology를 요구하지 않고 정상 EMPTY가 된다."""
    plan = _plan(
        stations=(),
        urgency=(
            _urgency("ST-1", "normal", 8),
            _urgency("ST-2", "supply_needed", 0),
            _urgency("ST-3", "retrieval_needed", -2),
        ),
    )

    assert plan == RebalanceRoutePlan((), ())
    with pytest.raises(ContractViolation, match="EMPTY.*artifact"):
        route_plan_to_parquet(plan)


def test_empty_publication_rejects_physical_parquet_artifacts() -> None:
    """SSOT EMPTY artifact_set=[] 계약상 빈 header·stop Parquet도 거부한다."""
    empty_routes = parquet_bytes(
        pa.Table.from_pylist(
            [],
            schema=pa.schema(
                (
                    pa.field("route_id", pa.string(), nullable=False),
                    pa.field("dispatch_center_id", pa.string(), nullable=False),
                    pa.field("route_status_cd", pa.string(), nullable=False),
                    pa.field(
                        "proposed_dttm",
                        pa.timestamp("us", tz="UTC"),
                        nullable=False,
                    ),
                    pa.field(
                        "dispatched_dttm",
                        pa.timestamp("us", tz="UTC"),
                        nullable=True,
                    ),
                    pa.field(
                        "completed_dttm",
                        pa.timestamp("us", tz="UTC"),
                        nullable=True,
                    ),
                )
            ),
        )
    )
    empty_stops = parquet_bytes(
        pa.Table.from_pylist(
            [],
            schema=pa.schema(
                (
                    pa.field("route_id", pa.string(), nullable=False),
                    pa.field("visit_no", pa.int16(), nullable=False),
                    pa.field("sta_id", pa.string(), nullable=False),
                    pa.field("route_action_type_cd", pa.string(), nullable=False),
                    pa.field("bike_cnt", pa.int32(), nullable=False),
                )
            ),
        )
    )

    with pytest.raises(ContractViolation, match="EMPTY.*artifact"):
        route_plan_from_parquet(
            empty_routes,
            empty_stops,
            expected_plan=RebalanceRoutePlan((), ()),
        )


def test_plan_rejects_route_coverage_from_a_different_stock_anchor() -> None:
    """urgency anchor와 coverage stock anchor가 다르면 stale 계산을 거부한다."""
    stale_coverage = build_current_route_coverage(
        stock_anchor_dttm=_UTC_1600 - timedelta(minutes=5),
        routes=(),
    )

    with pytest.raises(ContractViolation, match="stock anchor"):
        _plan(
            stations=(_station("ST-1"),),
            urgency=(_urgency("ST-1", "retrieval_needed", 1),),
            coverage=stale_coverage,
        )


def test_coverage_keeps_dispatched_and_unobserved_completed_only() -> None:
    """coverage는 proposed·관측 완료를 빼고 실행 중·anchor 뒤 완료만 보존한다."""
    proposed = _existing_route(
        "00000000-0000-0000-0000-000000000004",
        "proposed",
    )
    dispatched = _existing_route(
        "00000000-0000-0000-0000-000000000002",
        "dispatched",
        dispatched_dttm=_UTC_1600 - timedelta(hours=2),
    )
    observed_completed = _existing_route(
        "00000000-0000-0000-0000-000000000003",
        "completed",
        dispatched_dttm=_UTC_1600 - timedelta(hours=2),
        completed_dttm=_UTC_1600,
    )
    unobserved_completed = _existing_route(
        "00000000-0000-0000-0000-000000000001",
        "completed",
        dispatched_dttm=_UTC_1600 - timedelta(minutes=5),
        completed_dttm=_UTC_1600 + timedelta(minutes=1),
    )

    coverage = build_current_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(proposed, dispatched, observed_completed, unobserved_completed),
    )

    assert tuple(route.route_id for route in coverage.routes) == (
        unobserved_completed.route_id,
        dispatched.route_id,
    )
    assert tuple(route.status for route in coverage.routes) == (
        "completed",
        "dispatched",
    )
    assert parse_route_coverage(coverage.canonical_bytes) == coverage


def test_coverage_matches_publication_contract_fixed_vector() -> None:
    """DB projection 경계에서도 SSOT canonical coverage bytes와 SHA를 재현한다."""
    coverage = build_current_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(
            _existing_route(
                "00000000-0000-0000-0000-000000000001",
                "dispatched",
                stops=(_stop("ST-9001", bike_cnt=3),),
                dispatched_dttm=_UTC_1600 + timedelta(minutes=2),
            ),
        ),
    )

    assert coverage.canonical_bytes == (
        b'{"routes":[{"completed_dttm":null,"dispatched_dttm":'
        b'"2026-08-19T16:02:00.000000Z","route_id":'
        b'"00000000-0000-0000-0000-000000000001","status":"dispatched",'
        b'"stops":[{"action":"pickup","bike_cnt":3,"sta_id":"ST-9001",'
        b'"visit_no":1}]}],"schema_version":"gold-route-coverage-v1",'
        b'"stock_anchor_dttm":"2026-08-19T16:00:00.000000Z"}'
    )
    assert coverage.sha256 == (
        "13cd1f4fe82d4b09370fd4141d1ee1a727f25c5b109de11f06bb904f9c001e8b"
    )


def test_terminal_coverage_nets_action_specific_quantities() -> None:
    """dispatched와 anchor 뒤 completed stop을 같은 action의 필요량에서 차감한다."""
    dispatched = _existing_route(
        "00000000-0000-0000-0000-000000000001",
        "dispatched",
        stops=(_stop("ST-1", bike_cnt=3),),
        dispatched_dttm=_UTC_1600 - timedelta(minutes=10),
    )
    completed = _existing_route(
        "00000000-0000-0000-0000-000000000002",
        "completed",
        stops=(
            _stop("ST-99", action="pickup", bike_cnt=2),
            _stop("ST-2", action="dropoff", bike_cnt=2, visit_no=2),
        ),
        dispatched_dttm=_UTC_1600 - timedelta(minutes=10),
        completed_dttm=_UTC_1600 + timedelta(seconds=1),
    )
    proposed = _existing_route(
        "00000000-0000-0000-0000-000000000003",
        "proposed",
        stops=(_stop("ST-1", bike_cnt=4),),
    )
    coverage = build_current_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(dispatched, completed, proposed),
    )

    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 8),
            _urgency("ST-2", "supply_needed", 8),
        ),
        coverage=coverage,
    )

    assert [
        (stop.sta_id, stop.route_action_type_cd, stop.bike_cnt)
        for stop in plan.route_stops
    ] == [
        ("ST-1", "pickup", 5),
        ("ST-2", "dropoff", 5),
    ]
    assert all(
        route_id not in {dispatched.route_id, completed.route_id}
        for route_id in (route.route_id for route in plan.routes)
    )


def test_coverage_rejects_terminal_route_with_invalid_prefix_load() -> None:
    """DB drift가 있어도 초기 적재 0에서 dropoff부터 시작한 route를 netting하지 않는다."""
    with pytest.raises(ContractViolation, match="existing route stop prefix"):
        _existing_route(
            "00000000-0000-0000-0000-000000000001",
            "dispatched",
            stops=(_stop("ST-1", action="dropoff", bike_cnt=5),),
            dispatched_dttm=_UTC_1600 - timedelta(minutes=5),
        )


def test_planner_revalidates_canonical_coverage_prefix_load() -> None:
    """직접 전달된 canonical coverage도 dropoff-only netting을 우회하지 못한다."""
    bad_coverage = build_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(
            build_route_coverage_route(
                completed_dttm=None,
                dispatched_dttm=_UTC_1600 - timedelta(minutes=5),
                route_id="00000000-0000-0000-0000-000000000001",
                status="dispatched",
                stops=(
                    RouteCoverageStop(
                        action="dropoff",
                        bike_cnt=5,
                        sta_id="ST-2",
                        visit_no=1,
                    ),
                ),
            ),
        ),
    )

    with pytest.raises(ContractViolation, match="coverage stop prefix"):
        _plan(
            stations=(_station("ST-1"), _station("ST-2")),
            urgency=(
                _urgency("ST-1", "retrieval_needed", 5),
                _urgency("ST-2", "supply_needed", 5),
            ),
            coverage=bad_coverage,
        )


def test_current_station_center_fk_controls_grouping_and_center_order() -> None:
    """Point 최근접 재계산 없이 current station FK를 쓰고 center ID 순으로 생성한다."""
    center_a = _center("center_a", longitude=126.90)
    center_b = _center("center_b", longitude=127.10)
    stations = (
        _station("ST-1", center_id="center_b", longitude=126.9001),
        _station("ST-2", center_id="center_a", longitude=127.0999),
        _station("ST-3", center_id="center_b", longitude=126.9002),
        _station("ST-4", center_id="center_a", longitude=127.0998),
    )

    plan = _plan(
        centers=(center_b, center_a),
        stations=stations,
        urgency=(
            _urgency("ST-1", "retrieval_needed", 1),
            _urgency("ST-2", "retrieval_needed", 1),
            _urgency("ST-3", "supply_needed", 1),
            _urgency("ST-4", "supply_needed", 1),
        ),
    )

    assert tuple(route.dispatch_center_id for route in plan.routes) == (
        "center_a",
        "center_b",
    )
    stations_by_route = {
        route.route_id: {
            stop.sta_id for stop in plan.route_stops if stop.route_id == route.route_id
        }
        for route in plan.routes
    }
    assert stations_by_route[plan.routes[0].route_id] == {"ST-2", "ST-4"}
    assert stations_by_route[plan.routes[1].route_id] == {"ST-1", "ST-3"}


def test_candidate_and_nearest_ties_break_by_station_id() -> None:
    """후보 점수와 최근접 거리가 같으면 모두 sta_id 오름차순으로 결정한다."""
    stations = (
        _station("ST-2", longitude=127.001),
        _station("ST-1", longitude=127.001),
        _station("ST-3", longitude=127.002),
        _station("ST-4", longitude=127.003),
    )
    plan = _plan(
        stations=stations,
        urgency=(
            _urgency("ST-2", "retrieval_needed", 15, score=90),
            _urgency("ST-1", "retrieval_needed", 15, score=90),
            _urgency("ST-3", "retrieval_needed", 1, score=10),
            _urgency("ST-4", "supply_needed", 20, score=50),
        ),
    )

    first_route_id = plan.routes[0].route_id
    first_stops = [stop for stop in plan.route_stops if stop.route_id == first_route_id]
    assert [(stop.sta_id, stop.bike_cnt) for stop in first_stops] == [
        ("ST-1", 15),
        ("ST-2", 5),
        ("ST-4", 20),
    ]


def test_additional_stop_combines_urgency_with_anchor_distance() -> None:
    """첫 긴급 pickup은 보존하고 추가 stop은 가까운 작업 효율을 반영한다."""
    plan = _plan(
        stations=(
            _station("ST-1", longitude=127.0),
            _station("ST-2", longitude=127.1),
            _station("ST-3", longitude=127.001),
            _station("ST-4", longitude=127.002),
        ),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 10, score=100),
            _urgency("ST-2", "retrieval_needed", 10, score=80),
            _urgency("ST-3", "retrieval_needed", 10, score=70),
            _urgency("ST-4", "supply_needed", 20, score=60),
        ),
    )

    selected_ids = {stop.sta_id for stop in plan.route_stops}
    assert selected_ids == {"ST-1", "ST-3", "ST-4"}


def test_pickups_precede_dropoffs_and_dropoff_continues_from_last_pickup() -> None:
    """route-v2는 마지막 pickup 위치에서 가까운 dropoff로 동선을 이어간다."""
    stations = (
        _station("ST-1", longitude=127.01),
        _station("ST-2", longitude=127.011),
        _station("ST-3", longitude=127.001),
        _station("ST-4", longitude=127.012),
    )
    plan = _plan(
        stations=stations,
        urgency=(
            _urgency("ST-1", "retrieval_needed", 5),
            _urgency("ST-2", "retrieval_needed", 5),
            _urgency("ST-3", "supply_needed", 5),
            _urgency("ST-4", "supply_needed", 5),
        ),
    )

    assert [stop.sta_id for stop in plan.route_stops] == [
        "ST-1",
        "ST-2",
        "ST-4",
        "ST-3",
    ]
    assert [stop.route_action_type_cd for stop in plan.route_stops] == [
        "pickup",
        "pickup",
        "dropoff",
        "dropoff",
    ]


def test_capacity_rollover_uses_one_based_ordinal_and_ssot_uuid() -> None:
    """20대를 넘는 pickup은 center 내 ordinal 1부터 결정적 회차로 분리한다."""
    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 25),
            _urgency("ST-2", "supply_needed", 25),
        ),
    )

    assert [stop.bike_cnt for stop in plan.route_stops] == [20, 20, 5, 5]
    assert [route.route_id for route in plan.routes] == [
        "7dd58c8d-7dc7-5279-8845-7673c9c87be2",
        str(route_uuid_v5("center_a", _UTC_1600, 0, 2)),
    ]


def test_dropoff_is_limited_by_actual_pickup_per_route() -> None:
    """각 회차 pickup과 dropoff 수량은 같은 완결 작업이다."""
    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 25),
            _urgency("ST-2", "supply_needed", 50),
        ),
    )

    for route in plan.routes:
        stops = [stop for stop in plan.route_stops if stop.route_id == route.route_id]
        picked = sum(
            stop.bike_cnt for stop in stops if stop.route_action_type_cd == "pickup"
        )
        dropped = sum(
            stop.bike_cnt for stop in stops if stop.route_action_type_cd == "dropoff"
        )
        assert dropped == picked <= TRUCK_CAPACITY
    assert (
        sum(
            stop.bike_cnt
            for stop in plan.route_stops
            if stop.route_action_type_cd == "dropoff"
        )
        == 25
    )


def test_unpaired_action_does_not_create_incomplete_route() -> None:
    """회수 또는 배치 한쪽만 있으면 실행 불가능한 proposed 작업을 만들지 않는다."""
    pickup_only = _plan(
        stations=(_station("ST-1"),),
        urgency=(_urgency("ST-1", "retrieval_needed", 20),),
    )
    dropoff_only = _plan(
        stations=(_station("ST-2"),),
        urgency=(_urgency("ST-2", "supply_needed", 20),),
    )

    assert pickup_only == RebalanceRoutePlan((), ())
    assert dropoff_only == RebalanceRoutePlan((), ())


def test_work_unit_has_at_most_eight_stops_and_center_route_cap() -> None:
    """작업은 2~8개 대여소로 완결되고 센터별 상위 3개까지만 제안한다."""
    pickup_stations = tuple(_station(f"ST-{100 + index}") for index in range(1, 13))
    dropoff_stations = tuple(_station(f"ST-{200 + index}") for index in range(1, 13))
    plan = _plan(
        stations=pickup_stations + dropoff_stations,
        urgency=tuple(
            _urgency(station.sta_id, "retrieval_needed", 1, score=100 - index)
            for index, station in enumerate(pickup_stations)
        )
        + tuple(
            _urgency(station.sta_id, "supply_needed", 1, score=100 - index)
            for index, station in enumerate(dropoff_stations)
        ),
    )

    assert len(plan.routes) == MAX_ROUTES_PER_CENTER
    for route in plan.routes:
        stops = [stop for stop in plan.route_stops if stop.route_id == route.route_id]
        assert 2 <= len(stops) <= MAX_STOPS_PER_ROUTE
        picked = sum(
            stop.bike_cnt for stop in stops if stop.route_action_type_cd == "pickup"
        )
        dropped = sum(
            stop.bike_cnt for stop in stops if stop.route_action_type_cd == "dropoff"
        )
        assert picked == dropped > 0


def test_exclusive_pickup_station_is_not_split_across_concurrent_routes() -> None:
    """한 공급원의 큰 수량은 한 plan에서 한 경로에만 배정하고 잔량은 재평가한다."""
    policy = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )
    plan = _plan(
        stations=tuple(_station(f"ST-{index}") for index in range(1, 5)),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 40, score=100),
            _urgency("ST-2", "supply_needed", 10, score=90),
            _urgency("ST-3", "supply_needed", 10, score=80),
            _urgency("ST-4", "supply_needed", 10, score=70),
        ),
        policy_config=policy,
    )
    pickup_routes = {
        stop.route_id
        for stop in plan.route_stops
        if stop.sta_id == "ST-1" and stop.route_action_type_cd == "pickup"
    }
    assert len(pickup_routes) == 1


def test_exclusive_active_pickup_waits_for_next_recalculation() -> None:
    """이미 active 경로가 예약한 공급원은 남은 수량이 있어도 새 plan에서 제외한다."""
    policy = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
    )
    coverage = build_current_route_coverage(
        stock_anchor_dttm=_UTC_1600,
        routes=(
            _existing_route(
                "00000000-0000-0000-0000-000000000001",
                "dispatched",
                stops=(
                    _stop("ST-1", action="pickup", bike_cnt=10),
                    _stop("ST-2", action="dropoff", bike_cnt=10, visit_no=2),
                ),
                dispatched_dttm=_UTC_1600 - timedelta(minutes=5),
            ),
        ),
    )
    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 40),
            _urgency("ST-2", "supply_needed", 40),
        ),
        coverage=coverage,
        policy_config=policy,
    )
    assert plan == RebalanceRoutePlan((), ())


def test_pickup_cooldown_station_is_excluded_from_new_routes() -> None:
    """최근 회수한 공급원은 수량이 남아도 cooldown 동안 다시 배차하지 않는다."""
    policy = risk_band_policy(
        protection_horizon_hours=2,
        minimum_stock_ratio=0.2,
        uncertainty_z=0.0,
        pickup_cooldown_minutes=120,
    )
    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 20),
            _urgency("ST-2", "supply_needed", 20),
        ),
        policy_config=policy,
        pickup_cooldown_sta_ids=frozenset({"ST-1"}),
    )
    assert plan == RebalanceRoutePlan((), ())


def test_revision_is_explicit_route_identity_input() -> None:
    """같은 계산 입력도 명시 revision이 다르면 UUID만 새 identity로 고정된다."""
    arguments = {
        "stations": (_station("ST-1"), _station("ST-2")),
        "urgency": (
            _urgency("ST-1", "retrieval_needed", 1),
            _urgency("ST-2", "supply_needed", 1),
        ),
    }

    revision_zero = _plan(revision_no=0, **arguments)
    revision_one = _plan(revision_no=1, **arguments)

    assert revision_zero.route_stops[0].bike_cnt == revision_one.route_stops[0].bike_cnt
    assert revision_zero.routes[0].route_id != revision_one.routes[0].route_id
    assert revision_one.routes[0].route_id == str(
        route_uuid_v5("center_a", _UTC_1600, 1, 1)
    )


def test_route_revision_preview_reuses_exact_content_and_increments_correction() -> (
    None
):
    """현재 revision preview hash가 같으면 replay하고 다르면 정확히 하나 올린다."""
    current = PublicationStateRecord(
        publication_key="rebalance_route",
        logical_dttm=_UTC_1600,
        revision_no=7,
        manifest_uri=f"s3://fixture/publication-{'1' * 64}.json",
        artifact_set_sha256="2" * 64,
        input_fingerprint_sha256="3" * 64,
        published_row_cnt=1,
    )
    exact = SimpleNamespace(
        logical_dttm=_UTC_1600,
        revision_no=7,
        artifact_set=SimpleNamespace(sha256="2" * 64),
        input_fingerprint=SimpleNamespace(sha256="3" * 64),
        plan=SimpleNamespace(routes=(object(),)),
    )
    changed = SimpleNamespace(
        logical_dttm=_UTC_1600,
        revision_no=7,
        artifact_set=SimpleNamespace(sha256="4" * 64),
        input_fingerprint=SimpleNamespace(sha256="3" * 64),
        plan=SimpleNamespace(routes=(object(),)),
    )

    assert route_module._choose_route_revision(exact, current) == 7
    assert route_module._choose_route_revision(changed, current) == 8


def test_route_revision_preview_resets_new_anchor_and_rejects_overflow() -> None:
    """새 logical은 revision 0이고 같은 logical 최대 revision correction은 실패한다."""
    current = PublicationStateRecord(
        publication_key="rebalance_route",
        logical_dttm=_UTC_1600,
        revision_no=2_147_483_647,
        manifest_uri=f"s3://fixture/publication-{'1' * 64}.json",
        artifact_set_sha256="2" * 64,
        input_fingerprint_sha256="3" * 64,
        published_row_cnt=1,
    )
    changed = SimpleNamespace(
        logical_dttm=_UTC_1600,
        revision_no=current.revision_no,
        artifact_set=SimpleNamespace(sha256="4" * 64),
        input_fingerprint=SimpleNamespace(sha256="3" * 64),
        plan=SimpleNamespace(routes=(object(),)),
    )
    new_anchor = SimpleNamespace(
        logical_dttm=_UTC_1600 + timedelta(minutes=5),
        revision_no=0,
        artifact_set=changed.artifact_set,
        input_fingerprint=changed.input_fingerprint,
        plan=changed.plan,
    )

    assert route_module._choose_route_revision(new_anchor, current) == 0
    with pytest.raises(ContractViolation, match="INTEGER"):
        route_module._choose_route_revision(changed, current)


def test_plan_rejects_actionable_station_outside_current_active_topology() -> None:
    """stale urgency가 inactive station을 가리키면 조용히 누락하지 않고 실패한다."""
    with pytest.raises(ContractViolation, match="current active topology"):
        _plan(
            stations=(_station("ST-1", is_active=False),),
            urgency=(_urgency("ST-1", "retrieval_needed", 1),),
        )


def test_plan_validator_rejects_negative_prefix_load() -> None:
    """수동 aggregate도 초기 적재 0에서 음수가 되는 dropoff를 거부한다."""
    route = RebalanceRoute(
        route_id="00000000-0000-0000-0000-000000000001",
        dispatch_center_id="center_a",
        route_status_cd="proposed",
        proposed_dttm=_UTC_1600,
        dispatched_dttm=None,
        completed_dttm=None,
    )
    stop = RebalanceRouteStop(
        route_id=route.route_id,
        visit_no=1,
        sta_id="ST-1",
        route_action_type_cd="dropoff",
        bike_cnt=1,
    )

    with pytest.raises(ContractViolation, match="prefix"):
        RebalanceRoutePlan((route,), (stop,))


def test_fixed_schema_parquet_roundtrip_preserves_aggregate() -> None:
    """header·stop artifact가 exact target schema로 roundtrip된다."""
    plan = _plan(
        stations=(_station("ST-1"), _station("ST-2")),
        urgency=(
            _urgency("ST-1", "retrieval_needed", 7),
            _urgency("ST-2", "supply_needed", 4),
        ),
    )

    artifacts = route_plan_to_parquet(plan)
    route_table = read_parquet_bytes(artifacts.routes)
    stop_table = read_parquet_bytes(artifacts.route_stops)

    assert route_table.schema == pa.schema(
        (
            pa.field("route_id", pa.string(), nullable=False),
            pa.field("dispatch_center_id", pa.string(), nullable=False),
            pa.field("route_status_cd", pa.string(), nullable=False),
            pa.field("proposed_dttm", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("dispatched_dttm", pa.timestamp("us", tz="UTC"), nullable=True),
            pa.field("completed_dttm", pa.timestamp("us", tz="UTC"), nullable=True),
        )
    )
    assert stop_table.schema == pa.schema(
        (
            pa.field("route_id", pa.string(), nullable=False),
            pa.field("visit_no", pa.int16(), nullable=False),
            pa.field("sta_id", pa.string(), nullable=False),
            pa.field("route_action_type_cd", pa.string(), nullable=False),
            pa.field("bike_cnt", pa.int32(), nullable=False),
        )
    )
    assert (
        route_plan_from_parquet(
            artifacts.routes,
            artifacts.route_stops,
            expected_plan=plan,
        )
        == plan
    )


def test_parquet_reader_binds_uuid_anchor_and_topology_to_expected_plan() -> None:
    """구조가 유효한 다른 aggregate도 locked 재계산 결과와 다르면 거부한다."""
    expected = _plan(
        stations=(_station("ST-1"),),
        urgency=(_urgency("ST-1", "retrieval_needed", 1),),
    )
    forged_route = RebalanceRoute(
        route_id="00000000-0000-0000-0000-000000000099",
        dispatch_center_id="center_b",
        route_status_cd="proposed",
        proposed_dttm=datetime(2099, 1, 1, tzinfo=UTC),
        dispatched_dttm=None,
        completed_dttm=None,
    )
    forged_stop = RebalanceRouteStop(
        route_id=forged_route.route_id,
        visit_no=1,
        sta_id="ST-2",
        route_action_type_cd="pickup",
        bike_cnt=1,
    )
    artifacts = route_plan_to_parquet(
        RebalanceRoutePlan((forged_route,), (forged_stop,))
    )

    with pytest.raises(ContractViolation, match="locked topology"):
        route_plan_from_parquet(
            artifacts.routes,
            artifacts.route_stops,
            expected_plan=expected,
        )
