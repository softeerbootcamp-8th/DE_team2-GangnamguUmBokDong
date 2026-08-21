"""5분 정책 판단과 트럭 작업을 사건 순서대로 재생하는 시뮬레이터를 제공한다."""

from __future__ import annotations

import heapq
import math
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

from core.scoring_config import URGENCY_STOCK_HISTORY_OFFSETS_MINUTES

from gold.demand import DemandForecastRecord
from gold.rebalance_policy import (
    LEGACY_REBALANCE_POLICY,
    RebalancePolicyConfig,
)
from gold.rebalance_route import (
    DispatchCenterTopology,
    ExistingRoute,
    ExistingRouteStop,
    RebalanceRoutePlan,
    RouteUrgencyInput,
    StationRouteTopology,
    build_current_route_coverage,
    plan_rebalance_routes,
)
from gold.station_stock import StationStockRecord
from gold.urgency import (
    ActiveStation,
    StockHistoryPoint,
    UrgencyCalculationInputs,
    compute_urgency_projection,
)

from .backtest_contract import EvaluationContract
from .historical_inputs import HistoricalStation, PredictionAudit
from .rebalance_backtest import RentalTrip

ForecastProvider = Callable[
    [datetime, Mapping[int, int], Sequence[RentalTrip]],
    tuple[tuple[DemandForecastRecord, ...], PredictionAudit],
]
SEOUL = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True, slots=True)
class ScheduledStop:
    """한 트럭이 실행할 경로 stop과 계획 수량을 표현한다."""

    executed_at: datetime
    station_no: int
    station_id: str
    action: str
    planned_quantity: int
    minimum_station_stock: int
    visit_no: int


@dataclass(slots=True)
class ActiveJob:
    """배차 후 완료·센터 복귀까지 추적하는 한 트럭 작업을 표현한다."""

    route_id: str
    truck_id: int
    dispatched_at: datetime
    stops: tuple[ScheduledStop, ...]
    return_at: datetime
    truck_load: int = 0
    completed_at: datetime | None = None
    moved_bikes: int = 0
    executed_quantities: dict[int, int] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class UnfulfilledRequest:
    """재생 중 재고 0으로 충족하지 못한 관측 대여 요청을 표현한다."""

    bike_id: str
    rented_at: str
    station_no: int


@dataclass(frozen=True, slots=True)
class JobAudit:
    """배차된 route의 계획·실행·복귀 시각과 이동량을 표현한다."""

    route_id: str
    truck_id: int
    dispatched_at: str
    completed_at: str | None
    return_at: str
    planned_bikes: int
    moved_bikes: int
    stop_count: int
    stops: tuple[StopAudit, ...]


@dataclass(frozen=True, slots=True)
class StopAudit:
    """route stop별 계획·실행 시각과 실제 작업 수량을 표현한다."""

    visit_no: int
    station_no: int
    station_id: str
    action: str
    executed_at: str
    planned_quantity: int
    minimum_station_stock: int
    actual_quantity: int | None


@dataclass(frozen=True, slots=True)
class TickAudit:
    """한 정책 tick의 추론·후보·배차·활성 작업 상태를 기록한다."""

    tick: str
    proposed_routes: int
    dispatched_routes: int
    active_jobs_before: int
    idle_trucks_before: int
    prediction: PredictionAudit


@dataclass(frozen=True, slots=True)
class SimulationMetrics:
    """한 정책의 시민 서비스와 운영 자원 결과를 표현한다."""

    policy: str
    policy_configuration: Mapping[str, object]
    window_start: str
    window_end: str
    observed_requests: int
    fulfilled_requests: int
    unfulfilled_requests: int
    observed_demand_fulfillment_rate: float
    empty_station_minutes: float
    moved_bikes: int
    planned_bikes: int
    dispatched_routes: int
    completed_routes_by_cutoff: int
    trucks_still_busy_at_cutoff: int
    executed_stops: int
    vehicle_busy_minutes: float
    decision_ticks: int
    movement_budget: int | None
    movement_budget_used: int
    cold_start_stock_history_minutes: int
    unfulfilled_request_log: tuple[UnfulfilledRequest, ...]
    job_audits: tuple[JobAudit, ...]
    tick_audits: tuple[TickAudit, ...]


@dataclass(frozen=True, slots=True)
class _Event:
    """heap에서 동일 시각 우선순위를 포함해 처리할 사건을 표현한다."""

    kind: str
    payload: Any


def simulate_policy(
    *,
    policy: str,
    contract: EvaluationContract,
    center: DispatchCenterTopology,
    stations: Sequence[HistoricalStation],
    initial_stock: Mapping[int, int],
    trips: Sequence[RentalTrip],
    forecast_provider: ForecastProvider | None,
    max_stops_per_route: int,
    movement_budget: int | None,
    policy_config: RebalancePolicyConfig = LEGACY_REBALANCE_POLICY,
) -> SimulationMetrics:
    """동일 수요에서 5분 재계획·진행 작업 coverage·트럭 복귀를 재생한다."""
    if contract.approval_delay_minutes != 0:
        raise ValueError("현재 검증 시나리오는 자동 승인 지연 0분만 지원합니다.")
    if type(policy_config) is not RebalancePolicyConfig:
        raise ValueError("policy_config는 RebalancePolicyConfig여야 합니다.")
    start = datetime.combine(
        contract.target_date,
        datetime.min.time(),
        tzinfo=SEOUL,
    ) + timedelta(hours=contract.start_hour)
    end = start + timedelta(minutes=contract.evaluation_minutes)
    selected = {
        station.station_no: station
        for station in stations
        if station.station_no in initial_stock
    }
    if not selected:
        raise ValueError("초기 재고와 station master의 공통 대여소가 없습니다.")
    station_ids = frozenset(selected)
    stock = {station_no: int(initial_stock[station_no]) for station_no in selected}
    if any(quantity < 0 for quantity in stock.values()):
        raise ValueError("초기 재고는 비음수여야 합니다.")

    events: list[tuple[datetime, int, int, _Event]] = []
    sequence = 0

    def push(moment: datetime, priority: int, kind: str, payload: Any) -> None:
        """결정적인 순서로 사건을 heap에 추가한다."""
        nonlocal sequence
        heapq.heappush(events, (moment, priority, sequence, _Event(kind, payload)))
        sequence += 1

    successful_trips: list[RentalTrip] = []
    for trip in trips:
        if trip.rented_at < start or trip.rent_station_no not in station_ids:
            successful_trips.append(trip)
            if (
                start <= trip.returned_at < end
                and trip.return_station_no in station_ids
            ):
                push(trip.returned_at, 1, "citizen_return", trip)
        elif start <= trip.rented_at < end:
            push(trip.rented_at, 2, "citizen_rental", trip)

    tick = start
    while tick < end:
        push(tick, 3, "decision_tick", tick)
        tick += timedelta(minutes=contract.tick_minutes)

    truck_available = {truck_id: start for truck_id in range(contract.fleet_size)}
    active_jobs: dict[str, ActiveJob] = {}
    completed_jobs: list[ActiveJob] = []
    pickup_cooldown_until: dict[int, datetime] = {}
    stock_history: dict[datetime, dict[int, int]] = {}
    tick_audits = []
    observed_requests = 0
    fulfilled_requests = 0
    unfulfilled_request_log = []
    moved_bikes = 0
    planned_bikes = 0
    executed_stops = 0
    dispatched_routes = 0
    completed_by_cutoff = 0
    budget_used = 0
    empty_station_minutes = 0.0
    last_event_time = start

    topology = tuple(
        StationRouteTopology(
            sta_id=station.station_id,
            dispatch_center_id=center.dispatch_center_id,
            longitude=station.longitude,
            latitude=station.latitude,
            is_active=True,
        )
        for station in sorted(selected.values(), key=lambda row: row.station_id)
    )
    active_station_rows = tuple(
        ActiveStation(
            sta_id=station.station_id,
            hold_cnt=station.capacity,
            longitude=station.longitude,
            latitude=station.latitude,
            dispatch_center_id=center.dispatch_center_id,
        )
        for station in sorted(selected.values(), key=lambda row: row.station_id)
    )

    while events:
        occurred_at, _, _, event = heapq.heappop(events)
        if occurred_at >= end:
            break
        elapsed = (occurred_at - last_event_time).total_seconds() / 60.0
        if elapsed < -1e-9:
            raise RuntimeError("사건 시각이 역행했습니다.")
        empty_station_minutes += elapsed * sum(value <= 0 for value in stock.values())
        last_event_time = occurred_at

        if event.kind == "citizen_return":
            trip = event.payload
            if trip.return_station_no in stock:
                stock[trip.return_station_no] += 1
            continue
        if event.kind == "citizen_rental":
            trip = event.payload
            observed_requests += 1
            if stock[trip.rent_station_no] <= 0:
                unfulfilled_request_log.append(
                    UnfulfilledRequest(
                        bike_id=trip.bike_id,
                        rented_at=trip.rented_at.isoformat(),
                        station_no=trip.rent_station_no,
                    )
                )
                continue
            stock[trip.rent_station_no] -= 1
            fulfilled_requests += 1
            successful_trips.append(trip)
            if trip.return_station_no in stock and trip.returned_at < end:
                push(trip.returned_at, 1, "citizen_return", trip)
            continue
        if event.kind == "route_stop":
            route_id, stop = event.payload
            job = active_jobs[route_id]
            if stop.action == "pickup":
                available = max(
                    0,
                    stock[stop.station_no] - stop.minimum_station_stock,
                )
                available = min(
                    available,
                    math.floor(
                        stock[stop.station_no] * policy_config.max_pickup_stock_fraction
                    ),
                )
                actual = min(stop.planned_quantity, available)
                stock[stop.station_no] -= actual
                job.truck_load += actual
            else:
                actual = min(stop.planned_quantity, job.truck_load)
                stock[stop.station_no] += actual
                job.truck_load -= actual
                job.moved_bikes += actual
                moved_bikes += actual
            job.executed_quantities[stop.visit_no] = actual
            executed_stops += 1
            if stop.visit_no == len(job.stops):
                job.completed_at = occurred_at
                completed_by_cutoff += 1
            continue
        if event.kind == "truck_return":
            route_id = event.payload
            job = active_jobs.pop(route_id)
            if job.truck_load:
                raise RuntimeError(
                    f"완결 경로가 센터 복귀 시 자전거를 보유합니다: {route_id}"
                )
            completed_jobs.append(job)
            continue
        if event.kind != "decision_tick":
            raise RuntimeError(f"알 수 없는 사건입니다: {event.kind}")

        stock_history[occurred_at] = dict(stock)
        if forecast_provider is None:
            continue
        records, prediction_audit = forecast_provider(
            occurred_at,
            stock,
            successful_trips,
        )
        base_utc = occurred_at.astimezone(UTC)
        supported_ids = tuple(
            sorted(
                {record.sta_id for record in records},
                key=lambda value: value.encode("utf-8"),
            )
        )
        current_stock = tuple(
            StationStockRecord(
                sta_id=station.station_id,
                base_dttm=base_utc,
                parking_bike_tot_cnt=stock[station.station_no],
            )
            for station in sorted(selected.values(), key=lambda row: row.station_id)
            if station.station_id in supported_ids
        )
        history_windows = []
        # 리터럴로 두면 scoring config가 바뀔 때 조용히 어긋난다.
        for offset in URGENCY_STOCK_HISTORY_OFFSETS_MINUTES:
            history_time = occurred_at + timedelta(minutes=offset)
            snapshot = stock_history.get(history_time)
            history_windows.append(
                ()
                if snapshot is None
                else tuple(
                    StockHistoryPoint(
                        sta_id=station.station_id,
                        observed_at=history_time.astimezone(UTC),
                        parking_bike_tot_cnt=snapshot[station.station_no],
                    )
                    for station in sorted(
                        selected.values(), key=lambda row: row.station_id
                    )
                    if station.station_id in supported_ids
                )
            )
        urgency = compute_urgency_projection(
            UrgencyCalculationInputs(
                active_stations=tuple(
                    row for row in active_station_rows if row.sta_id in supported_ids
                ),
                history_offsets_minutes=URGENCY_STOCK_HISTORY_OFFSETS_MINUTES,
                history_windows=tuple(history_windows),
                current_stock=current_stock,
                demand=records,
                base_dttm=base_utc,
            ),
            policy_config=policy_config,
        )
        active_routes = tuple(
            ExistingRoute(
                route_id=job.route_id,
                route_status_cd="dispatched",
                dispatched_dttm=job.dispatched_at.astimezone(UTC),
                completed_dttm=None,
                stops=tuple(
                    ExistingRouteStop(
                        visit_no=stop.visit_no,
                        sta_id=stop.station_id,
                        action=stop.action,
                        bike_cnt=stop.planned_quantity,
                    )
                    for stop in job.stops
                ),
            )
            for job in active_jobs.values()
            if job.completed_at is None
        )
        coverage = build_current_route_coverage(
            stock_anchor_dttm=base_utc,
            routes=active_routes,
        )
        plan = plan_rebalance_routes(
            logical_dttm=base_utc,
            revision_no=0,
            dispatch_centers=(center,),
            stations=topology,
            urgency=tuple(
                RouteUrgencyInput(
                    sta_id=row.sta_id,
                    urgency_score=row.urgency_score,
                    action_type=row.rebalance_need_type_cd,
                    bike_qty=row.bike_qty,
                )
                for row in urgency.records
            ),
            route_coverage=coverage,
            max_stops_per_route=max_stops_per_route,
            policy_config=policy_config,
            pickup_cooldown_sta_ids=frozenset(
                selected[station_no].station_id
                for station_no, until in pickup_cooldown_until.items()
                if until > occurred_at
            ),
        )
        idle = sorted(
            truck_id
            for truck_id, available_at in truck_available.items()
            if available_at <= occurred_at
        )
        active_before = len(active_jobs)
        idle_before = len(idle)
        dispatched_now = 0
        for route in plan.routes:
            if not idle:
                break
            truck_id = idle[0]
            remaining_budget = (
                None if movement_budget is None else movement_budget - budget_used
            )
            if remaining_budget is not None and remaining_budget <= 0:
                break
            job = _schedule_job(
                plan=plan,
                route_id=route.route_id,
                truck_id=truck_id,
                dispatched_at=occurred_at,
                center=center,
                stations=selected,
                speed_kmh=contract.speed_kmh,
                service_minutes=contract.service_minutes_per_stop,
                transfer_limit=remaining_budget,
                policy_config=policy_config,
            )
            if job is None:
                continue
            if job.return_at > end:
                continue
            transfer = sum(
                stop.planned_quantity for stop in job.stops if stop.action == "dropoff"
            )
            active_jobs[job.route_id] = job
            if policy_config.pickup_cooldown_minutes > 0:
                cooldown_until = occurred_at + timedelta(
                    minutes=policy_config.pickup_cooldown_minutes
                )
                for stop in job.stops:
                    if stop.action == "pickup":
                        pickup_cooldown_until[stop.station_no] = cooldown_until
            idle.pop(0)
            truck_available[truck_id] = job.return_at
            for stop in job.stops:
                push(stop.executed_at, 0, "route_stop", (job.route_id, stop))
            push(job.return_at, 0, "truck_return", job.route_id)
            planned_bikes += transfer
            budget_used += transfer
            dispatched_routes += 1
            dispatched_now += 1
        tick_audits.append(
            TickAudit(
                tick=occurred_at.isoformat(),
                proposed_routes=len(plan.routes),
                dispatched_routes=dispatched_now,
                active_jobs_before=active_before,
                idle_trucks_before=idle_before,
                prediction=prediction_audit,
            )
        )

    empty_station_minutes += (
        (end - last_event_time).total_seconds()
        / 60.0
        * sum(value <= 0 for value in stock.values())
    )
    all_jobs = [*completed_jobs, *active_jobs.values()]
    busy_minutes = sum(
        max(
            0.0,
            (min(job.return_at, end) - job.dispatched_at).total_seconds() / 60.0,
        )
        for job in all_jobs
    )
    still_busy = sum(job.return_at > end for job in all_jobs)
    job_audits = tuple(
        JobAudit(
            route_id=job.route_id,
            truck_id=job.truck_id,
            dispatched_at=job.dispatched_at.isoformat(),
            completed_at=(
                None if job.completed_at is None else job.completed_at.isoformat()
            ),
            return_at=job.return_at.isoformat(),
            planned_bikes=sum(
                stop.planned_quantity for stop in job.stops if stop.action == "dropoff"
            ),
            moved_bikes=job.moved_bikes,
            stop_count=len(job.stops),
            stops=tuple(
                StopAudit(
                    visit_no=stop.visit_no,
                    station_no=stop.station_no,
                    station_id=stop.station_id,
                    action=stop.action,
                    executed_at=stop.executed_at.isoformat(),
                    planned_quantity=stop.planned_quantity,
                    minimum_station_stock=stop.minimum_station_stock,
                    actual_quantity=job.executed_quantities.get(stop.visit_no),
                )
                for stop in job.stops
            ),
        )
        for job in sorted(all_jobs, key=lambda row: (row.dispatched_at, row.route_id))
    )
    return SimulationMetrics(
        policy=policy,
        policy_configuration=policy_config.audit_document(),
        window_start=start.isoformat(),
        window_end=end.isoformat(),
        observed_requests=observed_requests,
        fulfilled_requests=fulfilled_requests,
        unfulfilled_requests=observed_requests - fulfilled_requests,
        observed_demand_fulfillment_rate=(
            fulfilled_requests / observed_requests if observed_requests else 1.0
        ),
        empty_station_minutes=round(empty_station_minutes, 3),
        moved_bikes=moved_bikes,
        planned_bikes=planned_bikes,
        dispatched_routes=dispatched_routes,
        completed_routes_by_cutoff=completed_by_cutoff,
        trucks_still_busy_at_cutoff=still_busy,
        executed_stops=executed_stops,
        vehicle_busy_minutes=round(busy_minutes, 3),
        decision_ticks=len(tick_audits),
        movement_budget=movement_budget,
        movement_budget_used=budget_used,
        cold_start_stock_history_minutes=25,
        unfulfilled_request_log=tuple(unfulfilled_request_log),
        job_audits=job_audits,
        tick_audits=tuple(tick_audits),
    )


def simulate_no_rebalance(
    *,
    contract: EvaluationContract,
    center: DispatchCenterTopology,
    stations: Sequence[HistoricalStation],
    initial_stock: Mapping[int, int],
    trips: Sequence[RentalTrip],
) -> SimulationMetrics:
    """동일 사건 엔진에서 재배치 판단만 제거한 기준선을 계산한다."""
    return simulate_policy(
        policy="no_rebalance",
        contract=contract,
        center=center,
        stations=stations,
        initial_stock=initial_stock,
        trips=trips,
        forecast_provider=None,
        max_stops_per_route=8,
        movement_budget=0,
        policy_config=LEGACY_REBALANCE_POLICY,
    )


def _schedule_job(
    *,
    plan: RebalanceRoutePlan,
    route_id: str,
    truck_id: int,
    dispatched_at: datetime,
    center: DispatchCenterTopology,
    stations: Mapping[int, HistoricalStation],
    speed_kmh: float,
    service_minutes: float,
    transfer_limit: int | None,
    policy_config: RebalancePolicyConfig,
) -> ActiveJob | None:
    """planner 경로를 예산에 맞춰 완결 수량으로 자르고 센터 복귀까지 예약한다."""
    by_id = {station.station_id: station for station in stations.values()}
    raw_stops = sorted(
        (stop for stop in plan.route_stops if stop.route_id == route_id),
        key=lambda row: row.visit_no,
    )
    planned_transfer = sum(
        stop.bike_cnt for stop in raw_stops if stop.route_action_type_cd == "dropoff"
    )
    transfer = (
        planned_transfer
        if transfer_limit is None
        else min(planned_transfer, transfer_limit)
    )
    if transfer <= 0:
        return None
    remaining_by_action = {"pickup": transfer, "dropoff": transfer}
    chosen = []
    for stop in raw_stops:
        action = stop.route_action_type_cd
        quantity = min(stop.bike_cnt, remaining_by_action[action])
        if quantity <= 0:
            continue
        chosen.append((stop, quantity))
        remaining_by_action[action] -= quantity
    if any(remaining_by_action.values()):
        raise RuntimeError("예산 절단 뒤 pickup/dropoff 완결 수량이 맞지 않습니다.")
    current_latitude = center.latitude
    current_longitude = center.longitude
    elapsed = 0.0
    scheduled = []
    for visit_no, (stop, quantity) in enumerate(chosen, start=1):
        station = by_id[stop.sta_id]
        elapsed += (
            _haversine_km(
                current_latitude,
                current_longitude,
                station.latitude,
                station.longitude,
            )
            / speed_kmh
            * 60.0
            + service_minutes
        )
        scheduled.append(
            ScheduledStop(
                executed_at=dispatched_at + timedelta(minutes=elapsed),
                station_no=station.station_no,
                station_id=station.station_id,
                action=stop.route_action_type_cd,
                planned_quantity=quantity,
                minimum_station_stock=(
                    math.ceil(station.capacity * policy_config.execution_reserve_ratio)
                    if stop.route_action_type_cd == "pickup"
                    else 0
                ),
                visit_no=visit_no,
            )
        )
        current_latitude = station.latitude
        current_longitude = station.longitude
    elapsed += (
        _haversine_km(
            current_latitude,
            current_longitude,
            center.latitude,
            center.longitude,
        )
        / speed_kmh
        * 60.0
    )
    return ActiveJob(
        route_id=route_id,
        truck_id=truck_id,
        dispatched_at=dispatched_at,
        stops=tuple(scheduled),
        return_at=dispatched_at + timedelta(minutes=elapsed),
    )


def _haversine_km(
    latitude_a: float,
    longitude_a: float,
    latitude_b: float,
    longitude_b: float,
) -> float:
    """두 WGS84 좌표 사이 구면 직선거리를 km로 계산한다."""
    radius_km = 6371.0
    phi_a = math.radians(latitude_a)
    phi_b = math.radians(latitude_b)
    delta_phi = math.radians(latitude_b - latitude_a)
    delta_lambda = math.radians(longitude_b - longitude_a)
    value = (
        math.sin(delta_phi / 2.0) ** 2
        + math.cos(phi_a) * math.cos(phi_b) * math.sin(delta_lambda / 2.0) ** 2
    )
    return 2.0 * radius_km * math.asin(math.sqrt(value))
