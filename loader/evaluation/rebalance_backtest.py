"""실제 대여 이력과 시간대별 재고로 재배치 정책을 좁게 백테스트한다."""

from __future__ import annotations

import argparse
import csv
import heapq
import json
import math
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import asdict, dataclass
from datetime import UTC, date, datetime, timedelta
from itertools import pairwise
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import yaml
from core.gold_publication import build_route_coverage
from gold.rebalance_route import (
    DispatchCenterTopology,
    RebalanceRoutePlan,
    RouteUrgencyInput,
    StationRouteTopology,
    plan_rebalance_routes,
)

EVIDENCE_GRADE = "exploratory_oracle"
DEFAULT_SPEED_KMH = 20.0
DEFAULT_SERVICE_MINUTES = 3.0
SEOUL = ZoneInfo("Asia/Seoul")


@dataclass(frozen=True, slots=True)
class StationMetadata:
    """백테스트에 필요한 대여소 식별자와 좌표를 표현한다."""

    station_no: int
    station_id: str
    station_name: str
    latitude: float
    longitude: float
    center_id: str


@dataclass(frozen=True, slots=True)
class RentalTrip:
    """시민의 실제 대여와 반납 한 건을 표현한다."""

    bike_id: str
    rented_at: datetime
    rent_station_no: int
    returned_at: datetime
    return_station_no: int


@dataclass(frozen=True, slots=True)
class StockObservation:
    """정시 기준 대여소의 실측 재고를 표현한다."""

    observed_at: datetime
    station_no: int
    quantity: int


@dataclass(frozen=True, slots=True)
class RouteAction:
    """시뮬레이션 시각에 실행할 트럭의 회수 또는 배치를 표현한다."""

    executed_at: datetime
    route_id: str
    station_no: int
    action: str
    quantity: int


@dataclass(frozen=True, slots=True)
class ReplayMetrics:
    """한 정책을 재생한 서비스·운영 결과를 표현한다."""

    policy: str
    observed_requests: int
    fulfilled_requests: int
    unfulfilled_requests: int
    fulfillment_rate: float
    empty_station_hours: int
    moved_bikes: int
    planned_bikes: int
    route_count: int
    route_stop_count: int
    estimated_vehicle_minutes: float


@dataclass(frozen=True, slots=True)
class ExistingOperationEstimate:
    """시간대별 재고 잔차로 추정한 기존 운영자 이동량을 표현한다."""

    balanced_moved_bikes: int
    added_bikes: int
    removed_bikes: int
    external_imbalance_bikes: int
    station_hour_residual_mae: float


@dataclass(frozen=True, slots=True)
class BacktestResult:
    """실험 조건·비교 지표·근거 한계를 포함한 결과를 표현한다."""

    evidence_grade: str
    target_date: str
    center_id: str
    center_name: str
    window_start: str
    window_end: str
    station_count: int
    trip_count: int
    relocation_candidate_count: int
    existing_operation: ExistingOperationEstimate
    existing_empty_station_hours: int
    no_rebalance: ReplayMetrics
    current_route_v2: ReplayMetrics
    empty_station_hour_change_vs_existing_pct: float | None
    assumptions: tuple[str, ...]


def load_centers(seed_path: Path) -> tuple[tuple[DispatchCenterTopology, str], ...]:
    """버전 고정 dispatch center seed에서 활성 센터를 읽는다."""
    payload = yaml.safe_load(seed_path.read_text(encoding="utf-8"))
    centers = []
    for row in payload["centers"]:
        if not row["is_active"]:
            continue
        topology = DispatchCenterTopology(
            dispatch_center_id=row["dispatch_center_id"],
            longitude=float(row["longitude"]),
            latitude=float(row["latitude"]),
            is_active=True,
        )
        centers.append((topology, row["dispatch_center_nm"]))
    return tuple(centers)


def load_station_coordinates(path: Path) -> dict[int, tuple[str, float, float]]:
    """대시보드 정적 자산에서 공공 대여소 번호별 이름과 좌표를 읽는다."""
    rows = json.loads(path.read_text(encoding="utf-8"))
    return {
        int(row["sta_id"]): (
            str(row["sta_nm"]),
            float(row["lat"]),
            float(row["lon"]),
        )
        for row in rows
        if row.get("sta_id") is not None
        and row.get("lat") is not None
        and row.get("lon") is not None
    }


def read_rental_trips(path: Path, target_date: date) -> tuple[RentalTrip, ...]:
    """월 대여 이력 CSV에서 목표일과 겹치는 정상 이용 건을 스트리밍해 읽는다."""
    day_start = datetime.combine(target_date, datetime.min.time(), tzinfo=SEOUL)
    day_end = day_start + timedelta(days=1)
    trips: list[RentalTrip] = []
    with path.open("r", encoding="cp949", newline="") as stream:
        for row in csv.DictReader(stream):
            try:
                rented_at = datetime.strptime(
                    row["대여일시"], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=SEOUL)
                returned_at = datetime.strptime(
                    row["반납일시"], "%Y-%m-%d %H:%M:%S"
                ).replace(tzinfo=SEOUL)
                rent_station_no = _station_no(row["대여 대여소번호"])
                return_station_no = _station_no(row["반납대여소번호"])
            except (KeyError, TypeError, ValueError):
                continue
            if returned_at < day_start or rented_at >= day_end:
                continue
            if returned_at < rented_at:
                continue
            trips.append(
                RentalTrip(
                    bike_id=row.get("자전거번호", "").strip(),
                    rented_at=rented_at,
                    rent_station_no=rent_station_no,
                    returned_at=returned_at,
                    return_station_no=return_station_no,
                )
            )
    return tuple(sorted(trips, key=lambda trip: (trip.rented_at, trip.bike_id)))


def station_id_candidates(
    path: Path,
    target_date: date,
) -> dict[int, str]:
    """목표일 이력의 공공 번호와 내부 ST 식별자 사이 최빈 대응을 만든다."""
    counts: dict[int, Counter[str]] = defaultdict(Counter)
    day_text = target_date.isoformat()
    with path.open("r", encoding="cp949", newline="") as stream:
        for row in csv.DictReader(stream):
            if not (
                row.get("대여일시", "").startswith(day_text)
                or row.get("반납일시", "").startswith(day_text)
            ):
                continue
            for number_key, id_key in (
                ("대여 대여소번호", "대여대여소ID"),
                ("반납대여소번호", "반납대여소ID"),
            ):
                try:
                    station_no = _station_no(row[number_key])
                except (KeyError, TypeError, ValueError):
                    continue
                station_id = row.get(id_key, "").strip()
                if station_id.startswith("ST-"):
                    counts[station_no][station_id] += 1
    return {
        station_no: candidates.most_common(1)[0][0]
        for station_no, candidates in counts.items()
        if candidates
    }


def read_stock_observations(
    path: Path,
    target_date: date,
) -> tuple[StockObservation, ...]:
    """시간대별 재고 CSV에서 목표일의 유효한 관측을 읽는다."""
    observations = []
    with path.open("r", encoding="cp949", newline="") as stream:
        for row in csv.DictReader(stream):
            if row.get("일시") != target_date.isoformat():
                continue
            try:
                observed_at = datetime.combine(
                    target_date,
                    datetime.min.time(),
                    tzinfo=SEOUL,
                ) + timedelta(hours=int(row["시간대"]))
                observations.append(
                    StockObservation(
                        observed_at=observed_at,
                        station_no=_station_no(row["대여소번호"]),
                        quantity=max(0, int(float(row["거치대수량"]))),
                    )
                )
            except (KeyError, TypeError, ValueError):
                continue
    return tuple(
        sorted(observations, key=lambda row: (row.observed_at, row.station_no))
    )


def build_station_metadata(
    *,
    coordinates: dict[int, tuple[str, float, float]],
    station_ids: dict[int, str],
    centers: tuple[tuple[DispatchCenterTopology, str], ...],
) -> dict[int, StationMetadata]:
    """좌표가 있는 대여소를 최근접 dispatch center에 결정적으로 배정한다.

    목표일 이용이 없어 내부 ST ID를 관측하지 못한 대여소도 품절 지표에서 빠지지
    않도록 평가 전용 synthetic ID를 부여한다. planner의 순서·거리 계산에는 ID의
    의미가 개입하지 않으며, 관측된 실제 ID와 충돌하지 않는 범위만 사용한다.
    """
    metadata = {}
    used_station_ids = set(station_ids.values())
    for station_no, coordinate in coordinates.items():
        station_id = station_ids.get(station_no)
        if station_id is None:
            synthetic_number = 10_000_000 + station_no
            station_id = f"ST-{synthetic_number}"
            while station_id in used_station_ids:
                synthetic_number += 10_000_000
                station_id = f"ST-{synthetic_number}"
        used_station_ids.add(station_id)
        name, latitude, longitude = coordinate
        center = min(
            (row[0] for row in centers),
            key=lambda row: (
                _haversine_km(latitude, longitude, row.latitude, row.longitude),
                row.dispatch_center_id,
            ),
        )
        metadata[station_no] = StationMetadata(
            station_no=station_no,
            station_id=station_id,
            station_name=name,
            latitude=latitude,
            longitude=longitude,
            center_id=center.dispatch_center_id,
        )
    return metadata


def estimate_existing_operations(
    *,
    observations: Sequence[StockObservation],
    trips: Sequence[RentalTrip],
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
) -> ExistingOperationEstimate:
    """실측 재고 변화에서 시민 흐름을 제거한 시간대별 운영 잔차를 추정한다."""
    by_time = _observations_by_time(observations, station_nos)
    checkpoints = sorted(time for time in by_time if window_start <= time <= window_end)
    added = 0
    removed = 0
    absolute_residual = 0
    residual_cells = 0
    for start, end in pairwise(checkpoints):
        rentals, returns = _actual_flows(trips, station_nos, start, end)
        for station_no in station_nos:
            if station_no not in by_time[start] or station_no not in by_time[end]:
                continue
            expected = (
                by_time[start][station_no] - rentals[station_no] + returns[station_no]
            )
            residual = by_time[end][station_no] - expected
            added += max(0, residual)
            removed += max(0, -residual)
            absolute_residual += abs(residual)
            residual_cells += 1
    return ExistingOperationEstimate(
        balanced_moved_bikes=min(added, removed),
        added_bikes=added,
        removed_bikes=removed,
        external_imbalance_bikes=abs(added - removed),
        station_hour_residual_mae=(
            absolute_residual / residual_cells if residual_cells else 0.0
        ),
    )


def build_oracle_urgency(
    *,
    trips: Sequence[RentalTrip],
    initial_stock: dict[int, int],
    stations: dict[int, StationMetadata],
    window_start: datetime,
    window_end: datetime,
    movement_budget: int,
) -> tuple[RouteUrgencyInput, ...]:
    """실제 미래 수요로 route-v2의 탐색용 상한 urgency를 구성한다.

    이 함수는 미래 정보를 사용하므로 운영 성능 근거가 아니라 경로 구성의 잠재
    효과와 제약을 확인하는 oracle 실험에만 사용해야 한다.
    """
    station_nos = frozenset(stations)
    changes: dict[int, list[tuple[datetime, int]]] = defaultdict(list)
    for trip in trips:
        if (
            window_start <= trip.rented_at < window_end
            and trip.rent_station_no in station_nos
        ):
            changes[trip.rent_station_no].append((trip.rented_at, -1))
        if (
            window_start <= trip.returned_at < window_end
            and trip.return_station_no in station_nos
        ):
            changes[trip.return_station_no].append((trip.returned_at, 1))

    dropoffs = []
    pickups = []
    horizon_minutes = max(1.0, (window_end - window_start).total_seconds() / 60.0)
    for station_no, station in stations.items():
        running = initial_stock.get(station_no, 0)
        minimum = running
        first_shortage_at: datetime | None = None
        for changed_at, delta in sorted(changes[station_no]):
            running += delta
            minimum = min(minimum, running)
            if running < 0 and first_shortage_at is None:
                first_shortage_at = changed_at
        deficit = max(0, -minimum)
        safe_surplus = max(0, minimum - 1)
        if deficit:
            shortage_minutes = (
                (first_shortage_at - window_start).total_seconds() / 60.0
                if first_shortage_at is not None
                else horizon_minutes
            )
            urgency = min(
                100.0, 60.0 + 40.0 * (1.0 - shortage_minutes / horizon_minutes)
            )
            dropoffs.append((urgency, deficit, station))
        elif safe_surplus:
            pickups.append((min(100.0, 40.0 + safe_surplus), safe_surplus, station))

    transferable = min(
        movement_budget,
        sum(row[1] for row in pickups),
        sum(row[1] for row in dropoffs),
    )
    pickup_allocations = _allocate_quantities(pickups, transferable)
    dropoff_allocations = _allocate_quantities(dropoffs, transferable)
    rows = [
        RouteUrgencyInput(
            sta_id=station.station_id,
            urgency_score=urgency,
            action_type="retrieval_needed",
            bike_qty=quantity,
        )
        for urgency, quantity, station in pickup_allocations
    ]
    rows.extend(
        RouteUrgencyInput(
            sta_id=station.station_id,
            urgency_score=urgency,
            action_type="supply_needed",
            bike_qty=quantity,
        )
        for urgency, quantity, station in dropoff_allocations
    )
    return tuple(sorted(rows, key=lambda row: row.sta_id.encode("utf-8")))


def build_current_route_plan(
    *,
    logical_dttm: datetime,
    center: DispatchCenterTopology,
    stations: dict[int, StationMetadata],
    urgency: tuple[RouteUrgencyInput, ...],
) -> RebalanceRoutePlan:
    """운영 코드의 현재 route-v2 planner로 한 센터의 작업을 계산한다."""
    topology = tuple(
        StationRouteTopology(
            sta_id=station.station_id,
            dispatch_center_id=station.center_id,
            longitude=station.longitude,
            latitude=station.latitude,
            is_active=True,
        )
        for station in sorted(stations.values(), key=lambda row: row.station_id)
    )
    return plan_rebalance_routes(
        logical_dttm=logical_dttm.astimezone(UTC),
        revision_no=0,
        dispatch_centers=(center,),
        stations=topology,
        urgency=urgency,
        route_coverage=build_route_coverage(
            stock_anchor_dttm=logical_dttm.astimezone(UTC),
            routes=(),
        ),
    )


def schedule_route_actions(
    *,
    plan: RebalanceRoutePlan,
    center: DispatchCenterTopology,
    stations: dict[int, StationMetadata],
    window_start: datetime,
    speed_kmh: float = DEFAULT_SPEED_KMH,
    service_minutes: float = DEFAULT_SERVICE_MINUTES,
) -> tuple[RouteAction, ...]:
    """각 route를 동시 출발 트럭으로 보고 이동·작업시간에 따라 stop을 예약한다."""
    station_by_id = {station.station_id: station for station in stations.values()}
    stops_by_route: dict[str, list[Any]] = defaultdict(list)
    for stop in plan.route_stops:
        stops_by_route[stop.route_id].append(stop)
    actions = []
    for route in plan.routes:
        current_latitude = center.latitude
        current_longitude = center.longitude
        elapsed_minutes = 0.0
        for stop in sorted(
            stops_by_route[route.route_id], key=lambda row: row.visit_no
        ):
            station = station_by_id[stop.sta_id]
            distance = _haversine_km(
                current_latitude,
                current_longitude,
                station.latitude,
                station.longitude,
            )
            elapsed_minutes += distance / speed_kmh * 60.0 + service_minutes
            actions.append(
                RouteAction(
                    executed_at=window_start + timedelta(minutes=elapsed_minutes),
                    route_id=route.route_id,
                    station_no=station.station_no,
                    action=stop.route_action_type_cd,
                    quantity=stop.bike_cnt,
                )
            )
            current_latitude = station.latitude
            current_longitude = station.longitude
    return tuple(sorted(actions, key=lambda row: (row.executed_at, row.route_id)))


def replay_policy(
    *,
    policy: str,
    trips: Sequence[RentalTrip],
    initial_stock: dict[int, int],
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
    checkpoints: Sequence[datetime],
    route_actions: Sequence[RouteAction] = (),
) -> ReplayMetrics:
    """동일 초기 재고와 시민 수요에 선택 정책의 작업을 시간순으로 재생한다."""
    stock = {station_no: initial_stock.get(station_no, 0) for station_no in station_nos}
    truck_loads: dict[str, int] = defaultdict(int)
    events: list[tuple[datetime, int, int, str, Any]] = []
    sequence = 0
    observed_requests = 0
    fulfilled_requests = 0
    moved_bikes = 0
    planned_bikes = sum(
        action.quantity for action in route_actions if action.action == "dropoff"
    )
    for action in route_actions:
        heapq.heappush(events, (action.executed_at, 0, sequence, "route", action))
        sequence += 1
    for trip in trips:
        if trip.rented_at < window_start <= trip.returned_at < window_end:
            heapq.heappush(
                events, (trip.returned_at, 1, sequence, "external_return", trip)
            )
            sequence += 1
        elif window_start <= trip.rented_at < window_end:
            heapq.heappush(events, (trip.rented_at, 2, sequence, "rental", trip))
            sequence += 1

    checkpoint_index = 0
    empty_station_hours = 0
    checkpoint_list = list(checkpoints)

    def record_until(moment: datetime) -> None:
        """현재 사건 직전까지 지난 정시 체크포인트의 품절 수를 기록한다."""
        nonlocal checkpoint_index, empty_station_hours
        while (
            checkpoint_index < len(checkpoint_list)
            and checkpoint_list[checkpoint_index] <= moment
        ):
            empty_station_hours += sum(quantity <= 0 for quantity in stock.values())
            checkpoint_index += 1

    while events:
        occurred_at, _, _, event_type, payload = heapq.heappop(events)
        if occurred_at >= window_end:
            break
        record_until(occurred_at)
        if event_type == "route":
            action = payload
            if action.action == "pickup":
                actual = min(action.quantity, stock.get(action.station_no, 0))
                stock[action.station_no] -= actual
                truck_loads[action.route_id] += actual
            else:
                actual = min(action.quantity, truck_loads[action.route_id])
                stock[action.station_no] = stock.get(action.station_no, 0) + actual
                truck_loads[action.route_id] -= actual
                moved_bikes += actual
        elif event_type == "external_return":
            trip = payload
            if trip.return_station_no in station_nos:
                stock[trip.return_station_no] += 1
        else:
            trip = payload
            if trip.rent_station_no not in station_nos:
                if (
                    trip.return_station_no in station_nos
                    and trip.returned_at < window_end
                ):
                    heapq.heappush(
                        events, (trip.returned_at, 1, sequence, "external_return", trip)
                    )
                    sequence += 1
                continue
            observed_requests += 1
            if stock[trip.rent_station_no] <= 0:
                continue
            stock[trip.rent_station_no] -= 1
            fulfilled_requests += 1
            if trip.return_station_no in station_nos and trip.returned_at < window_end:
                heapq.heappush(
                    events, (trip.returned_at, 1, sequence, "external_return", trip)
                )
                sequence += 1
    record_until(window_end)
    route_ids = {action.route_id for action in route_actions}
    duration_by_route: dict[str, float] = {}
    for action in route_actions:
        duration_by_route[action.route_id] = max(
            duration_by_route.get(action.route_id, 0.0),
            (action.executed_at - window_start).total_seconds() / 60.0,
        )
    return ReplayMetrics(
        policy=policy,
        observed_requests=observed_requests,
        fulfilled_requests=fulfilled_requests,
        unfulfilled_requests=observed_requests - fulfilled_requests,
        fulfillment_rate=(
            fulfilled_requests / observed_requests if observed_requests else 1.0
        ),
        empty_station_hours=empty_station_hours,
        moved_bikes=moved_bikes,
        planned_bikes=planned_bikes,
        route_count=len(route_ids),
        route_stop_count=len(route_actions),
        estimated_vehicle_minutes=sum(duration_by_route.values()),
    )


def run_backtest(
    *,
    target_date: date,
    center_id: str,
    start_hour: int,
    duration_hours: int,
    rental_csv: Path,
    stock_csv: Path,
    station_json: Path,
    center_seed: Path,
) -> BacktestResult:
    """하루·한 센터의 탐색용 route-v2 백테스트 전체 절차를 실행한다."""
    centers = load_centers(center_seed)
    center_rows = [row for row in centers if row[0].dispatch_center_id == center_id]
    if len(center_rows) != 1:
        raise ValueError(f"활성 center_id를 찾을 수 없습니다: {center_id}")
    center, center_name = center_rows[0]
    window_start = datetime.combine(
        target_date,
        datetime.min.time(),
        tzinfo=SEOUL,
    ) + timedelta(hours=start_hour)
    window_end = window_start + timedelta(hours=duration_hours)
    trips = read_rental_trips(rental_csv, target_date)
    station_ids = station_id_candidates(rental_csv, target_date)
    metadata = build_station_metadata(
        coordinates=load_station_coordinates(station_json),
        station_ids=station_ids,
        centers=centers,
    )
    selected = {
        station_no: station
        for station_no, station in metadata.items()
        if station.center_id == center_id
    }
    observations = read_stock_observations(stock_csv, target_date)
    by_time = _observations_by_time(observations, frozenset(selected))
    observation_times = tuple(
        window_start + timedelta(hours=offset) for offset in range(duration_hours + 1)
    )
    selected = {
        station_no: station
        for station_no, station in selected.items()
        if all(station_no in by_time.get(moment, {}) for moment in observation_times)
    }
    station_nos = frozenset(selected)
    if not station_nos:
        raise ValueError("목표 센터에 좌표와 구간 전체 재고가 있는 대여소가 없습니다.")
    initial_stock = {
        station_no: by_time[window_start][station_no] for station_no in station_nos
    }
    checkpoints = observation_times[:-1]
    existing = estimate_existing_operations(
        observations=observations,
        trips=trips,
        station_nos=station_nos,
        window_start=window_start,
        window_end=window_end,
    )
    urgency = build_oracle_urgency(
        trips=trips,
        initial_stock=initial_stock,
        stations=selected,
        window_start=window_start,
        window_end=window_end,
        movement_budget=existing.balanced_moved_bikes,
    )
    plan = build_current_route_plan(
        logical_dttm=window_start,
        center=center,
        stations=selected,
        urgency=urgency,
    )
    actions = schedule_route_actions(
        plan=plan,
        center=center,
        stations=selected,
        window_start=window_start,
    )
    no_rebalance = replay_policy(
        policy="no_rebalance",
        trips=trips,
        initial_stock=initial_stock,
        station_nos=station_nos,
        window_start=window_start,
        window_end=window_end,
        checkpoints=checkpoints,
    )
    current = replay_policy(
        policy="current_route_v2_oracle_need",
        trips=trips,
        initial_stock=initial_stock,
        station_nos=station_nos,
        window_start=window_start,
        window_end=window_end,
        checkpoints=checkpoints,
        route_actions=actions,
    )
    actual_empty = _actual_empty_station_hours(by_time, station_nos, checkpoints)
    change = (
        (actual_empty - current.empty_station_hours) / actual_empty * 100.0
        if actual_empty
        else None
    )
    relocation_candidates = detect_relocation_candidates(
        trips,
        station_nos=station_nos,
        window_start=window_start,
        window_end=window_end,
    )
    return BacktestResult(
        evidence_grade=EVIDENCE_GRADE,
        target_date=target_date.isoformat(),
        center_id=center_id,
        center_name=center_name,
        window_start=window_start.isoformat(),
        window_end=window_end.isoformat(),
        station_count=len(station_nos),
        trip_count=sum(
            window_start <= trip.rented_at < window_end
            and trip.rent_station_no in station_nos
            for trip in trips
        ),
        relocation_candidate_count=relocation_candidates,
        existing_operation=existing,
        existing_empty_station_hours=actual_empty,
        no_rebalance=no_rebalance,
        current_route_v2=current,
        empty_station_hour_change_vs_existing_pct=change,
        assumptions=(
            "기존 운영 이동량은 시간대별 실측 재고 변화에서 시민 대여·반납을 제거한 순잔차다.",
            "운영자 이동 시각·트럭 경로는 공개 데이터로 식별할 수 없다.",
            "현재 정책의 필요량은 평가 구간의 미래 실제 수요를 사용한 oracle 상한이며 모델 성능이 아니다.",
            "route-v2 트럭은 동시에 출발하고 직선거리 20km/h, 대여소당 3분 작업으로 근사한다.",
            "실패해 관측되지 않은 잠재 대여 요청은 평가할 수 없다.",
        ),
    )


def detect_relocation_candidates(
    trips: Sequence[RentalTrip],
    *,
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
    maximum_gap: timedelta = timedelta(hours=48),
) -> int:
    """동일 자전거의 이전 반납과 다음 대여 위치가 다른 후보 수를 센다."""
    by_bike: dict[str, list[RentalTrip]] = defaultdict(list)
    for trip in trips:
        if trip.bike_id:
            by_bike[trip.bike_id].append(trip)
    count = 0
    for bike_trips in by_bike.values():
        ordered = sorted(bike_trips, key=lambda row: row.rented_at)
        for previous, following in pairwise(ordered):
            gap = following.rented_at - previous.returned_at
            if not timedelta(0) <= gap <= maximum_gap:
                continue
            if previous.return_station_no == following.rent_station_no:
                continue
            if not window_start <= following.rented_at < window_end:
                continue
            if (
                previous.return_station_no in station_nos
                or following.rent_station_no in station_nos
            ):
                count += 1
    return count


def result_markdown(result: BacktestResult) -> str:
    """백테스트 결과와 주장 한계를 사람이 검토할 Markdown으로 만든다."""
    change = (
        "계산 불가"
        if result.empty_station_hour_change_vs_existing_pct is None
        else f"{result.empty_station_hour_change_vs_existing_pct:.2f}%"
    )
    lines = [
        "# 재배치 정책 소규모 백테스트",
        "",
        f"- 근거 등급: `{result.evidence_grade}`",
        (
            f"- 범위: {result.target_date} {result.center_name} "
            f"({result.window_start[11:16]}~{result.window_end[11:16]})"
        ),
        f"- 대여소: {result.station_count}곳",
        f"- 관측 대여: {result.trip_count}건",
        "",
        "## 결과",
        "",
        "| 정책 | 품절 대여소-시간 | 관측 대여 미충족 | 이동 대수 | 경로/정차 | 추정 차량-분 |",
        "|---|---:|---:|---:|---:|---:|",
        (
            f"| 실제 기존 운영 | {result.existing_empty_station_hours} | 측정 불가 | "
            f"{result.existing_operation.balanced_moved_bikes} 추정 | 측정 불가 | 측정 불가 |"
        ),
        (
            f"| 재배치 없음 | {result.no_rebalance.empty_station_hours} | "
            f"{result.no_rebalance.unfulfilled_requests} | 0 | 0/0 | 0 |"
        ),
        (
            f"| 현재 route-v2 (oracle need) | {result.current_route_v2.empty_station_hours} | "
            f"{result.current_route_v2.unfulfilled_requests} | {result.current_route_v2.moved_bikes} | "
            f"{result.current_route_v2.route_count}/{result.current_route_v2.route_stop_count} | "
            f"{result.current_route_v2.estimated_vehicle_minutes:.1f} |"
        ),
        "",
        f"실제 운영 대비 route-v2 품절 대여소-시간 변화: **{change}**",
        "",
        "## 해석 제한",
        "",
    ]
    lines.extend(f"- {assumption}" for assumption in result.assumptions)
    lines.extend(
        (
            "",
            (
                "> 이 결과는 미래 실제 수요를 사용한 경로 알고리즘 탐색용 상한이다. "
                "역사 시점 모델 예측을 연결하기 전에는 시스템의 실제 개선율로 발표하면 안 된다."
            ),
            "",
        )
    )
    return "\n".join(lines)


def write_result(result: BacktestResult, output_dir: Path) -> tuple[Path, Path]:
    """동일 결과를 기계 판독 JSON과 검토용 Markdown으로 저장한다."""
    output_dir.mkdir(parents=True, exist_ok=True)
    stem = f"{result.target_date}-{result.center_id}-{result.window_start[11:13]}h"
    json_path = output_dir / f"{stem}.json"
    markdown_path = output_dir / f"{stem}.md"
    json_path.write_text(
        json.dumps(asdict(result), ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(result_markdown(result), encoding="utf-8")
    return json_path, markdown_path


def _allocate_quantities(
    candidates: Sequence[tuple[float, int, StationMetadata]],
    total: int,
) -> tuple[tuple[float, int, StationMetadata], ...]:
    """우선순위가 높은 후보부터 전체 수량 상한 안에서 양수를 배정한다."""
    remaining = max(0, total)
    allocated = []
    for urgency, quantity, station in sorted(
        candidates,
        key=lambda row: (-row[0], row[2].station_id),
    ):
        take = min(quantity, remaining)
        if take > 0:
            allocated.append((urgency, take, station))
            remaining -= take
        if remaining == 0:
            break
    return tuple(allocated)


def _actual_flows(
    trips: Sequence[RentalTrip],
    station_nos: frozenset[int],
    start: datetime,
    end: datetime,
) -> tuple[Counter[int], Counter[int]]:
    """반개구간의 실제 대여·반납 횟수를 대여소별로 집계한다."""
    rentals: Counter[int] = Counter()
    returns: Counter[int] = Counter()
    for trip in trips:
        if start <= trip.rented_at < end and trip.rent_station_no in station_nos:
            rentals[trip.rent_station_no] += 1
        if start <= trip.returned_at < end and trip.return_station_no in station_nos:
            returns[trip.return_station_no] += 1
    return rentals, returns


def _observations_by_time(
    observations: Sequence[StockObservation],
    station_nos: frozenset[int],
) -> dict[datetime, dict[int, int]]:
    """실측 재고를 정시와 대여소 번호의 중첩 사전으로 바꾼다."""
    indexed: dict[datetime, dict[int, int]] = defaultdict(dict)
    for observation in observations:
        if observation.station_no in station_nos:
            indexed[observation.observed_at][observation.station_no] = (
                observation.quantity
            )
    return dict(indexed)


def _actual_empty_station_hours(
    observations: dict[datetime, dict[int, int]],
    station_nos: frozenset[int],
    checkpoints: Sequence[datetime],
) -> int:
    """실제 정시 재고가 0인 대여소-시간 셀 수를 계산한다."""
    return sum(
        observations[checkpoint].get(station_no, 0) <= 0
        for checkpoint in checkpoints
        for station_no in station_nos
    )


def _station_no(value: str) -> int:
    """선행 0이 있는 공공 대여소 번호를 정수 식별자로 정규화한다."""
    normalized = value.strip().strip('"')
    if not normalized:
        raise ValueError("대여소 번호가 비었습니다.")
    return int(normalized)


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


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """백테스트 CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(
        description="실제 수요 기반 재배치 소규모 백테스트"
    )
    parser.add_argument("--date", required=True, type=date.fromisoformat)
    parser.add_argument("--center", required=True)
    parser.add_argument("--start-hour", type=int, default=6)
    parser.add_argument("--duration-hours", type=int, default=6)
    parser.add_argument("--rental-csv", required=True, type=Path)
    parser.add_argument("--stock-csv", required=True, type=Path)
    parser.add_argument(
        "--station-json",
        type=Path,
        default=Path("../apps/api/seed_data/stations_seoul.json"),
    )
    parser.add_argument(
        "--center-seed",
        type=Path,
        default=Path("../docs/gold/dispatch-center-seed.yaml"),
    )
    parser.add_argument(
        "--output-dir", type=Path, default=Path("../data/backtest-results")
    )
    args = parser.parse_args(argv)
    if not 0 <= args.start_hour <= 23:
        parser.error("--start-hour는 0..23이어야 합니다.")
    if not 1 <= args.duration_hours <= 24 - args.start_hour:
        parser.error("평가 구간은 목표일 안의 1시간 이상이어야 합니다.")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    """CLI 입력으로 백테스트를 실행하고 결과 파일 위치를 출력한다."""
    args = parse_args(argv)
    result = run_backtest(
        target_date=args.date,
        center_id=args.center,
        start_hour=args.start_hour,
        duration_hours=args.duration_hours,
        rental_csv=args.rental_csv,
        stock_csv=args.stock_csv,
        station_json=args.station_json,
        center_seed=args.center_seed,
    )
    json_path, markdown_path = write_result(result, args.output_dir)
    print(result_markdown(result))
    print(f"JSON: {json_path}")
    print(f"Markdown: {markdown_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
