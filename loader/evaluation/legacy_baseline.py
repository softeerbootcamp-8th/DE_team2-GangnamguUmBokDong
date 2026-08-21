"""시간별 재고 잔차로 기존 운영 개입을 역산하고 시각 불확실성을 평가한다."""

from __future__ import annotations

import hashlib
import heapq
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime
from itertools import pairwise

from .rebalance_backtest import BikeRelocationInterval, RentalTrip, StockObservation


@dataclass(frozen=True, slots=True)
class OperatorAdjustment:
    """시민 흐름으로 설명되지 않는 station별 순재고 변화를 표현한다."""

    interval_start: datetime
    interval_end: datetime
    station_no: int
    quantity: int


@dataclass(frozen=True, slots=True)
class AssignedRelocationEvidence:
    """재고 잔차 방향과 양립해 하나의 정시 구간에 할당한 ID 이동을 표현한다."""

    bike_id_sha256: str
    origin_station_no: int
    destination_station_no: int
    earliest_at: datetime
    latest_at: datetime
    assigned_interval_start: datetime
    assigned_interval_end: datetime
    hourly_bounded: bool
    source_gap_minutes: float


@dataclass(frozen=True, slots=True)
class RelocationEvidenceSummary:
    """자전거 ID 이동구간의 검열 상태와 재고 잔차 결합 결과를 요약한다."""

    candidate_intervals: int
    internal_candidate_intervals: int
    boundary_candidate_intervals: int
    inside_window_intervals: int
    left_censored_intervals: int
    right_censored_intervals: int
    spanning_window_intervals: int
    hourly_bounded_intervals: int
    hourly_bounded_internal_intervals: int
    hourly_bounded_unmatched_intervals: int
    residual_compatible_intervals: int
    residual_compatible_internal_intervals: int
    residual_compatible_within_1h: int
    residual_compatible_within_6h: int
    residual_compatible_within_24h: int
    residual_compatible_within_7d: int
    residual_station_units: int
    residual_explained_station_units: int
    residual_explained_pct: float | None
    gap_minutes_p50: float | None
    gap_minutes_p95: float | None
    assigned_gap_minutes_p50: float | None
    assigned_gap_minutes_p95: float | None


@dataclass(frozen=True, slots=True)
class LegacyMovementEstimate:
    """기존 운영 잔차와 공통 비교에 사용할 균형 이동 예산을 표현한다."""

    adjustments: tuple[OperatorAdjustment, ...]
    added_bikes: int
    removed_bikes: int
    balanced_movement_budget: int
    external_imbalance_bikes: int
    relocation_evidence: RelocationEvidenceSummary
    assigned_relocations: tuple[AssignedRelocationEvidence, ...]
    remaining_adjustments: tuple[OperatorAdjustment, ...]


@dataclass(frozen=True, slots=True)
class LegacyTimingMetrics:
    """운영 개입 시각 가정 하나에서 재현한 재고 가용성 결과를 표현한다."""

    timing: str
    reconstruction: str
    empty_station_minutes: float
    negative_station_minutes: float
    minimum_stock: int
    endpoint_max_absolute_error: int


def infer_legacy_movements(
    *,
    observations: Sequence[StockObservation],
    trips: Sequence[RentalTrip],
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
    relocations: Sequence[BikeRelocationInterval] = (),
) -> LegacyMovementEstimate:
    """재고 순잔차를 ID 이동 증거로 분해한 하이브리드 운영 추정을 만든다."""
    by_time: dict[datetime, dict[int, int]] = defaultdict(dict)
    for row in observations:
        if row.station_no in station_nos:
            by_time[row.observed_at][row.station_no] = row.quantity
    checkpoints = sorted(
        moment for moment in by_time if window_start <= moment <= window_end
    )
    if (
        not checkpoints
        or checkpoints[0] != window_start
        or checkpoints[-1] != window_end
    ):
        raise ValueError(
            "기존 운영 역산에는 시작·종료를 포함한 정시 재고가 필요합니다."
        )
    adjustments: list[OperatorAdjustment] = []
    for interval_start, interval_end in pairwise(checkpoints):
        rentals: dict[int, int] = defaultdict(int)
        returns: dict[int, int] = defaultdict(int)
        for trip in trips:
            if (
                interval_start <= trip.rented_at < interval_end
                and trip.rent_station_no in station_nos
            ):
                rentals[trip.rent_station_no] += 1
            if (
                interval_start <= trip.returned_at < interval_end
                and trip.return_station_no in station_nos
            ):
                returns[trip.return_station_no] += 1
        for station_no in sorted(station_nos):
            if (
                station_no not in by_time[interval_start]
                or station_no not in by_time[interval_end]
            ):
                raise ValueError(
                    f"기존 운영 역산 정시 재고가 누락됐습니다: {interval_start}, {station_no}"
                )
            citizen_only_end = (
                by_time[interval_start][station_no]
                - rentals[station_no]
                + returns[station_no]
            )
            residual = by_time[interval_end][station_no] - citizen_only_end
            if residual:
                adjustments.append(
                    OperatorAdjustment(
                        interval_start=interval_start,
                        interval_end=interval_end,
                        station_no=station_no,
                        quantity=residual,
                    )
                )
    assigned, evidence = _assign_relocation_evidence(
        relocations=relocations,
        checkpoints=checkpoints,
        station_nos=station_nos,
        window_start=window_start,
        window_end=window_end,
        residuals=adjustments,
    )
    assigned_flows: dict[tuple[datetime, datetime, int], int] = defaultdict(int)
    for relocation in assigned:
        interval_start = relocation.assigned_interval_start
        interval_end = relocation.assigned_interval_end
        if relocation.origin_station_no in station_nos:
            assigned_flows[
                (interval_start, interval_end, relocation.origin_station_no)
            ] -= 1
        if relocation.destination_station_no in station_nos:
            assigned_flows[
                (interval_start, interval_end, relocation.destination_station_no)
            ] += 1
    residual_by_key = {
        (row.interval_start, row.interval_end, row.station_no): row.quantity
        for row in adjustments
    }
    remaining = []
    for key in sorted(set(residual_by_key) | set(assigned_flows)):
        quantity = residual_by_key.get(key, 0) - assigned_flows.get(key, 0)
        if quantity:
            remaining.append(
                OperatorAdjustment(
                    interval_start=key[0],
                    interval_end=key[1],
                    station_no=key[2],
                    quantity=quantity,
                )
            )
    added = sum(max(0, row.quantity) for row in adjustments)
    removed = sum(max(0, -row.quantity) for row in adjustments)
    return LegacyMovementEstimate(
        adjustments=tuple(adjustments),
        added_bikes=added,
        removed_bikes=removed,
        balanced_movement_budget=min(added, removed),
        external_imbalance_bikes=abs(added - removed),
        relocation_evidence=evidence,
        assigned_relocations=assigned,
        remaining_adjustments=tuple(remaining),
    )


def replay_legacy_timing(
    *,
    timing: str,
    estimate: LegacyMovementEstimate,
    observations: Sequence[StockObservation],
    trips: Sequence[RentalTrip],
    initial_stock: Mapping[int, int],
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
    use_lineage_assignment: bool = True,
) -> LegacyTimingMetrics:
    """기존 운영 잔차를 구간 초·중·말에 적용해 식별 불가능 범위를 계산한다."""
    fractions = {
        "interval_start": 0.0,
        "interval_midpoint": 0.5,
        "interval_end": 1.0,
    }
    if timing not in fractions:
        raise ValueError(f"알 수 없는 기존 운영 시각 가정입니다: {timing}")
    stock = {station_no: int(initial_stock[station_no]) for station_no in station_nos}
    events: list[tuple[datetime, int, int, str, int, int]] = []
    sequence = 0

    def push(
        moment: datetime, priority: int, kind: str, station_no: int, quantity: int
    ) -> None:
        """재고 사건을 결정적인 tie-break 순서로 추가한다."""
        nonlocal sequence
        heapq.heappush(events, (moment, priority, sequence, kind, station_no, quantity))
        sequence += 1

    fraction = fractions[timing]
    adjustments = (
        estimate.remaining_adjustments
        if use_lineage_assignment
        else estimate.adjustments
    )
    for adjustment in adjustments:
        moment = (
            adjustment.interval_start
            + (adjustment.interval_end - adjustment.interval_start) * fraction
        )
        push(moment, 0, "operator", adjustment.station_no, adjustment.quantity)
    relocations = estimate.assigned_relocations if use_lineage_assignment else ()
    for relocation in relocations:
        moment = (
            relocation.earliest_at
            + (relocation.latest_at - relocation.earliest_at) * fraction
        )
        if relocation.origin_station_no in station_nos:
            push(moment, 2, "operator_relocation", relocation.origin_station_no, -1)
        if relocation.destination_station_no in station_nos:
            push(moment, 2, "operator_relocation", relocation.destination_station_no, 1)
    for trip in trips:
        if (
            window_start <= trip.rented_at < window_end
            and trip.rent_station_no in station_nos
        ):
            push(trip.rented_at, 3, "rental", trip.rent_station_no, -1)
        if (
            window_start <= trip.returned_at < window_end
            and trip.return_station_no in station_nos
        ):
            push(trip.returned_at, 1, "return", trip.return_station_no, 1)

    empty_minutes = 0.0
    negative_minutes = 0.0
    minimum_stock = min(stock.values())
    previous = window_start
    while events:
        moment, _, _, _, station_no, quantity = heapq.heappop(events)
        if moment > window_end:
            break
        elapsed = (moment - previous).total_seconds() / 60.0
        empty_minutes += elapsed * sum(value <= 0 for value in stock.values())
        negative_minutes += elapsed * sum(value < 0 for value in stock.values())
        stock[station_no] += quantity
        minimum_stock = min(minimum_stock, stock[station_no])
        previous = moment
    elapsed = (window_end - previous).total_seconds() / 60.0
    empty_minutes += elapsed * sum(value <= 0 for value in stock.values())
    negative_minutes += elapsed * sum(value < 0 for value in stock.values())
    expected_end = {
        row.station_no: row.quantity
        for row in observations
        if row.observed_at == window_end and row.station_no in station_nos
    }
    endpoint_error = max(
        (
            abs(stock[station_no] - expected_end[station_no])
            for station_no in station_nos
        ),
        default=0,
    )
    return LegacyTimingMetrics(
        timing=timing,
        reconstruction=(
            "id_residual_compatible" if use_lineage_assignment else "residual_only"
        ),
        empty_station_minutes=round(empty_minutes, 3),
        negative_station_minutes=round(negative_minutes, 3),
        minimum_stock=minimum_stock,
        endpoint_max_absolute_error=endpoint_error,
    )


def _assign_relocation_evidence(
    *,
    relocations: Sequence[BikeRelocationInterval],
    checkpoints: Sequence[datetime],
    station_nos: frozenset[int],
    window_start: datetime,
    window_end: datetime,
    residuals: Sequence[OperatorAdjustment],
) -> tuple[tuple[AssignedRelocationEvidence, ...], RelocationEvidenceSummary]:
    """ID 이동 후보를 양립 가능한 시간별 재고 잔차에 결정적으로 할당한다.

    할당은 이동 사실의 정확한 시각을 확정하지 않는다. 같은 후보를 한 번만 사용하고
    출발지는 음의 잔차, 도착지는 양의 잔차가 남은 정시 구간에만 연결함으로써 ID
    연속성과 재고 항등식을 동시에 만족하는 가능한 운영 재구성 하나를 만든다.
    """
    relevant = [
        row
        for row in relocations
        if row.earliest_at < window_end
        and row.latest_at >= window_start
        and (
            row.origin_station_no in station_nos
            or row.destination_station_no in station_nos
        )
    ]
    internal = [
        row
        for row in relevant
        if row.origin_station_no in station_nos
        and row.destination_station_no in station_nos
    ]
    inside = [
        row
        for row in relevant
        if row.earliest_at >= window_start and row.latest_at <= window_end
    ]
    left = [
        row
        for row in relevant
        if row.earliest_at < window_start and row.latest_at <= window_end
    ]
    right = [
        row
        for row in relevant
        if row.earliest_at >= window_start and row.latest_at > window_end
    ]
    spanning = [
        row
        for row in relevant
        if row.earliest_at < window_start and row.latest_at > window_end
    ]
    checkpoint_intervals = tuple(pairwise(checkpoints))
    remaining = {
        (row.interval_start, row.interval_end, row.station_no): row.quantity
        for row in residuals
    }
    assigned: list[AssignedRelocationEvidence] = []
    hourly_bounded_total = 0
    hourly_bounded_internal = 0
    hourly_bounded_unmatched = 0
    for row in sorted(
        relevant,
        key=lambda item: _relocation_assignment_order(item, station_nos),
    ):
        eligible = [
            interval
            for interval in checkpoint_intervals
            if _relocation_can_occur_in_interval(row, *interval)
        ]
        bounded = len(eligible) == 1 and (
            eligible[0][0] <= row.earliest_at <= row.latest_at < eligible[0][1]
        )
        if bounded:
            hourly_bounded_total += 1
            if (
                row.origin_station_no in station_nos
                and row.destination_station_no in station_nos
            ):
                hourly_bounded_internal += 1
        compatible = [
            interval
            for interval in eligible
            if _residual_direction_is_compatible(
                row,
                interval_start=interval[0],
                interval_end=interval[1],
                remaining=remaining,
                station_nos=station_nos,
            )
        ]
        if not compatible:
            if bounded:
                hourly_bounded_unmatched += 1
            continue
        interval_start, interval_end = min(
            compatible,
            key=lambda interval: _assignment_interval_order(row, *interval),
        )
        if row.origin_station_no in station_nos:
            remaining[(interval_start, interval_end, row.origin_station_no)] += 1
        if row.destination_station_no in station_nos:
            remaining[(interval_start, interval_end, row.destination_station_no)] -= 1
        assigned.append(
            AssignedRelocationEvidence(
                bike_id_sha256=hashlib.sha256(row.bike_id.encode("utf-8")).hexdigest(),
                origin_station_no=row.origin_station_no,
                destination_station_no=row.destination_station_no,
                earliest_at=max(row.earliest_at, interval_start),
                latest_at=min(row.latest_at, interval_end),
                assigned_interval_start=interval_start,
                assigned_interval_end=interval_end,
                hourly_bounded=bounded,
                source_gap_minutes=(row.latest_at - row.earliest_at).total_seconds()
                / 60.0,
            )
        )
    residual_units = sum(abs(row.quantity) for row in residuals)
    remaining_units = sum(abs(quantity) for quantity in remaining.values())
    explained_units = residual_units - remaining_units
    gaps = sorted(
        (row.latest_at - row.earliest_at).total_seconds() / 60.0 for row in relevant
    )
    assigned_gaps = sorted(row.source_gap_minutes for row in assigned)
    evidence = RelocationEvidenceSummary(
        candidate_intervals=len(relevant),
        internal_candidate_intervals=len(internal),
        boundary_candidate_intervals=len(relevant) - len(internal),
        inside_window_intervals=len(inside),
        left_censored_intervals=len(left),
        right_censored_intervals=len(right),
        spanning_window_intervals=len(spanning),
        hourly_bounded_intervals=hourly_bounded_total,
        hourly_bounded_internal_intervals=hourly_bounded_internal,
        hourly_bounded_unmatched_intervals=hourly_bounded_unmatched,
        residual_compatible_intervals=len(assigned),
        residual_compatible_internal_intervals=sum(
            row.origin_station_no in station_nos
            and row.destination_station_no in station_nos
            for row in assigned
        ),
        residual_compatible_within_1h=sum(gap <= 60 for gap in assigned_gaps),
        residual_compatible_within_6h=sum(gap <= 360 for gap in assigned_gaps),
        residual_compatible_within_24h=sum(gap <= 1440 for gap in assigned_gaps),
        residual_compatible_within_7d=sum(gap <= 10080 for gap in assigned_gaps),
        residual_station_units=residual_units,
        residual_explained_station_units=explained_units,
        residual_explained_pct=(
            None
            if residual_units == 0
            else round(explained_units / residual_units * 100.0, 3)
        ),
        gap_minutes_p50=_percentile(gaps, 0.5),
        gap_minutes_p95=_percentile(gaps, 0.95),
        assigned_gap_minutes_p50=_percentile(assigned_gaps, 0.5),
        assigned_gap_minutes_p95=_percentile(assigned_gaps, 0.95),
    )
    return tuple(assigned), evidence


def _relocation_assignment_order(
    row: BikeRelocationInterval,
    station_nos: frozenset[int],
) -> tuple[bool, float, datetime, int, int, str]:
    """짧고 시간정보가 강한 이동 후보부터 사용하는 결정적 순서를 반환한다."""
    return (
        not (
            row.origin_station_no in station_nos
            and row.destination_station_no in station_nos
        ),
        (row.latest_at - row.earliest_at).total_seconds(),
        row.latest_at,
        row.origin_station_no,
        row.destination_station_no,
        row.bike_id,
    )


def _relocation_can_occur_in_interval(
    row: BikeRelocationInterval,
    interval_start: datetime,
    interval_end: datetime,
) -> bool:
    """ID가 허용하는 이동시각과 정시 재고 구간이 겹치는지 반환한다."""
    return row.earliest_at < interval_end and row.latest_at >= interval_start


def _residual_direction_is_compatible(
    row: BikeRelocationInterval,
    *,
    interval_start: datetime,
    interval_end: datetime,
    remaining: Mapping[tuple[datetime, datetime, int], int],
    station_nos: frozenset[int],
) -> bool:
    """후보 이동이 선택 구간의 남은 출발·도착 잔차 방향과 맞는지 반환한다."""
    origin_inside = row.origin_station_no in station_nos
    destination_inside = row.destination_station_no in station_nos
    origin_matches = (
        not origin_inside
        or remaining.get((interval_start, interval_end, row.origin_station_no), 0) < 0
    )
    destination_matches = (
        not destination_inside
        or remaining.get((interval_start, interval_end, row.destination_station_no), 0)
        > 0
    )
    return (
        (origin_inside or destination_inside) and origin_matches and destination_matches
    )


def _assignment_interval_order(
    row: BikeRelocationInterval,
    interval_start: datetime,
    interval_end: datetime,
) -> tuple[float, datetime]:
    """후보 구간 중 ID 시간 중심과 가까운 정시 구간을 우선하는 순서를 반환한다."""
    candidate_midpoint = row.earliest_at + (row.latest_at - row.earliest_at) / 2
    interval_midpoint = interval_start + (interval_end - interval_start) / 2
    return (
        abs((candidate_midpoint - interval_midpoint).total_seconds()),
        interval_start,
    )


def _percentile(values: Sequence[float], quantile: float) -> float | None:
    """작은 감사 표면에 사용할 nearest-rank 분위수를 반환한다."""
    if not values:
        return None
    index = max(0, min(len(values) - 1, int(len(values) * quantile + 0.999999) - 1))
    return round(values[index], 3)
