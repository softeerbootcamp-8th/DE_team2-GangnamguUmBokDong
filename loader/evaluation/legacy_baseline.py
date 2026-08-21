"""시간별 재고 잔차로 기존 운영 개입을 역산하고 시각 불확실성을 평가한다."""

from __future__ import annotations

import heapq
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from itertools import pairwise

from .rebalance_backtest import RentalTrip, StockObservation


@dataclass(frozen=True, slots=True)
class OperatorAdjustment:
    """시민 흐름으로 설명되지 않는 station별 순재고 변화를 표현한다."""

    interval_start: datetime
    interval_end: datetime
    station_no: int
    quantity: int


@dataclass(frozen=True, slots=True)
class LegacyMovementEstimate:
    """기존 운영 잔차와 공통 비교에 사용할 균형 이동 예산을 표현한다."""

    adjustments: tuple[OperatorAdjustment, ...]
    added_bikes: int
    removed_bikes: int
    balanced_movement_budget: int
    external_imbalance_bikes: int


@dataclass(frozen=True, slots=True)
class LegacyTimingMetrics:
    """운영 개입 시각 가정 하나에서 재현한 재고 가용성 결과를 표현한다."""

    timing: str
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
) -> LegacyMovementEstimate:
    """각 시간 구간의 실측 재고에서 시민 대여·반납을 제거한 잔차를 계산한다."""
    by_time: dict[datetime, dict[int, int]] = defaultdict(dict)
    for row in observations:
        if row.station_no in station_nos:
            by_time[row.observed_at][row.station_no] = row.quantity
    checkpoints = sorted(
        moment for moment in by_time if window_start <= moment <= window_end
    )
    if not checkpoints or checkpoints[0] != window_start or checkpoints[-1] != window_end:
        raise ValueError("기존 운영 역산에는 시작·종료를 포함한 정시 재고가 필요합니다.")
    adjustments = []
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
            if station_no not in by_time[interval_start] or station_no not in by_time[interval_end]:
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
    added = sum(max(0, row.quantity) for row in adjustments)
    removed = sum(max(0, -row.quantity) for row in adjustments)
    return LegacyMovementEstimate(
        adjustments=tuple(adjustments),
        added_bikes=added,
        removed_bikes=removed,
        balanced_movement_budget=min(added, removed),
        external_imbalance_bikes=abs(added - removed),
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

    def push(moment: datetime, priority: int, kind: str, station_no: int, quantity: int) -> None:
        """재고 사건을 결정적인 tie-break 순서로 추가한다."""
        nonlocal sequence
        heapq.heappush(events, (moment, priority, sequence, kind, station_no, quantity))
        sequence += 1

    fraction = fractions[timing]
    for adjustment in estimate.adjustments:
        moment = adjustment.interval_start + (
            adjustment.interval_end - adjustment.interval_start
        ) * fraction
        push(moment, 0, "operator", adjustment.station_no, adjustment.quantity)
    for trip in trips:
        if window_start <= trip.rented_at < window_end and trip.rent_station_no in station_nos:
            push(trip.rented_at, 2, "rental", trip.rent_station_no, -1)
        if window_start <= trip.returned_at < window_end and trip.return_station_no in station_nos:
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
        (abs(stock[station_no] - expected_end[station_no]) for station_no in station_nos),
        default=0,
    )
    return LegacyTimingMetrics(
        timing=timing,
        empty_station_minutes=round(empty_minutes, 3),
        negative_station_minutes=round(negative_minutes, 3),
        minimum_stock=minimum_stock,
        endpoint_max_absolute_error=endpoint_error,
    )
