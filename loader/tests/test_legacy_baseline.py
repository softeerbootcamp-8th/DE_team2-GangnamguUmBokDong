"""기존 운영 잔차 역산과 시각 불확실성 범위 계산을 검증한다."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from evaluation.legacy_baseline import infer_legacy_movements, replay_legacy_timing
from evaluation.rebalance_backtest import (
    BikeRelocationInterval,
    RentalTrip,
    StockObservation,
)

SEOUL = ZoneInfo("Asia/Seoul")
START = datetime(2025, 6, 17, 6, tzinfo=SEOUL)
END = START + timedelta(hours=1)


def test_residual_is_exact_operator_balance_identity() -> None:
    """역산 개입을 더하면 구간 말 실측 재고를 오차 없이 복원한다."""
    trips = (
        RentalTrip(
            "A", START + timedelta(minutes=10), 1, START + timedelta(minutes=20), 2
        ),
    )
    observations = (
        StockObservation(START, 1, 5),
        StockObservation(START, 2, 1),
        StockObservation(END, 1, 2),
        StockObservation(END, 2, 4),
    )
    estimate = infer_legacy_movements(
        observations=observations,
        trips=trips,
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
    )
    assert estimate.added_bikes == 2
    assert estimate.removed_bikes == 2
    assert estimate.balanced_movement_budget == 2
    for timing in ("interval_start", "interval_midpoint", "interval_end"):
        metrics = replay_legacy_timing(
            timing=timing,
            estimate=estimate,
            observations=observations,
            trips=trips,
            initial_stock={1: 5, 2: 1},
            station_nos=frozenset((1, 2)),
            window_start=START,
            window_end=END,
        )
        assert metrics.endpoint_max_absolute_error == 0


def test_unknown_operator_time_changes_empty_minutes_and_is_not_hidden() -> None:
    """같은 잔차도 적용 시각에 따라 품절시간이 달라져 세 시나리오를 모두 남긴다."""
    observations = (
        StockObservation(START, 1, 0),
        StockObservation(START, 2, 2),
        StockObservation(END, 1, 1),
        StockObservation(END, 2, 1),
    )
    estimate = infer_legacy_movements(
        observations=observations,
        trips=(),
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
    )
    values = []
    for timing in ("interval_start", "interval_midpoint", "interval_end"):
        values.append(
            replay_legacy_timing(
                timing=timing,
                estimate=estimate,
                observations=observations,
                trips=(),
                initial_stock={1: 0, 2: 2},
                station_nos=frozenset((1, 2)),
                window_start=START,
                window_end=END,
            ).empty_station_minutes
        )
    assert values == [0.0, 30.0, 60.0]


def test_bounded_bike_relocation_refines_residual_timing_and_reconciles() -> None:
    """한 정시 구간에 닫힌 ID 이동은 잔차에서 분리해 자체 시각 범위로 재생한다."""
    observations = (
        StockObservation(START, 1, 3),
        StockObservation(START, 2, 0),
        StockObservation(END, 1, 2),
        StockObservation(END, 2, 1),
    )
    relocation = BikeRelocationInterval(
        bike_id="A",
        origin_station_no=1,
        destination_station_no=2,
        earliest_at=START + timedelta(minutes=10),
        latest_at=START + timedelta(minutes=20),
    )
    estimate = infer_legacy_movements(
        observations=observations,
        trips=(),
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
        relocations=(relocation,),
    )
    assert estimate.relocation_evidence.hourly_bounded_internal_intervals == 1
    assert estimate.relocation_evidence.residual_compatible_internal_intervals == 1
    assert estimate.relocation_evidence.residual_explained_station_units == 2
    assert estimate.relocation_evidence.residual_explained_pct == 100.0
    assert estimate.remaining_adjustments == ()
    values = []
    for timing in ("interval_start", "interval_midpoint", "interval_end"):
        metrics = replay_legacy_timing(
            timing=timing,
            estimate=estimate,
            observations=observations,
            trips=(),
            initial_stock={1: 3, 2: 0},
            station_nos=frozenset((1, 2)),
            window_start=START,
            window_end=END,
        )
        assert metrics.endpoint_max_absolute_error == 0
        values.append(metrics.empty_station_minutes)
    assert values == [10.0, 15.0, 20.0]


def test_censored_bike_relocation_is_only_assigned_to_compatible_residual() -> None:
    """긴 ID 구간도 같은 후보를 한 번만 써서 잔차 방향과 맞는 구간에 할당한다."""
    observations = (
        StockObservation(START, 1, 3),
        StockObservation(START, 2, 0),
        StockObservation(END, 1, 2),
        StockObservation(END, 2, 1),
    )
    relocation = BikeRelocationInterval(
        bike_id="A",
        origin_station_no=1,
        destination_station_no=2,
        earliest_at=START - timedelta(hours=2),
        latest_at=END + timedelta(hours=2),
    )
    estimate = infer_legacy_movements(
        observations=observations,
        trips=(),
        station_nos=frozenset((1, 2)),
        window_start=START,
        window_end=END,
        relocations=(relocation,),
    )
    evidence = estimate.relocation_evidence
    assert evidence.spanning_window_intervals == 1
    assert evidence.hourly_bounded_intervals == 0
    assert evidence.residual_compatible_internal_intervals == 1
    assert evidence.residual_explained_pct == 100.0
    assert len(estimate.assigned_relocations) == 1
    assert estimate.remaining_adjustments == ()
