"""기존 운영 잔차 역산과 시각 불확실성 범위 계산을 검증한다."""

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from evaluation.legacy_baseline import infer_legacy_movements, replay_legacy_timing
from evaluation.rebalance_backtest import RentalTrip, StockObservation

SEOUL = ZoneInfo("Asia/Seoul")
START = datetime(2025, 6, 17, 6, tzinfo=SEOUL)
END = START + timedelta(hours=1)


def test_residual_is_exact_operator_balance_identity() -> None:
    """역산 개입을 더하면 구간 말 실측 재고를 오차 없이 복원한다."""
    trips = (
        RentalTrip("A", START + timedelta(minutes=10), 1, START + timedelta(minutes=20), 2),
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
