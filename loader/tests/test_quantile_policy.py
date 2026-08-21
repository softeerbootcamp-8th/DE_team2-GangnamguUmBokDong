"""Quantile 재고 구간 수량 정책의 보수적 clamp를 검증한다."""

from datetime import UTC, datetime, timedelta

import pytest
from evaluation.historical_inputs import DemandForecastQuantiles
from evaluation.quantile_policy import (
    QuantileQuantityPolicy,
    quantile_bike_quantities,
)
from gold.urgency import UrgencyRecord

BASE = datetime(2025, 6, 16, 21, tzinfo=UTC)


def _urgency(station_id: str, action: str) -> UrgencyRecord:
    """수량 계산에 필요한 합성 urgency 행을 만든다."""
    return UrgencyRecord(station_id, BASE, 0.8, 30, action, 99)


def _forecast(
    station_id: str,
    horizon: int,
    *,
    rental: tuple[float, float, float],
    returned: tuple[float, float, float],
) -> DemandForecastQuantiles:
    """한 horizon의 합성 대여·반납 quantile을 만든다."""
    return DemandForecastQuantiles(
        base_dttm=BASE,
        sta_id=station_id,
        predicted_dttm=BASE + timedelta(hours=horizon),
        rental_p10=rental[0],
        rental_p50=rental[1],
        rental_p90=rental[2],
        return_p10=returned[0],
        return_p50=returned[1],
        return_p90=returned[2],
    )


def test_quantile_policy_caps_mean_target_with_adverse_safety_paths() -> None:
    """평균 정책 필요량을 q10·q90 안전 여유와 현재 재고로 제한한다."""
    forecasts = tuple(
        [
            _forecast(
                "ST-1",
                horizon,
                rental=(0.0, 0.0, 0.0),
                returned=(0.0, 2.0, 3.0),
            )
            for horizon in (1, 2)
        ]
        + [
            _forecast(
                "ST-2",
                horizon,
                rental=(2.0, 3.0, 4.0),
                returned=(0.0, 0.0, 0.0),
            )
            for horizon in (1, 2)
        ]
    )
    quantities = quantile_bike_quantities(
        urgency=(
            _urgency("ST-1", "retrieval_needed"),
            _urgency("ST-2", "supply_needed"),
        ),
        current_stock={"ST-1": 10, "ST-2": 0},
        capacities={"ST-1": 10, "ST-2": 10},
        forecasts=forecasts,
        policy=QuantileQuantityPolicy(),
        max_pickup_stock_fraction=0.5,
    )
    assert quantities == {"ST-1": 5, "ST-2": 9}


def test_quantile_policy_blocks_pickup_when_lower_path_needs_the_stock() -> None:
    """중앙 경로가 넘쳐도 하방 재고가 reserve를 깨면 회수하지 않는다."""
    forecasts = tuple(
        _forecast(
            "ST-1",
            horizon,
            rental=(0.0, 0.0, 10.0),
            returned=(0.0, 2.0, 3.0),
        )
        for horizon in (1, 2)
    )
    quantities = quantile_bike_quantities(
        urgency=(_urgency("ST-1", "retrieval_needed"),),
        current_stock={"ST-1": 10},
        capacities={"ST-1": 10},
        forecasts=forecasts,
        policy=QuantileQuantityPolicy(),
        max_pickup_stock_fraction=1.0,
    )
    assert quantities == {"ST-1": 0}


def test_quantile_policy_does_not_net_returns_before_same_hour_rentals() -> None:
    """시간별 순수요가 0이어도 대여가 먼저 올 수 있으면 공급원 재고를 보호한다."""
    forecasts = tuple(
        _forecast(
            "ST-1",
            horizon,
            rental=(2.0, 2.0, 2.0),
            returned=(2.0, 2.0, 2.0),
        )
        for horizon in (1, 2)
    )
    quantities = quantile_bike_quantities(
        urgency=(_urgency("ST-1", "retrieval_needed"),),
        current_stock={"ST-1": 2},
        capacities={"ST-1": 3},
        forecasts=forecasts,
        policy=QuantileQuantityPolicy(),
        max_pickup_stock_fraction=1.0,
    )
    assert quantities == {"ST-1": 0}


def test_forecast_quantiles_reject_crossed_interval() -> None:
    """q10·q50·q90 순서가 깨진 모델 출력은 fail-closed한다."""
    with pytest.raises(ValueError, match="q10 <= q50 <= q90"):
        _forecast(
            "ST-1",
            1,
            rental=(2.0, 1.0, 3.0),
            returned=(0.0, 1.0, 2.0),
        )
