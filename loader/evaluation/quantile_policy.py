"""대여·반납 quantile로 재배치 수량의 동적 재고 구간을 계산한다."""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any

from gold.urgency import UrgencyRecord

from .historical_inputs import DemandForecastQuantiles


@dataclass(frozen=True, slots=True)
class QuantileQuantityPolicy:
    """q10·q50·q90 경로와 물리적 최소 여유로 수량을 결정하는 정책이다."""

    version: str = "quantile-guard-v1"
    horizon_hours: int = 2
    minimum_bikes: int = 1
    minimum_empty_docks: int = 1

    def __post_init__(self) -> None:
        """정책 식별자와 작은 물리적 reserve 범위를 검증한다."""
        if not self.version.strip():
            raise ValueError("quantile policy version은 nonblank여야 합니다.")
        if not 1 <= self.horizon_hours <= 12:
            raise ValueError("quantile horizon은 1..12시간이어야 합니다.")
        if not 0 <= self.minimum_bikes <= 20:
            raise ValueError("minimum bikes는 0..20이어야 합니다.")
        if not 0 <= self.minimum_empty_docks <= 20:
            raise ValueError("minimum empty docks는 0..20이어야 합니다.")

    def audit_document(self) -> dict[str, Any]:
        """결과 JSON에 남길 결정적인 정책 설정을 반환한다."""
        return {
            "version": self.version,
            "quantity_strategy": "quantile_guard",
            "horizon_hours": self.horizon_hours,
            "minimum_bikes": self.minimum_bikes,
            "minimum_empty_docks": self.minimum_empty_docks,
            "stock_lower_path": "rental_q90_before_return_q10_within_hour",
            "stock_upper_path": "return_q90_before_rental_q10_within_hour",
            "quantity_target": "mean_risk_band_quantity_from_urgency",
        }


def quantile_bike_quantities(
    *,
    urgency: Sequence[UrgencyRecord],
    current_stock: Mapping[str, int],
    capacities: Mapping[str, int],
    forecasts: Sequence[DemandForecastQuantiles],
    policy: QuantileQuantityPolicy,
    max_pickup_stock_fraction: float,
) -> dict[str, int]:
    """기존 action·점수는 유지하고 quantile 경로로 이동 수량만 계산한다.

    q10·q90은 시간별 주변 quantile이므로 누적 경로가 결합 확률구간이라는 주장은
    하지 않는다. 동일 모델의 고정 안전재고 후보와 비교할 보수적 의사결정
    envelope로만 사용한다.
    """
    if not 0.0 <= max_pickup_stock_fraction <= 1.0:
        raise ValueError("max pickup stock fraction은 0..1이어야 합니다.")
    grouped: defaultdict[str, list[DemandForecastQuantiles]] = defaultdict(list)
    for row in forecasts:
        grouped[row.sta_id].append(row)
    result = {}
    for row in urgency:
        try:
            current = current_stock[row.sta_id]
            capacity = capacities[row.sta_id]
        except KeyError as exc:
            raise ValueError(
                f"quantile 수량의 대여소 입력이 없습니다: {row.sta_id}"
            ) from exc
        station_forecasts = sorted(
            grouped.get(row.sta_id, ()),
            key=lambda item: item.predicted_dttm,
        )[: policy.horizon_hours]
        if len(station_forecasts) != policy.horizon_hours:
            raise ValueError(f"quantile horizon이 부족합니다: {row.sta_id}")
        lower, upper = _stock_paths(current, station_forecasts)
        if row.rebalance_need_type_cd == "supply_needed":
            desired = row.bike_qty
            safe_room = max(
                0,
                math.floor(capacity - policy.minimum_empty_docks - max(upper)),
            )
            result[row.sta_id] = min(
                desired,
                safe_room,
                max(0, capacity - current),
            )
        elif row.rebalance_need_type_cd == "retrieval_needed":
            desired = row.bike_qty
            safe_stock = max(
                0,
                math.floor(min(lower) - policy.minimum_bikes),
            )
            concentration_limit = math.floor(current * max_pickup_stock_fraction)
            result[row.sta_id] = min(
                desired,
                safe_stock,
                max(0, current - policy.minimum_bikes),
                concentration_limit,
            )
        else:
            result[row.sta_id] = 0
    return result


def _stock_paths(
    current: int,
    forecasts: Sequence[DemandForecastQuantiles],
) -> tuple[
    tuple[float, ...],
    tuple[float, ...],
]:
    """시간 내 사건 순서까지 보수화한 하한·상한 경로를 반환한다."""
    lower = [float(current)]
    upper = [float(current)]
    for row in forecasts:
        lower.extend(
            (
                lower[-1] - row.rental_p90,
                lower[-1] - row.rental_p90 + row.return_p10,
            )
        )
        upper.extend(
            (
                upper[-1] + row.return_p90,
                upper[-1] + row.return_p90 - row.rental_p10,
            )
        )
    return tuple(lower), tuple(upper)
