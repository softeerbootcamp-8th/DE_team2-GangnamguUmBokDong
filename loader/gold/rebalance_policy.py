"""예측 이후 재배치 수량·배차 안전성의 버전 고정 정책을 정의한다."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from core.gold_publication import ContractViolation

_QUANTITY_STRATEGIES = frozenset({"legacy", "risk_band"})
REBALANCE_POLICY_CONFIG_SCHEMA_VERSION = "rebalance-policy-config-v5"
PICKUP_DONOR_GUARD_NONE = "none"
PICKUP_DONOR_GUARD_CAPACITY_RESERVE_V1 = "capacity-reserve-v1"


@dataclass(frozen=True, slots=True)
class RebalancePolicyConfig:
    """긴급도 수량과 공급원 동시 배차에 적용할 설명 가능한 정책을 표현한다."""

    version: str
    quantity_strategy: str
    protection_horizon_hours: int
    minimum_stock_ratio: float
    uncertainty_z: float
    exclusive_pickup_station: bool
    pickup_cooldown_minutes: int

    def __post_init__(self) -> None:
        """정책 식별자와 각 물리 파라미터 범위를 검증한다."""
        if type(self.version) is not str or not self.version.strip():
            raise ContractViolation("rebalance policy version은 nonblank여야 합니다.")
        if self.quantity_strategy not in _QUANTITY_STRATEGIES:
            raise ContractViolation("rebalance quantity strategy가 지원 범위 밖입니다.")
        if (
            type(self.protection_horizon_hours) is not int
            or not 1 <= self.protection_horizon_hours <= 12
        ):
            raise ContractViolation("protection horizon은 1..12시간이어야 합니다.")
        if (
            type(self.pickup_cooldown_minutes) is not int
            or not 0 <= self.pickup_cooldown_minutes <= 12 * 60
        ):
            raise ContractViolation("pickup cooldown은 0..720분이어야 합니다.")
        for value, name, maximum in (
            (self.minimum_stock_ratio, "minimum_stock_ratio", 0.5),
            (self.uncertainty_z, "uncertainty_z", 3.0),
        ):
            if (
                type(value) is not float
                or not math.isfinite(value)
                or not 0.0 <= value <= maximum
            ):
                raise ContractViolation(
                    f"{name}은 0..{maximum} finite float여야 합니다."
                )
        if type(self.exclusive_pickup_station) is not bool:
            raise ContractViolation("exclusive_pickup_station은 bool이어야 합니다.")
        if self.quantity_strategy == "legacy" and (
            self.protection_horizon_hours != 12
            or self.minimum_stock_ratio != 0.0
            or self.uncertainty_z != 0.0
            or self.exclusive_pickup_station
            or self.pickup_cooldown_minutes != 0
        ):
            raise ContractViolation(
                "legacy 정책은 기존 동작과 exact한 중립값이어야 합니다."
            )
        if (
            self.quantity_strategy == "risk_band"
            and self.pickup_cooldown_minutes < self.protection_horizon_hours * 60
        ):
            raise ContractViolation(
                "risk-band pickup cooldown은 protection horizon 이상이어야 합니다."
            )

    def audit_document(self) -> dict[str, Any]:
        """결과와 publication fingerprint에 기록할 canonical 정책 값을 반환한다."""
        return {
            "schema_version": REBALANCE_POLICY_CONFIG_SCHEMA_VERSION,
            "version": self.version,
            "quantity_strategy": self.quantity_strategy,
            "protection_horizon_hours": self.protection_horizon_hours,
            "minimum_stock_ratio": self.minimum_stock_ratio,
            "uncertainty_z": self.uncertainty_z,
            "uncertainty_scope": "pickup_only",
            "pickup_donor_guard": (
                PICKUP_DONOR_GUARD_NONE
                if self.quantity_strategy == "legacy"
                else PICKUP_DONOR_GUARD_CAPACITY_RESERVE_V1
            ),
            "exclusive_pickup_station": self.exclusive_pickup_station,
            "pickup_cooldown_minutes": self.pickup_cooldown_minutes,
        }

    @property
    def canonical_json(self) -> str:
        """Publication fingerprint에 넣을 정렬된 compact JSON을 반환한다."""
        return json.dumps(
            self.audit_document(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


LEGACY_REBALANCE_POLICY = RebalancePolicyConfig(
    version="rebalance-policy-v1",
    quantity_strategy="legacy",
    protection_horizon_hours=12,
    minimum_stock_ratio=0.0,
    uncertainty_z=0.0,
    exclusive_pickup_station=False,
    pickup_cooldown_minutes=0,
)
"""구 urgency-v1·route-v2 동작과 같은 비교 기준 정책이다."""


def risk_band_policy(
    *,
    protection_horizon_hours: int,
    minimum_stock_ratio: float,
    uncertainty_z: float,
    pickup_cooldown_minutes: int | None = None,
    exclusive_pickup_station: bool = True,
) -> RebalancePolicyConfig:
    """실험값을 식별자에 포함한 v5 정원보존 정책을 만든다."""
    resolved_cooldown = (
        protection_horizon_hours * 60
        if pickup_cooldown_minutes is None
        else pickup_cooldown_minutes
    )
    version = (
        "rebalance-risk-band-v5-capacity-reserve-"
        f"h{protection_horizon_hours}-"
        f"r{minimum_stock_ratio:.2f}-"
        f"z{uncertainty_z:.3f}-"
        f"cooldown{resolved_cooldown}-"
        f"exclusive{int(exclusive_pickup_station)}"
    )
    return RebalancePolicyConfig(
        version=version,
        quantity_strategy="risk_band",
        protection_horizon_hours=protection_horizon_hours,
        minimum_stock_ratio=float(minimum_stock_ratio),
        uncertainty_z=float(uncertainty_z),
        exclusive_pickup_station=exclusive_pickup_station,
        pickup_cooldown_minutes=resolved_cooldown,
    )


RISK_BAND_REBALANCE_POLICY_V5 = risk_band_policy(
    protection_horizon_hours=2,
    minimum_stock_ratio=0.2,
    uncertainty_z=1.645,
    pickup_cooldown_minutes=120,
)
"""현재와 보수적 미래 재고 모두 정원을 넘는 수량만 회수하는 정책이다."""


DEFAULT_REBALANCE_POLICY = RISK_BAND_REBALANCE_POLICY_V5
"""Gold urgency·route가 명시적으로 fingerprint하는 기본 재배치 정책이다."""
