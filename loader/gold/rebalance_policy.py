"""예측 이후 재배치 수량·배차 안전성의 버전 고정 정책을 정의한다."""

from __future__ import annotations

import json
import math
from dataclasses import dataclass
from typing import Any

from core.gold_publication import ContractViolation

_QUANTITY_STRATEGIES = frozenset({"legacy", "risk_band"})
QUANTILE_POLICY_TARGET_COVERAGE = 0.8
QUANTILE_POLICY_MAX_COVERAGE_ERROR = 0.05
QUANTILE_POLICY_GATE_VERSION = "rebalance-quantile-quality-gate-v1"
QUANTILE_GUARD_VERSION = "rebalance-quantile-guard-v1"


@dataclass(frozen=True, slots=True)
class QuantilePolicyDecision:
    """모델 calibration 근거로 quantile 수량 사용 여부를 고정한다."""

    version: str
    selected_strategy: str
    fallback_strategy: str
    target_coverage: float
    max_coverage_error: float
    rental_coverage: float | None
    return_coverage: float | None
    reasons: tuple[str, ...]

    def __post_init__(self) -> None:
        """결정 문서의 버전·전략·수치·사유를 검증한다."""
        if self.version != QUANTILE_POLICY_GATE_VERSION:
            raise ContractViolation("quantile policy gate version이 다릅니다.")
        if self.selected_strategy not in {"quantile_guard", "risk_band"}:
            raise ContractViolation("quantile selected strategy가 잘못됐습니다.")
        if self.fallback_strategy != "risk_band":
            raise ContractViolation("quantile fallback strategy는 risk_band여야 합니다.")
        for value, name in (
            (self.target_coverage, "target_coverage"),
            (self.max_coverage_error, "max_coverage_error"),
        ):
            if type(value) is not float or not math.isfinite(value) or not 0.0 < value < 1.0:
                raise ContractViolation(f"{name}은 0..1 finite float여야 합니다.")
        for value, name in (
            (self.rental_coverage, "rental_coverage"),
            (self.return_coverage, "return_coverage"),
        ):
            if value is not None and (
                type(value) is not float
                or not math.isfinite(value)
                or not 0.0 <= value <= 1.0
            ):
                raise ContractViolation(f"{name}은 null 또는 0..1 finite float여야 합니다.")
        if type(self.reasons) is not tuple or any(
            type(reason) is not str or not reason for reason in self.reasons
        ):
            raise ContractViolation("quantile policy reasons는 nonblank string tuple이어야 합니다.")
        if (self.selected_strategy == "quantile_guard") != (not self.reasons):
            raise ContractViolation("quantile strategy와 fallback reason이 모순됩니다.")

    def audit_document(self) -> dict[str, Any]:
        """Publication fingerprint에 남길 canonical 판단 근거를 반환한다."""
        return {
            "fallback_strategy": self.fallback_strategy,
            "quantile_guard": {
                "horizon_hours": 2,
                "minimum_bikes": 1,
                "minimum_empty_docks": 1,
                "q50_role": "preserved_for_diagnostics",
                "target_quantity": "validated_risk_band_quantity",
                "version": QUANTILE_GUARD_VERSION,
            },
            "max_coverage_error": self.max_coverage_error,
            "reasons": list(self.reasons),
            "rental_coverage": self.rental_coverage,
            "return_coverage": self.return_coverage,
            "selected_strategy": self.selected_strategy,
            "target_coverage": self.target_coverage,
            "version": self.version,
        }

    @property
    def canonical_json(self) -> str:
        """판단 근거를 정렬된 compact JSON 문자열로 반환한다."""
        return json.dumps(
            self.audit_document(),
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )


def decide_quantile_policy(
    rental_metrics_payload: bytes,
    return_metrics_payload: bytes,
) -> QuantilePolicyDecision:
    """두 모델의 pinned test coverage가 의사결정 기준을 통과하는지 판정한다."""
    coverages: dict[str, float | None] = {}
    reasons: list[str] = []
    for model_name, payload in (
        ("rental", rental_metrics_payload),
        ("return", return_metrics_payload),
    ):
        coverage: float | None = None
        try:
            document = json.loads(payload)
            raw = document["p10_p90_coverage_calibrated_test"]
            if type(raw) not in {int, float} or type(raw) is bool:
                raise ValueError
            coverage = float(raw)
            if not math.isfinite(coverage) or not 0.0 <= coverage <= 1.0:
                raise ValueError
        except (KeyError, TypeError, ValueError, json.JSONDecodeError, UnicodeDecodeError):
            reasons.append(f"{model_name}_calibration_metric_missing_or_invalid")
        else:
            if (
                abs(coverage - QUANTILE_POLICY_TARGET_COVERAGE)
                > QUANTILE_POLICY_MAX_COVERAGE_ERROR
            ):
                reasons.append(f"{model_name}_coverage_out_of_policy_range")
        coverages[model_name] = coverage
    return QuantilePolicyDecision(
        version=QUANTILE_POLICY_GATE_VERSION,
        selected_strategy="risk_band" if reasons else "quantile_guard",
        fallback_strategy="risk_band",
        target_coverage=QUANTILE_POLICY_TARGET_COVERAGE,
        max_coverage_error=QUANTILE_POLICY_MAX_COVERAGE_ERROR,
        rental_coverage=coverages["rental"],
        return_coverage=coverages["return"],
        reasons=tuple(reasons),
    )


def parse_quantile_policy_decision(value: str) -> QuantilePolicyDecision:
    """Fingerprint의 canonical quantile 판단 JSON을 typed 값으로 복원한다."""
    try:
        document = json.loads(value)
        decision = QuantilePolicyDecision(
            version=document["version"],
            selected_strategy=document["selected_strategy"],
            fallback_strategy=document["fallback_strategy"],
            target_coverage=document["target_coverage"],
            max_coverage_error=document["max_coverage_error"],
            rental_coverage=document["rental_coverage"],
            return_coverage=document["return_coverage"],
            reasons=tuple(document["reasons"]),
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ContractViolation("quantile policy decision JSON이 잘못됐습니다.") from exc
    if decision.canonical_json != value:
        raise ContractViolation("quantile policy decision은 canonical JSON이어야 합니다.")
    return decision


def fallback_quantile_policy(
    decision: QuantilePolicyDecision,
    reason: str,
) -> QuantilePolicyDecision:
    """기존 calibration 감사값을 보존하며 추가 결측 사유로 fallback한다."""
    if type(decision) is not QuantilePolicyDecision:
        raise ContractViolation("quantile fallback 원본 decision 타입이 잘못됐습니다.")
    if type(reason) is not str or not reason:
        raise ContractViolation("quantile fallback reason은 nonblank여야 합니다.")
    reasons = decision.reasons
    if reason not in reasons:
        reasons = (*reasons, reason)
    return QuantilePolicyDecision(
        version=decision.version,
        selected_strategy="risk_band",
        fallback_strategy=decision.fallback_strategy,
        target_coverage=decision.target_coverage,
        max_coverage_error=decision.max_coverage_error,
        rental_coverage=decision.rental_coverage,
        return_coverage=decision.return_coverage,
        reasons=reasons,
    )


FALLBACK_QUANTILE_POLICY_DECISION = QuantilePolicyDecision(
    version=QUANTILE_POLICY_GATE_VERSION,
    selected_strategy="risk_band",
    fallback_strategy="risk_band",
    target_coverage=QUANTILE_POLICY_TARGET_COVERAGE,
    max_coverage_error=QUANTILE_POLICY_MAX_COVERAGE_ERROR,
    rental_coverage=None,
    return_coverage=None,
    reasons=("quantile_decision_not_supplied",),
)


@dataclass(frozen=True, slots=True)
class RebalancePolicyConfig:
    """긴급도 수량과 공급원 동시 배차에 적용할 설명 가능한 정책을 표현한다."""

    version: str
    quantity_strategy: str
    protection_horizon_hours: int
    minimum_stock_ratio: float
    uncertainty_z: float
    max_pickup_stock_fraction: float
    exclusive_pickup_station: bool
    pickup_cooldown_minutes: int
    execution_reserve_ratio: float

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
            (
                self.max_pickup_stock_fraction,
                "max_pickup_stock_fraction",
                1.0,
            ),
            (self.execution_reserve_ratio, "execution_reserve_ratio", 0.5),
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
            or self.max_pickup_stock_fraction != 1.0
            or self.exclusive_pickup_station
            or self.pickup_cooldown_minutes != 0
            or self.execution_reserve_ratio != 0.0
        ):
            raise ContractViolation(
                "legacy 정책은 기존 동작과 exact한 중립값이어야 합니다."
            )
        if (
            self.quantity_strategy == "risk_band"
            and self.execution_reserve_ratio < self.minimum_stock_ratio
        ):
            raise ContractViolation(
                "실행 reserve는 계획 minimum stock보다 작을 수 없습니다."
            )

    def audit_document(self) -> dict[str, Any]:
        """결과와 publication fingerprint에 기록할 canonical 정책 값을 반환한다."""
        return {
            "version": self.version,
            "quantity_strategy": self.quantity_strategy,
            "protection_horizon_hours": self.protection_horizon_hours,
            "minimum_stock_ratio": self.minimum_stock_ratio,
            "uncertainty_z": self.uncertainty_z,
            "uncertainty_scope": "pickup_only",
            "max_pickup_stock_fraction": self.max_pickup_stock_fraction,
            "exclusive_pickup_station": self.exclusive_pickup_station,
            "pickup_cooldown_minutes": self.pickup_cooldown_minutes,
            "execution_reserve_ratio": self.execution_reserve_ratio,
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
    max_pickup_stock_fraction=1.0,
    exclusive_pickup_station=False,
    pickup_cooldown_minutes=0,
    execution_reserve_ratio=0.0,
)
"""현행 urgency-v1·route-v2와 byte 의미가 같은 비교 기준 정책이다."""


def risk_band_policy(
    *,
    protection_horizon_hours: int,
    minimum_stock_ratio: float,
    uncertainty_z: float,
    max_pickup_stock_fraction: float = 1.0,
    pickup_cooldown_minutes: int = 0,
    exclusive_pickup_station: bool = True,
) -> RebalancePolicyConfig:
    """실험값을 식별자에 포함한 위험 구간 재고 정책을 만든다."""
    version = (
        "rebalance-risk-band-"
        f"h{protection_horizon_hours}-"
        f"r{minimum_stock_ratio:.2f}-"
        f"z{uncertainty_z:.3f}-"
        f"f{max_pickup_stock_fraction:.2f}-"
        f"cooldown{pickup_cooldown_minutes}-"
        f"exclusive{int(exclusive_pickup_station)}"
    )
    return RebalancePolicyConfig(
        version=version,
        quantity_strategy="risk_band",
        protection_horizon_hours=protection_horizon_hours,
        minimum_stock_ratio=float(minimum_stock_ratio),
        uncertainty_z=float(uncertainty_z),
        max_pickup_stock_fraction=float(max_pickup_stock_fraction),
        exclusive_pickup_station=exclusive_pickup_station,
        pickup_cooldown_minutes=pickup_cooldown_minutes,
        execution_reserve_ratio=float(minimum_stock_ratio),
    )


RISK_BAND_REBALANCE_POLICY_V2 = risk_band_policy(
    protection_horizon_hours=2,
    minimum_stock_ratio=0.2,
    uncertainty_z=0.0,
    max_pickup_stock_fraction=0.15,
    pickup_cooldown_minutes=60,
)
"""2025년 교정 2일·독립 검증 8일을 통과한 보수적 production 정책이다."""


DEFAULT_REBALANCE_POLICY = RISK_BAND_REBALANCE_POLICY_V2
"""Gold urgency·route가 명시적으로 fingerprint하는 기본 재배치 정책이다."""
