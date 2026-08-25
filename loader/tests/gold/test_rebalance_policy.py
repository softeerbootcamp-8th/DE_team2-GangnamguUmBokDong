"""재배치 정책 v5 정원보존의 공급원 보호 불변조건을 검증한다."""

import pytest
from core.gold_publication import ContractViolation
from gold.rebalance_policy import (
    DEFAULT_REBALANCE_POLICY,
    LEGACY_REBALANCE_POLICY,
    PICKUP_DONOR_GUARD_CAPACITY_RESERVE_V1,
    PICKUP_DONOR_GUARD_NONE,
    REBALANCE_POLICY_CONFIG_SCHEMA_VERSION,
    RISK_BAND_REBALANCE_POLICY_V5,
    risk_band_policy,
)


def test_risk_band_default_cooldown_matches_protection_horizon() -> None:
    """Cooldown 미지정 시 보호 horizon 전체를 자동 적용한다."""
    policy = risk_band_policy(
        protection_horizon_hours=3,
        minimum_stock_ratio=0.2,
        uncertainty_z=1.645,
    )

    assert policy.pickup_cooldown_minutes == 180
    assert "cooldown180" in policy.version


def test_risk_band_rejects_cooldown_shorter_than_protection_horizon() -> None:
    """명시 cooldown이 보호 horizon보다 짧으면 정책 생성을 fail-closed한다."""
    with pytest.raises(ContractViolation, match="cooldown.*protection horizon"):
        risk_band_policy(
            protection_horizon_hours=2,
            minimum_stock_ratio=0.2,
            uncertainty_z=1.645,
            pickup_cooldown_minutes=119,
        )


def test_default_risk_band_policy_is_v5_capacity_reserve_candidate() -> None:
    """Gold 기본 정책을 정원보존 후보의 exact 파라미터로 고정한다."""
    assert DEFAULT_REBALANCE_POLICY is RISK_BAND_REBALANCE_POLICY_V5
    assert DEFAULT_REBALANCE_POLICY.version == (
        "rebalance-risk-band-v5-capacity-reserve-h2-r0.20-z1.645-"
        "cooldown120-exclusive1"
    )
    assert DEFAULT_REBALANCE_POLICY.protection_horizon_hours == 2
    assert DEFAULT_REBALANCE_POLICY.minimum_stock_ratio == 0.2
    assert DEFAULT_REBALANCE_POLICY.uncertainty_z == 1.645
    assert DEFAULT_REBALANCE_POLICY.pickup_cooldown_minutes == 120
    audit = DEFAULT_REBALANCE_POLICY.audit_document()
    assert audit["schema_version"] == REBALANCE_POLICY_CONFIG_SCHEMA_VERSION
    assert audit["pickup_donor_guard"] == (
        PICKUP_DONOR_GUARD_CAPACITY_RESERVE_V1
    )


def test_legacy_policy_audits_no_pickup_donor_guard() -> None:
    """Legacy 비교군은 정원보존 동작을 적용하지 않았음을 기록한다."""
    assert (
        LEGACY_REBALANCE_POLICY.audit_document()["pickup_donor_guard"]
        == PICKUP_DONOR_GUARD_NONE
    )
