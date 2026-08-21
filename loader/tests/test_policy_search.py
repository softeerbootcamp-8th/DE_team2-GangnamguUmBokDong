"""재배치 위험 구간 정책 search 후보 생성 계약을 검증한다."""

from evaluation.run_policy_search import build_policy_variants


def test_policy_search_grid_is_deterministic_and_named_by_parameters() -> None:
    """제한된 grid의 모든 조합과 legacy 기준선을 고유 이름으로 만든다."""
    variants = build_policy_variants(
        protection_hours=(2, 3),
        minimum_stock_ratios=(0.2,),
        uncertainty_values=(0.0, 1.282),
        max_pickup_stock_fractions=(1.0,),
        pickup_cooldown_minutes=(0,),
        max_stops=(5,),
        include_legacy=True,
    )
    assert tuple(variant.name for variant in variants) == (
        "legacy_s5",
        "risk_h2_r20_z0000_f100_cd000_s5",
        "risk_h2_r20_z1282_f100_cd000_s5",
        "risk_h3_r20_z0000_f100_cd000_s5",
        "risk_h3_r20_z1282_f100_cd000_s5",
    )
    assert all(
        variant.policy_config.exclusive_pickup_station for variant in variants[1:]
    )
