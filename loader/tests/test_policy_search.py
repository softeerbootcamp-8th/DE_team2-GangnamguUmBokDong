"""재배치 위험 구간 정책 search 후보 생성 계약을 검증한다."""

from evaluation.run_policy_search import build_policy_variants


def test_policy_search_grid_is_deterministic_and_named_by_parameters() -> None:
    """제한된 grid의 모든 조합과 legacy 기준선을 고유 이름으로 만든다."""
    variants = build_policy_variants(
        protection_hours=(2, 3),
        minimum_stock_ratios=(0.2,),
        uncertainty_values=(0.0, 1.282),
        max_pickup_stock_fractions=(1.0,),
        pickup_cooldown_minutes=(None,),
        max_stops=(5,),
        include_legacy=True,
    )
    assert tuple(variant.name for variant in variants) == (
        "legacy_s5",
        "risk_h2_r20_z0000_f10000bp_cd120_s5",
        "risk_h2_r20_z1282_f10000bp_cd120_s5",
        "risk_h3_r20_z0000_f10000bp_cd180_s5",
        "risk_h3_r20_z1282_f10000bp_cd180_s5",
    )
    assert all(
        variant.policy_config.exclusive_pickup_station for variant in variants[1:]
    )


def test_policy_search_names_sub_percent_fraction_candidates_uniquely() -> None:
    """Search 이름은 basis-point 단위 fraction을 빠짐없이 구분한다."""
    variants = build_policy_variants(
        protection_hours=(2,),
        minimum_stock_ratios=(0.2,),
        uncertainty_values=(1.645,),
        max_pickup_stock_fractions=(0.01, 0.015, 0.02),
        pickup_cooldown_minutes=(None,),
        max_stops=(5,),
        include_legacy=False,
    )

    assert tuple(variant.name for variant in variants) == (
        "risk_h2_r20_z1645_f0100bp_cd120_s5",
        "risk_h2_r20_z1645_f0150bp_cd120_s5",
        "risk_h2_r20_z1645_f0200bp_cd120_s5",
    )
    assert len({variant.policy_config.version for variant in variants}) == 3
