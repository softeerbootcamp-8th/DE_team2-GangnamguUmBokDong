"""재배치 quantile 품질 게이트와 fallback 계약을 검증한다."""

from gold.rebalance_policy import (
    QUANTILE_POLICY_GATE_VERSION,
    decide_quantile_policy,
    parse_quantile_policy_decision,
)


def _metrics(coverage: float) -> bytes:
    """테스트용 calibrated P10~P90 coverage JSON bytes를 만든다."""
    return (
        '{"p10_p90_coverage_calibrated_test":' + str(coverage) + "}"
    ).encode()


def test_calibrated_pair_selects_quantile_guard_and_round_trips_audit() -> None:
    """두 모델이 목표 오차 범위면 quantile guard 선택을 canonical하게 고정한다."""
    decision = decide_quantile_policy(_metrics(0.81), _metrics(0.79))

    assert decision.version == QUANTILE_POLICY_GATE_VERSION
    assert decision.selected_strategy == "quantile_guard"
    assert decision.reasons == ()
    assert parse_quantile_policy_decision(decision.canonical_json) == decision


def test_temporary_model_coverage_selects_auditable_risk_band_fallback() -> None:
    """현재 임시 모델의 과도한 coverage는 검증된 risk-band로 fallback한다."""
    decision = decide_quantile_policy(
        _metrics(0.9069386401440077),
        _metrics(0.9129313629915786),
    )

    assert decision.selected_strategy == "risk_band"
    assert decision.reasons == (
        "rental_coverage_out_of_policy_range",
        "return_coverage_out_of_policy_range",
    )


def test_missing_or_invalid_metric_falls_back_without_breaking_pipeline() -> None:
    """결측·깨진 metrics는 추론 게시를 막지 않고 사유가 있는 fallback이 된다."""
    decision = decide_quantile_policy(b"{}", b"not-json")

    assert decision.selected_strategy == "risk_band"
    assert decision.rental_coverage is None
    assert decision.return_coverage is None
    assert decision.reasons == (
        "rental_calibration_metric_missing_or_invalid",
        "return_calibration_metric_missing_or_invalid",
    )
