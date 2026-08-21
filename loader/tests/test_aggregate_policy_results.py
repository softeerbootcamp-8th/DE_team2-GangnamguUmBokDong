"""여러 held-out 결과 집계가 계약 불일치와 날짜별 악화를 숨기지 않는지 검증한다."""

import pytest
from evaluation.aggregate_policy_results import aggregate_results


def _policy(name: str, requests: int, fulfilled: int, empty: float) -> dict:
    """집계 테스트용 최소 정책 결과를 만든다."""
    return {
        "policy": name,
        "observed_requests": requests,
        "fulfilled_requests": fulfilled,
        "unfulfilled_requests": requests - fulfilled,
        "observed_demand_fulfillment_rate": fulfilled / requests,
        "empty_station_minutes": empty,
        "moved_bikes": 1 if name != "no_rebalance" else 0,
        "dispatched_routes": 1 if name != "no_rebalance" else 0,
        "vehicle_busy_minutes": 10.0 if name != "no_rebalance" else 0.0,
        "planned_bikes": 1 if name != "no_rebalance" else 0,
        "movement_budget_used": 1 if name != "no_rebalance" else 0,
    }


def _document(target_date: str, baseline_fulfilled: int, model_fulfilled: int) -> dict:
    """계약이 같은 날짜별 최소 결과를 만든다."""
    return {
        "target_date": target_date,
        "model_bundle_sha256": "a" * 64,
        "evidence_gate": {
            "point_in_time_feature_inputs": True,
            "operation_contract_passed": True,
            "legacy_endpoint_reconciliation_passed": True,
            "heldout_day_of_month": True,
        },
        "contracts": [
            {
                "contract": {
                    "target_date": target_date,
                    "evaluation_minutes": 120,
                    "fleet_size": 3,
                }
            }
        ],
        "durations": [
            {
                "evaluation_minutes": 120,
                "legacy_movement": {
                    "balanced_movement_budget": 1,
                    "added_bikes": 1,
                    "removed_bikes": 1,
                },
                "legacy_timing": [
                    {
                        "empty_station_minutes": 90.0,
                        "negative_station_minutes": 0.0,
                        "endpoint_max_absolute_error": 0,
                    }
                ],
                "no_rebalance": _policy("no_rebalance", 100, baseline_fulfilled, 100.0),
                "model_policies": [
                    _policy("model_route_v2_max_stops_5", 100, model_fulfilled, 80.0)
                ],
            }
        ],
    }


def test_aggregate_reports_better_and_worse_dates_separately() -> None:
    """micro 평균만으로 날짜 하나의 서비스 악화를 가리지 않는다."""
    result = aggregate_results(
        (_document("2025-05-17", 98, 99), _document("2025-06-17", 100, 99))
    )
    row = next(
        row for row in result["rows"] if row["policy"] == "model_route_v2_max_stops_5"
    )
    assert row["dates_fulfillment_better"] == 1
    assert row["dates_fulfillment_worse"] == 1
    assert row["empty_station_minutes_change_vs_no_rebalance_pct"] == -20.0


def test_aggregate_rejects_resource_contract_mismatch() -> None:
    """날짜 사이 fleet가 다르면 같은 실험으로 집계하지 않는다."""
    first = _document("2025-05-17", 100, 100)
    second = _document("2025-06-17", 100, 100)
    second["contracts"][0]["contract"]["fleet_size"] = 2
    with pytest.raises(ValueError, match="운영 계약"):
        aggregate_results((first, second))
