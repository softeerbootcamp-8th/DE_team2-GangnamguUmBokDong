"""단일 profile 평가기와 공통 결과 schema를 검증한다."""

from __future__ import annotations

import json
from copy import deepcopy
from datetime import date

import pytest
from evaluation.backtest_contract import EvaluationContract
from evaluation.evaluation_profiles import (
    CALIBRATION_PROFILE,
    CONFIRMATORY_PROFILE,
    PRODUCTION_MODEL_BUNDLE_SHA256,
    PRODUCTION_POLICY_NAME,
    PRODUCTION_PROFILE,
    EvaluationCell,
    EvaluationGate,
    EvaluationProfile,
    get_evaluation_profile,
    production_policy_configuration,
)
from evaluation.profile_evaluation import (
    EVALUATION_RESULT_SCHEMA_VERSION,
    evaluate_profile,
    load_backtest_result,
    result_markdown,
)


def _source_file(name: str) -> dict[str, object]:
    """합성 raw 결과에 넣을 source fingerprint를 만든다."""
    return {
        "path": f"/data/{name}.csv",
        "size_bytes": 10,
        "sha256": "a" * 64,
    }


def _policy(
    *,
    policy: str,
    requests: int,
    unfulfilled: int,
    empty_minutes: float,
    moved_bikes: int,
) -> dict[str, object]:
    """공통 평가기가 소비하는 최소 SimulationMetrics 문서를 만든다."""
    failures = [
        {
            "bike_id": f"bike-{index}",
            "rented_at": f"2025-03-17T06:{10 + index:02d}:00+09:00",
            "station_no": 100 + index,
        }
        for index in range(unfulfilled)
    ]
    return {
        "policy": policy,
        "policy_configuration": production_policy_configuration(),
        "observed_requests": requests,
        "fulfilled_requests": requests - unfulfilled,
        "unfulfilled_requests": unfulfilled,
        "observed_demand_fulfillment_rate": (requests - unfulfilled) / requests,
        "empty_station_minutes": empty_minutes,
        "planned_bikes": moved_bikes,
        "moved_bikes": moved_bikes,
        "dispatched_routes": 1 if moved_bikes else 0,
        "completed_routes_by_cutoff": 1 if moved_bikes else 0,
        "trucks_still_busy_at_cutoff": 0,
        "unfulfilled_request_log": failures,
        "job_audits": (
            [
                {
                    "dispatched_at": "2025-03-17T06:05:00+09:00",
                    "stops": [
                        {
                            "action": "pickup",
                            "executed_at": "2025-03-17T06:15:00+09:00",
                        }
                    ],
                }
            ]
            if moved_bikes
            else []
        ),
    }


def _document(cell: EvaluationCell) -> dict[str, object]:
    """세 horizon을 가진 합성 point-in-time 결과를 만든다."""
    contracts = [
        EvaluationContract(
            target_date=cell.target_date,
            start_hour=cell.start_hour,
            evaluation_minutes=minutes,
            fleet_size=3,
        ).audit_document()
        for minutes in (60, 120, 180)
    ]
    durations = []
    for minutes in (60, 120, 180):
        durations.append(
            {
                "evaluation_minutes": minutes,
                "station_count": 20,
                "legacy_movement": {"balanced_movement_budget": 10},
                "legacy_timing": [
                    {"empty_station_minutes": 95.0},
                    {"empty_station_minutes": 100.0},
                    {"empty_station_minutes": 105.0},
                ],
                "no_rebalance": _policy(
                    policy="no_rebalance",
                    requests=100,
                    unfulfilled=1 if minutes == 180 else 0,
                    empty_minutes=100.0,
                    moved_bikes=0,
                ),
                "model_policies": [
                    _policy(
                        policy=PRODUCTION_POLICY_NAME,
                        requests=100,
                        unfulfilled=0,
                        empty_minutes=90.0,
                        moved_bikes=5,
                    )
                ],
            }
        )
    return {
        "target_date": cell.target_date.isoformat(),
        "center_id": cell.center_id,
        "center_name": "테스트 센터",
        "start_hour": cell.start_hour,
        "model_bundle_root": "/models/test",
        "model_bundle_sha256": PRODUCTION_MODEL_BUNDLE_SHA256,
        "evidence_gate": {
            "point_in_time_feature_inputs": True,
            "operation_contract_passed": True,
            "legacy_endpoint_reconciliation_passed": True,
            "heldout_day_of_month": True,
        },
        "contracts": contracts,
        "source_provenance": {
            "backtest_contract_version": "point-in-time-policy-backtest-v3",
            "route_algorithm_version": "route-v4-supply-led-pickup-sla",
            "urgency_scoring_config_version": "urgency-scoring-v5-capacity-reserve",
            "rental_csv": _source_file("rental"),
            "stock_csv": _source_file("stock"),
            "weather_csv": _source_file("weather"),
            "population_csvs": [_source_file("population")],
            "station_master_content_sha256": "b" * 64,
            "station_crosswalk_count": 20,
            "station_crosswalk_sha256": "c" * 64,
            "population_excluded_station_count": 0,
            "population_excluded_grid_ids": [],
        },
        "durations": durations,
    }


@pytest.fixture
def profile() -> EvaluationProfile:
    """단일 셀 진단 profile을 반환한다."""
    return EvaluationProfile(
        name="calibration",
        purpose="합성 평가",
        cells=(EvaluationCell("test-center", date(2025, 3, 17), 6),),
        gate=EvaluationGate(
            require_aggregate_180_unfulfilled_strict_improvement=True,
            strict_empty_improvement_horizons=(60, 120, 180),
            max_pickup_dispatch_lag_minutes=30.0,
            require_planned_bikes_equal_moved_bikes=True,
            require_all_routes_finished_by_cutoff=True,
        ),
        release_gate=False,
    )


def test_profiles_only_change_data_and_gate() -> None:
    """세 공식 목적은 같은 profile 타입과 공통 horizon을 사용한다."""
    assert get_evaluation_profile("calibration") is CALIBRATION_PROFILE
    assert get_evaluation_profile("confirmatory") is CONFIRMATORY_PROFILE
    assert get_evaluation_profile("production") is PRODUCTION_PROFILE
    assert {len(profile.cells) for profile in (CALIBRATION_PROFILE, CONFIRMATORY_PROFILE)} == {12}
    assert len(PRODUCTION_PROFILE.cells) == 10
    assert {
        profile.evaluation_minutes
        for profile in (CALIBRATION_PROFILE, CONFIRMATORY_PROFILE, PRODUCTION_PROFILE)
    } == {(60, 120, 180)}


def test_evaluate_profile_uses_one_schema_and_gate(profile: EvaluationProfile) -> None:
    """공통 집계는 고객·품절·기존 운영 지표를 한 결과로 판정한다."""
    document = _document(profile.cells[0])

    result = evaluate_profile(profile, [document])

    assert result["schema_version"] == EVALUATION_RESULT_SCHEMA_VERSION
    assert result["acceptance_gate"]["passed"] is True
    aggregate = result["aggregates"][2]
    assert aggregate["baseline_unfulfilled_requests"] == 1
    assert aggregate["candidate_unfulfilled_requests"] == 0
    assert aggregate["empty_station_minutes_reduction_pct"] == 10.0
    assert aggregate["legacy_empty_station_minutes_min"] == 95.0
    assert "Gate 통과: **True**" in result_markdown(result)


def test_evaluate_profile_reports_request_displacement_diagnostic(
    profile: EvaluationProfile,
) -> None:
    """후보에만 생긴 실패 요청은 release를 막지 않고 진단값으로 공개한다."""
    document = _document(profile.cells[0])
    baseline = document["durations"][0]["no_rebalance"]
    baseline.update(_policy(
        policy="no_rebalance",
        requests=100,
        unfulfilled=1,
        empty_minutes=100.0,
        moved_bikes=0,
    ))
    candidate = document["durations"][0]["model_policies"][0]
    candidate.update(_policy(
        policy=PRODUCTION_POLICY_NAME,
        requests=100,
        unfulfilled=1,
        empty_minutes=90.0,
        moved_bikes=5,
    ))
    candidate["unfulfilled_request_log"][0]["bike_id"] = "new-failure"

    result = evaluate_profile(profile, [document])

    assert result["acceptance_gate"]["passed"] is True
    assert result["acceptance_gate"]["diagnostics"][
        "every_cell_and_duration_new_unfulfilled_request_set_empty"
    ] is False


def test_evaluate_profile_requires_exact_cells(profile: EvaluationProfile) -> None:
    """Profile 밖 셀과 누락 셀을 섞은 결과는 집계하지 않는다."""
    unexpected = deepcopy(_document(profile.cells[0]))
    unexpected["center_id"] = "unexpected"
    with pytest.raises(ValueError, match="profile에 없는"):
        evaluate_profile(profile, [unexpected])
    with pytest.raises(ValueError, match="결과가 없습니다"):
        evaluate_profile(profile, [])


def test_load_backtest_result_accepts_legacy_envelope(tmp_path) -> None:
    """이관 검증을 위해 과거 confirmatory envelope의 result만 읽는다."""
    path = tmp_path / "legacy.json"
    path.write_text(
        json.dumps({"candidate_id": "old", "result": {"target_date": "2025-03-17"}}),
        encoding="utf-8",
    )

    assert load_backtest_result(path) == {"target_date": "2025-03-17"}
