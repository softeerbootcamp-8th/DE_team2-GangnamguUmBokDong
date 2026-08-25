"""여러 held-out 날짜의 정책 백테스트를 같은 계약으로 검증하고 집계한다."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import defaultdict
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from gold.rebalance_route import ROUTE_ALGORITHM_VERSION

from .backtest_contract import BACKTEST_CONTRACT_VERSION, PRIMARY_METRIC
from .production_policy_contract import (
    PRODUCTION_EVALUATION_MINUTES,
    PRODUCTION_POLICY_NAME,
    production_evidence_scope,
    production_policy_configuration,
)

SUITE_SCHEMA_VERSION = "point-in-time-policy-suite-v3"
ACCEPTANCE_GATE_VERSION = "production-policy-acceptance-gate-v3"
CANDIDATE_GATE_VERSION = "policy-candidate-gate-v2"
_BASELINE_POLICY = "no_rebalance"
_FLOAT_TOLERANCE = 1e-9
_SEOUL = ZoneInfo("Asia/Seoul")
_UNFULFILLED_REQUEST_KEYS = frozenset({"bike_id", "rented_at", "station_no"})
_REQUIRED_EVIDENCE_GATES = (
    "point_in_time_feature_inputs",
    "operation_contract_passed",
    "legacy_endpoint_reconciliation_passed",
    "heldout_day_of_month",
)
_PROVENANCE_VERSION_FIELDS = frozenset(
    {
        "backtest_contract_version",
        "route_algorithm_version",
        "urgency_scoring_config_version",
    }
)
_ACCUMULATED_METRICS = (
    "observed_requests",
    "fulfilled_requests",
    "unfulfilled_requests",
    "empty_station_minutes",
    "moved_bikes",
    "dispatched_routes",
    "vehicle_busy_minutes",
    "planned_bikes",
    "movement_budget_used",
)


def aggregate_results(documents: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """계약과 모델이 같은 날짜별 결과를 micro-average 지표로 집계한다."""
    validation = _validate_documents(documents)
    dates = validation["dates"]
    model_hash = validation["model_bundle_sha256"]

    accumulators: dict[tuple[int, str], dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    legacy_accumulators: dict[int, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    per_date: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
    for document in documents:
        for duration in document["durations"]:
            minutes = int(duration["evaluation_minutes"])
            legacy = duration["legacy_movement"]
            legacy_timing = duration["legacy_timing"]
            legacy_values = [row["empty_station_minutes"] for row in legacy_timing]
            legacy_negative = [row["negative_station_minutes"] for row in legacy_timing]
            legacy_accumulators[minutes]["movement_budget"] += legacy[
                "balanced_movement_budget"
            ]
            legacy_accumulators[minutes]["added_bikes"] += legacy["added_bikes"]
            legacy_accumulators[minutes]["removed_bikes"] += legacy["removed_bikes"]
            legacy_accumulators[minutes]["empty_min"] += min(legacy_values)
            legacy_accumulators[minutes]["empty_max"] += max(legacy_values)
            legacy_accumulators[minutes]["negative_min"] += min(legacy_negative)
            legacy_accumulators[minutes]["negative_max"] += max(legacy_negative)
            legacy_accumulators[minutes]["endpoint_max_error"] = max(
                legacy_accumulators[minutes]["endpoint_max_error"],
                max(row["endpoint_max_absolute_error"] for row in legacy_timing),
            )
            policies = [duration["no_rebalance"], *duration["model_policies"]]
            baseline = duration["no_rebalance"]
            window_start, window_end = _expected_policy_window(
                document["target_date"],
                document["start_hour"],
                minutes,
            )
            baseline_failures = _unfulfilled_event_keys(
                baseline,
                f"{document['target_date']} {minutes}분 no_rebalance",
                window_start=window_start,
                window_end=window_end,
            )
            for policy in policies:
                key = (minutes, policy["policy"])
                accumulator = accumulators[key]
                for name in _ACCUMULATED_METRICS:
                    accumulator[name] += float(policy[name])
                policy_failures = _unfulfilled_event_keys(
                    policy,
                    f"{document['target_date']} {minutes}분 {policy['policy']}",
                    window_start=window_start,
                    window_end=window_end,
                )
                new_failures = policy_failures - baseline_failures
                resolved_failures = baseline_failures - policy_failures
                per_date[key].append(
                    {
                        "date": document["target_date"],
                        "fulfillment_delta": (
                            policy["observed_demand_fulfillment_rate"]
                            - baseline["observed_demand_fulfillment_rate"]
                        ),
                        "unfulfilled_delta": (
                            policy["unfulfilled_requests"]
                            - baseline["unfulfilled_requests"]
                        ),
                        "empty_station_minutes_delta": (
                            policy["empty_station_minutes"]
                            - baseline["empty_station_minutes"]
                        ),
                        "new_unfulfilled_request_count": len(new_failures),
                        "new_unfulfilled_request_keys": [
                            _unfulfilled_event_document(event)
                            for event in sorted(
                                new_failures,
                                key=_unfulfilled_event_sort_key,
                            )
                        ],
                        "resolved_unfulfilled_request_count": len(
                            resolved_failures
                        ),
                        "resolved_unfulfilled_request_keys": [
                            _unfulfilled_event_document(event)
                            for event in sorted(
                                resolved_failures,
                                key=_unfulfilled_event_sort_key,
                            )
                        ],
                    }
                )
    legacy_summaries = [
        {
            "evaluation_minutes": minutes,
            "balanced_movement_budget": int(values["movement_budget"]),
            "added_bikes": int(values["added_bikes"]),
            "removed_bikes": int(values["removed_bikes"]),
            "empty_station_minutes_min": round(values["empty_min"], 3),
            "empty_station_minutes_max": round(values["empty_max"], 3),
            "negative_station_minutes_min": round(values["negative_min"], 3),
            "negative_station_minutes_max": round(values["negative_max"], 3),
            "endpoint_max_absolute_error": int(values["endpoint_max_error"]),
        }
        for minutes, values in sorted(legacy_accumulators.items())
    ]
    legacy_by_minutes = {
        row["evaluation_minutes"]: row for row in legacy_summaries
    }
    rows = []
    for (minutes, policy), values in sorted(accumulators.items()):
        baseline = accumulators[(minutes, _BASELINE_POLICY)]
        requests = int(values["observed_requests"])
        fulfilled = int(values["fulfilled_requests"])
        comparisons = per_date[(minutes, policy)]
        legacy = legacy_by_minutes[minutes]
        legacy_low = legacy["empty_station_minutes_min"]
        legacy_high = legacy["empty_station_minutes_max"]
        rows.append(
            {
                "evaluation_minutes": minutes,
                "policy": policy,
                "date_count": len(comparisons),
                "observed_requests": requests,
                "fulfilled_requests": fulfilled,
                "unfulfilled_requests": int(values["unfulfilled_requests"]),
                "observed_demand_fulfillment_rate": (
                    fulfilled / requests if requests else 1.0
                ),
                "empty_station_minutes": round(values["empty_station_minutes"], 3),
                "empty_station_minutes_change_vs_no_rebalance_pct": (
                    None
                    if policy == _BASELINE_POLICY
                    or baseline["empty_station_minutes"] == 0
                    else round(
                        (values["empty_station_minutes"] - baseline["empty_station_minutes"])
                        / baseline["empty_station_minutes"]
                        * 100.0,
                        3,
                    )
                ),
                "unfulfilled_change_vs_no_rebalance": (
                    int(values["unfulfilled_requests"] - baseline["unfulfilled_requests"])
                ),
                "new_unfulfilled_request_count": sum(
                    row["new_unfulfilled_request_count"] for row in comparisons
                ),
                "resolved_unfulfilled_request_count": sum(
                    row["resolved_unfulfilled_request_count"] for row in comparisons
                ),
                "dates_fulfillment_better": sum(
                    row["fulfillment_delta"] > 1e-12 for row in comparisons
                ),
                "dates_fulfillment_equal": sum(
                    abs(row["fulfillment_delta"]) <= 1e-12 for row in comparisons
                ),
                "dates_fulfillment_worse": sum(
                    row["fulfillment_delta"] < -1e-12 for row in comparisons
                ),
                "moved_bikes": int(values["moved_bikes"]),
                "dispatched_routes": int(values["dispatched_routes"]),
                "vehicle_busy_minutes": round(values["vehicle_busy_minutes"], 3),
                "planned_bikes": int(values["planned_bikes"]),
                "movement_budget_used": int(values["movement_budget_used"]),
                "legacy_movement_budget_cap": legacy["balanced_movement_budget"],
                "empty_change_vs_legacy_timing_range_pct": (
                    None
                    if policy == _BASELINE_POLICY
                    or legacy_low == 0
                    or legacy_high == 0
                    else [
                        round(
                            (values["empty_station_minutes"] - legacy_high)
                            / legacy_high
                            * 100.0,
                            3,
                        ),
                        round(
                            (values["empty_station_minutes"] - legacy_low)
                            / legacy_low
                            * 100.0,
                            3,
                        ),
                    ]
                ),
                "per_date_comparison": comparisons,
            }
        )
    result = {
        "schema_version": SUITE_SCHEMA_VERSION,
        "primary_metric": PRIMARY_METRIC,
        "dates": sorted(dates),
        "center_id": validation["center_id"],
        "start_hour": validation["start_hour"],
        "operation_contract": validation["operation_contract"],
        "model_bundle_sha256": model_hash,
        "input_provenance_sha256_by_date": validation[
            "input_provenance_sha256_by_date"
        ],
        "station_surface_sha256_by_date": validation[
            "station_surface_sha256_by_date"
        ],
        "station_count_by_date": validation["station_count_by_date"],
        "source_contract": validation["source_contract"],
        "source_provenance_by_date": validation["source_provenance_by_date"],
        "backtest_contract_version": BACKTEST_CONTRACT_VERSION,
        "route_algorithm_version": ROUTE_ALGORITHM_VERSION,
        "urgency_scoring_config_version": URGENCY_SCORING_CONFIG_VERSION,
        "evaluation_minutes": validation["evaluation_minutes"],
        "policy_configurations": validation["policy_configurations"],
        "result_count": len(documents),
        "publication_grade_system_claim_allowed": False,
        "legacy_summaries": legacy_summaries,
        "rows": rows,
    }
    result["acceptance_gate"] = evaluate_acceptance_gate(result)
    result["candidate_gate"] = evaluate_candidate_gate(result["acceptance_gate"])
    return result


def _validate_documents(documents: Sequence[dict[str, Any]]) -> dict[str, Any]:
    """집계 전 provenance·운영 계약·정책 행의 완전성과 parity를 강제한다."""
    if not documents:
        raise ValueError("집계할 결과가 없습니다.")
    dates: list[str] = []
    model_hash: str | None = None
    center_id: str | None = None
    start_hour: int | None = None
    source_contract: dict[str, Any] | None = None
    contract_reference: dict[int, str] | None = None
    operation_contract_reference: dict[str, Any] | None = None
    duration_reference: tuple[int, ...] | None = None
    policy_reference: tuple[str, ...] | None = None
    policy_configurations: dict[str, tuple[str, dict[str, Any]]] = {}
    source_provenance_by_date: dict[str, dict[str, Any]] = {}
    input_provenance_sha256_by_date: dict[str, str] = {}
    station_surface_sha256_by_date: dict[str, str] = {}
    station_count_by_date: dict[str, int] = {}
    for index, value in enumerate(documents):
        document = _require_mapping(value, f"documents[{index}]")
        target_date = _require_nonblank(document, "target_date", f"documents[{index}]")
        dates.append(target_date)
        candidate_center_id = _require_nonblank(
            document,
            "center_id",
            f"documents[{index}]",
        )
        if center_id is None:
            center_id = candidate_center_id
        elif candidate_center_id != center_id:
            raise ValueError("날짜별 center_id가 다릅니다.")
        candidate_start_hour = document.get("start_hour")
        if type(candidate_start_hour) is not int or not 0 <= candidate_start_hour <= 23:
            raise ValueError(f"{target_date} start_hour가 0..23 정수가 아닙니다.")
        if start_hour is None:
            start_hour = candidate_start_hour
        elif candidate_start_hour != start_hour:
            raise ValueError("날짜별 start_hour가 다릅니다.")
        candidate_model_hash = _require_sha256(
            document.get("model_bundle_sha256"),
            f"{target_date} model_bundle_sha256",
        )
        if model_hash is None:
            model_hash = candidate_model_hash
        elif candidate_model_hash != model_hash:
            raise ValueError("서로 다른 모델 bundle 결과를 섞을 수 없습니다.")
        _validate_evidence_gate(document, target_date)
        source_audit = _validate_source_provenance(document, target_date)
        source_provenance_by_date[target_date] = source_audit
        input_provenance_sha256_by_date[target_date] = _input_provenance_sha256(
            source_audit
        )
        station_surface_sha256_by_date[target_date] = source_audit[
            "station_master_content_sha256"
        ]
        candidate_source_contract = {
            name: source_audit[name]
            for name in (
                "backtest_contract_version",
                "route_algorithm_version",
                "urgency_scoring_config_version",
                "weather_csv_sha256",
            )
        }
        if source_contract is None:
            source_contract = candidate_source_contract
        elif candidate_source_contract != source_contract:
            raise ValueError(f"날짜별 source provenance가 다릅니다: {target_date}")
        contracts, operation_contract = _contract_signatures(
            document,
            target_date,
            expected_start_hour=candidate_start_hour,
        )
        if contract_reference is None:
            contract_reference = contracts
        elif contracts != contract_reference:
            raise ValueError(f"날짜 외 운영 계약이 다릅니다: {target_date}")
        if operation_contract_reference is None:
            operation_contract_reference = operation_contract
        elif operation_contract != operation_contract_reference:
            raise ValueError(f"날짜별 operation contract가 다릅니다: {target_date}")
        durations = _require_sequence(document.get("durations"), f"{target_date} durations")
        duration_minutes: list[int] = []
        document_station_count: int | None = None
        document_policies: tuple[str, ...] | None = None
        for duration_index, duration_value in enumerate(durations):
            duration = _require_mapping(
                duration_value,
                f"{target_date} durations[{duration_index}]",
            )
            minutes = _require_positive_int(
                duration.get("evaluation_minutes"),
                f"{target_date} evaluation_minutes",
            )
            if minutes in duration_minutes:
                raise ValueError(f"{target_date}에 중복 평가 구간이 있습니다: {minutes}")
            duration_minutes.append(minutes)
            station_count = _require_positive_int(
                duration.get("station_count"),
                f"{target_date} {minutes}분 station_count",
            )
            if document_station_count is None:
                document_station_count = station_count
            elif station_count != document_station_count:
                raise ValueError(
                    f"{target_date}의 구간별 station_count가 다릅니다."
                )
            policies = _validate_duration_policies(
                duration,
                target_date=target_date,
                start_hour=candidate_start_hour,
                minutes=minutes,
                policy_configurations=policy_configurations,
            )
            if document_policies is None:
                document_policies = policies
            elif policies != document_policies:
                raise ValueError(
                    f"{target_date}의 구간별 policy×duration 결과가 불완전합니다."
                )
        normalized_minutes = tuple(sorted(duration_minutes))
        if set(normalized_minutes) != set(contracts):
            raise ValueError(f"{target_date}의 contracts와 durations 구간이 다릅니다.")
        if duration_reference is None:
            duration_reference = normalized_minutes
        elif normalized_minutes != duration_reference:
            raise ValueError("날짜별 평가 구간 수 또는 구간 값이 다릅니다.")
        if document_policies is None:
            raise ValueError(f"{target_date}에 정책 결과가 없습니다.")
        assert document_station_count is not None
        station_count_by_date[target_date] = document_station_count
        if policy_reference is None:
            policy_reference = document_policies
        elif document_policies != policy_reference:
            raise ValueError(
                f"{target_date}의 policy×duration 결과가 다른 날짜와 다릅니다."
            )
    if len(dates) != len(set(dates)):
        raise ValueError("집계 결과에 중복 날짜가 있습니다.")
    assert model_hash is not None
    assert center_id is not None
    assert start_hour is not None
    assert source_contract is not None
    assert duration_reference is not None
    assert operation_contract_reference is not None
    return {
        "dates": dates,
        "center_id": center_id,
        "start_hour": start_hour,
        "operation_contract": operation_contract_reference,
        "model_bundle_sha256": model_hash,
        "input_provenance_sha256_by_date": {
            target_date: input_provenance_sha256_by_date[target_date]
            for target_date in sorted(input_provenance_sha256_by_date)
        },
        "station_surface_sha256_by_date": {
            target_date: station_surface_sha256_by_date[target_date]
            for target_date in sorted(station_surface_sha256_by_date)
        },
        "station_count_by_date": {
            target_date: station_count_by_date[target_date]
            for target_date in sorted(station_count_by_date)
        },
        "source_contract": source_contract,
        "source_provenance_by_date": {
            target_date: source_provenance_by_date[target_date]
            for target_date in sorted(source_provenance_by_date)
        },
        "evaluation_minutes": list(duration_reference),
        "policy_configurations": {
            policy: configuration
            for policy, (_, configuration) in sorted(policy_configurations.items())
        },
    }


def _validate_evidence_gate(document: Mapping[str, Any], target_date: str) -> None:
    """필수 point-in-time 근거 gate가 정확히 bool true인지 검증한다."""
    gate = _require_mapping(document.get("evidence_gate"), f"{target_date} evidence_gate")
    if any(gate.get(name) is not True for name in _REQUIRED_EVIDENCE_GATES):
        raise ValueError(f"point-in-time held-out gate를 통과하지 못했습니다: {target_date}")


def _validate_source_provenance(
    document: Mapping[str, Any],
    target_date: str,
) -> dict[str, Any]:
    """원천 fingerprint와 production 버전을 검증해 날짜별 감사를 만든다."""
    provenance = _require_mapping(
        document.get("source_provenance"),
        f"{target_date} source_provenance",
    )
    expected_versions = {
        "backtest_contract_version": BACKTEST_CONTRACT_VERSION,
        "route_algorithm_version": ROUTE_ALGORITHM_VERSION,
        "urgency_scoring_config_version": URGENCY_SCORING_CONFIG_VERSION,
    }
    for name, expected in expected_versions.items():
        if provenance.get(name) != expected:
            raise ValueError(
                f"{target_date}의 {name}이 production 계약과 다릅니다: "
                f"expected={expected}, actual={provenance.get(name)}"
            )
    for name in ("rental_csv", "stock_csv", "weather_csv"):
        _validate_source_file(provenance.get(name), f"{target_date} {name}")
    population_csvs = _require_sequence(
        provenance.get("population_csvs"),
        f"{target_date} population_csvs",
    )
    if not population_csvs:
        raise ValueError(f"{target_date} population_csvs가 비어 있습니다.")
    for index, source in enumerate(population_csvs):
        _validate_source_file(source, f"{target_date} population_csvs[{index}]")
    station_hash = _require_sha256(
        provenance.get("station_master_content_sha256"),
        f"{target_date} station master SHA-256",
    )
    crosswalk_hash = _require_sha256(
        provenance.get("station_crosswalk_sha256"),
        f"{target_date} station crosswalk SHA-256",
    )
    crosswalk_count = _require_positive_int(
        provenance.get("station_crosswalk_count"),
        f"{target_date} station_crosswalk_count",
    )
    excluded_count = provenance.get("population_excluded_station_count")
    if type(excluded_count) is not int or excluded_count < 0:
        raise ValueError(f"{target_date} population 제외 개수가 잘못됐습니다.")
    excluded_ids = _require_sequence(
        provenance.get("population_excluded_grid_ids"),
        f"{target_date} population_excluded_grid_ids",
    )
    if any(type(value) is not str or not value for value in excluded_ids):
        raise ValueError(f"{target_date} population 제외 grid ID가 잘못됐습니다.")
    weather = _require_mapping(provenance["weather_csv"], f"{target_date} weather_csv")
    rental = _require_mapping(provenance["rental_csv"], f"{target_date} rental_csv")
    stock = _require_mapping(provenance["stock_csv"], f"{target_date} stock_csv")
    return {
        **expected_versions,
        "rental_csv_sha256": rental["sha256"],
        "stock_csv_sha256": stock["sha256"],
        "weather_csv_sha256": weather["sha256"],
        "population_csv_sha256": [source["sha256"] for source in population_csvs],
        "station_master_content_sha256": station_hash,
        "station_crosswalk_count": crosswalk_count,
        "station_crosswalk_sha256": crosswalk_hash,
        "population_excluded_station_count": excluded_count,
        "population_excluded_grid_ids": list(excluded_ids),
    }


def _validate_source_file(value: object, label: str) -> None:
    """원천 파일 provenance의 경로·크기·SHA-256 형식을 검증한다."""
    source = _require_mapping(value, label)
    _require_nonblank(source, "path", label)
    size = source.get("size_bytes")
    if type(size) is not int or size <= 0:
        raise ValueError(f"{label} size_bytes는 양의 정수여야 합니다.")
    _require_sha256(source.get("sha256"), f"{label} sha256")


def _contract_signatures(
    document: Mapping[str, Any],
    target_date: str,
    *,
    expected_start_hour: int,
) -> tuple[dict[int, str], dict[str, Any]]:
    """구간별 계약과 평가 시간 외 공통 operation surface를 검증한다."""
    values = _require_sequence(document.get("contracts"), f"{target_date} contracts")
    result: dict[int, str] = {}
    operation_contract: dict[str, Any] | None = None
    operation_contract_canonical: str | None = None
    for index, value in enumerate(values):
        audit = dict(_require_mapping(value, f"{target_date} contracts[{index}]"))
        if audit.get("primary_metric") != PRIMARY_METRIC:
            raise ValueError(
                f"{target_date} contracts[{index}] primary_metric이 "
                f"{PRIMARY_METRIC}과 exact하지 않습니다."
            )
        contract = dict(
            _require_mapping(audit.get("contract"), f"{target_date} contract[{index}]")
        )
        if contract.get("target_date") != target_date:
            raise ValueError(f"{target_date}와 contract target_date가 다릅니다.")
        if contract.get("start_hour") != expected_start_hour:
            raise ValueError(
                f"{target_date} top-level start_hour와 contract가 다릅니다."
            )
        minutes = _require_positive_int(
            contract.get("evaluation_minutes"),
            f"{target_date} contract evaluation_minutes",
        )
        if minutes in result:
            raise ValueError(f"{target_date}에 중복 운영 계약이 있습니다: {minutes}")
        candidate_operation_contract = dict(contract)
        candidate_operation_contract.pop("target_date")
        candidate_operation_contract.pop("evaluation_minutes")
        candidate_canonical = _canonical_json(
            candidate_operation_contract,
            f"{target_date} operation contract",
        )
        if operation_contract is None:
            operation_contract = candidate_operation_contract
            operation_contract_canonical = candidate_canonical
        elif candidate_canonical != operation_contract_canonical:
            raise ValueError(
                f"{target_date}의 구간별 operation contract가 다릅니다."
            )
        contract.pop("target_date")
        audit["contract"] = contract
        result[minutes] = _canonical_json(audit, f"{target_date} contract")
    if not result:
        raise ValueError(f"{target_date} contracts가 비어 있습니다.")
    assert operation_contract is not None
    return result, operation_contract


def _validate_duration_policies(
    duration: Mapping[str, Any],
    *,
    target_date: str,
    start_hour: int,
    minutes: int,
    policy_configurations: dict[str, tuple[str, dict[str, Any]]],
) -> tuple[str, ...]:
    """한 날짜·구간에 기준선과 완전한 고유 정책 결과가 있는지 검증한다."""
    baseline = _require_mapping(
        duration.get("no_rebalance"),
        f"{target_date} {minutes}분 no_rebalance",
    )
    model_values = _require_sequence(
        duration.get("model_policies"),
        f"{target_date} {minutes}분 model_policies",
    )
    if not model_values:
        raise ValueError(f"{target_date} {minutes}분 model_policies가 비어 있습니다.")
    models = tuple(
        _require_mapping(value, f"{target_date} {minutes}분 model_policies[{index}]")
        for index, value in enumerate(model_values)
    )
    policies = (baseline, *models)
    names = tuple(
        _require_nonblank(policy, "policy", f"{target_date} {minutes}분 policy")
        for policy in policies
    )
    if names[0] != _BASELINE_POLICY or _BASELINE_POLICY in names[1:]:
        raise ValueError(f"{target_date} {minutes}분 기준선 policy가 잘못됐습니다.")
    if len(names) != len(set(names)):
        raise ValueError(f"{target_date} {minutes}분에 중복 policy가 있습니다.")
    baseline_requests = _validate_policy_metrics(
        baseline,
        target_date,
        start_hour,
        minutes,
    )
    for policy, name in zip(policies, names, strict=True):
        requests = _validate_policy_metrics(
            policy,
            target_date,
            start_hour,
            minutes,
        )
        if requests != baseline_requests:
            raise ValueError(
                f"{target_date} {minutes}분 {name}의 관측 요청 수가 기준선과 다릅니다."
            )
        configuration = dict(
            _require_mapping(
                policy.get("policy_configuration"),
                f"{target_date} {minutes}분 {name} policy_configuration",
            )
        )
        canonical = _canonical_json(configuration, f"{name} policy_configuration")
        previous = policy_configurations.get(name)
        if previous is None:
            policy_configurations[name] = (canonical, configuration)
        elif previous[0] != canonical:
            raise ValueError(f"policy_configuration이 실행마다 다릅니다: {name}")
    return tuple(sorted(names[1:]))


def _validate_policy_metrics(
    policy: Mapping[str, Any],
    target_date: str,
    start_hour: int,
    minutes: int,
) -> int:
    """집계 핵심 지표가 유한하고 요청 수 항등식을 지키는지 검증한다."""
    name = policy.get("policy")
    window_start, window_end = _expected_policy_window(
        target_date,
        start_hour,
        minutes,
    )
    if policy.get("window_start") != window_start.isoformat():
        raise ValueError(
            f"{target_date} {minutes}분 {name} window_start가 계약과 다릅니다."
        )
    if policy.get("window_end") != window_end.isoformat():
        raise ValueError(
            f"{target_date} {minutes}분 {name} window_end가 계약과 다릅니다."
        )
    counts = {}
    for metric in ("observed_requests", "fulfilled_requests", "unfulfilled_requests"):
        value = policy.get(metric)
        if type(value) is not int or value < 0:
            raise ValueError(f"{target_date} {minutes}분 {name} {metric}이 잘못됐습니다.")
        counts[metric] = value
    if (
        counts["fulfilled_requests"] + counts["unfulfilled_requests"]
        != counts["observed_requests"]
    ):
        raise ValueError(f"{target_date} {minutes}분 {name} 요청 수 항등식이 깨졌습니다.")
    expected_rate = (
        counts["fulfilled_requests"] / counts["observed_requests"]
        if counts["observed_requests"]
        else 1.0
    )
    rate = policy.get("observed_demand_fulfillment_rate")
    if (
        type(rate) not in (int, float)
        or not math.isfinite(rate)
        or abs(float(rate) - expected_rate) > _FLOAT_TOLERANCE
    ):
        raise ValueError(f"{target_date} {minutes}분 {name} 충족률이 요청 수와 다릅니다.")
    for metric in _ACCUMULATED_METRICS:
        value = policy.get(metric)
        if type(value) not in (int, float) or not math.isfinite(value) or value < 0:
            raise ValueError(f"{target_date} {minutes}분 {name} {metric}이 잘못됐습니다.")
    _unfulfilled_event_keys(
        policy,
        f"{target_date} {minutes}분 {name}",
        expected_count=counts["unfulfilled_requests"],
        window_start=window_start,
        window_end=window_end,
    )
    return counts["observed_requests"]


def _expected_policy_window(
    target_date: str,
    start_hour: int,
    minutes: int,
) -> tuple[datetime, datetime]:
    """계약의 날짜·시작 시각·구간으로 exact Asia/Seoul 창을 만든다."""
    try:
        midnight = datetime.fromisoformat(target_date)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"target_date가 ISO 날짜가 아닙니다: {target_date}") from exc
    if (
        target_date != midnight.date().isoformat()
        or midnight.time() != datetime.min.time()
        or midnight.tzinfo is not None
    ):
        raise ValueError(f"target_date가 canonical ISO 날짜가 아닙니다: {target_date}")
    window_start = midnight.replace(tzinfo=_SEOUL) + timedelta(hours=start_hour)
    return window_start, window_start + timedelta(minutes=minutes)


def _unfulfilled_event_keys(
    policy: Mapping[str, Any],
    label: str,
    *,
    expected_count: int | None = None,
    window_start: datetime,
    window_end: datetime,
) -> frozenset[tuple[str, str, int]]:
    """미충족 로그를 exact event key 집합으로 검증해 반환한다."""
    values = _require_sequence(
        policy.get("unfulfilled_request_log"),
        f"{label} unfulfilled_request_log",
    )
    events: list[tuple[str, str, int]] = []
    for index, value in enumerate(values):
        event_label = f"{label} unfulfilled_request_log[{index}]"
        event = _require_mapping(value, event_label)
        actual_keys = frozenset(event)
        if actual_keys != _UNFULFILLED_REQUEST_KEYS:
            raise ValueError(
                f"{event_label} key가 exact하지 않습니다: "
                f"missing={sorted(_UNFULFILLED_REQUEST_KEYS - actual_keys)}, "
                f"extra={sorted(actual_keys - _UNFULFILLED_REQUEST_KEYS)}"
            )
        bike_id = event.get("bike_id")
        if (
            type(bike_id) is not str
            or not bike_id.strip()
            or bike_id != bike_id.strip()
        ):
            raise ValueError(
                f"{event_label} bike_id는 trim된 nonblank 문자열이어야 합니다."
            )
        rented_at = event.get("rented_at")
        if type(rented_at) is not str or not rented_at:
            raise ValueError(f"{event_label} rented_at은 nonblank 문자열이어야 합니다.")
        try:
            parsed_rented_at = datetime.fromisoformat(rented_at)
        except ValueError as exc:
            raise ValueError(f"{event_label} rented_at이 ISO-8601이 아닙니다.") from exc
        if parsed_rented_at.tzinfo is None or parsed_rented_at.utcoffset() is None:
            raise ValueError(f"{event_label} rented_at에 timezone offset이 없습니다.")
        if not window_start <= parsed_rented_at < window_end:
            raise ValueError(f"{event_label} rented_at이 [window_start, window_end) 밖입니다.")
        station_no = event.get("station_no")
        if type(station_no) is not int or station_no <= 0:
            raise ValueError(f"{event_label} station_no는 양의 정수여야 합니다.")
        events.append(
            (
                bike_id,
                parsed_rented_at.astimezone(UTC).isoformat(),
                station_no,
            )
        )
    if len(events) != len(set(events)):
        raise ValueError(f"{label} unfulfilled_request_log에 중복 event key가 있습니다.")
    required_count = (
        policy.get("unfulfilled_requests")
        if expected_count is None
        else expected_count
    )
    if type(required_count) is not int or required_count < 0:
        raise ValueError(f"{label} unfulfilled_requests가 잘못됐습니다.")
    if len(events) != required_count:
        raise ValueError(
            f"{label} unfulfilled_request_log 길이와 "
            "unfulfilled_requests가 다릅니다."
        )
    return frozenset(events)


def _unfulfilled_event_sort_key(
    event: tuple[str, str, int],
) -> tuple[str, bytes, int]:
    """미충족 event key를 시각·자전거·대여소 순으로 결정적으로 정렬한다."""
    return (event[1], event[0].encode("utf-8"), event[2])


def _unfulfilled_event_document(
    event: tuple[str, str, int],
) -> dict[str, object]:
    """내부 event key를 JSON 감사 문서로 변환한다."""
    return {
        "bike_id": event[0],
        "rented_at": event[1],
        "station_no": event[2],
    }


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    """값이 JSON object가 아니면 설명 가능한 검증 오류를 낸다."""
    if not isinstance(value, Mapping):
        raise ValueError(f"{label}은 object여야 합니다.")
    return value


def _require_sequence(value: object, label: str) -> Sequence[Any]:
    """문자열이 아닌 JSON array인지 검증한다."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label}은 array여야 합니다.")
    return value


def _require_nonblank(value: Mapping[str, Any], name: str, label: str) -> str:
    """mapping의 필드가 공백이 아닌 문자열인지 검증한다."""
    candidate = value.get(name)
    if type(candidate) is not str or not candidate.strip():
        raise ValueError(f"{label} {name}은 nonblank 문자열이어야 합니다.")
    return candidate


def _require_positive_int(value: object, label: str) -> int:
    """bool이 아닌 양의 정수만 반환한다."""
    if type(value) is not int or value <= 0:
        raise ValueError(f"{label}은 양의 정수여야 합니다.")
    return value


def _require_sha256(value: object, label: str) -> str:
    """소문자 hexadecimal SHA-256 문자열만 반환한다."""
    if (
        type(value) is not str
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise ValueError(f"{label} 형식이 잘못됐습니다.")
    return value


def _canonical_json(value: object, label: str) -> str:
    """JSON 호환 값의 결정적 비교 문자열을 만들고 NaN을 거부한다."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{label}은 canonical JSON이어야 합니다.") from exc


def _input_provenance_sha256(source_audit: Mapping[str, Any]) -> str:
    """코드 버전을 제외한 날짜별 입력 audit의 canonical SHA-256을 계산한다."""
    input_fields = {
        name: value
        for name, value in source_audit.items()
        if name not in _PROVENANCE_VERSION_FIELDS
    }
    payload = _canonical_json(input_fields, "input provenance").encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def evaluate_acceptance_gate(result: Mapping[str, Any]) -> dict[str, Any]:
    """Exact production 정책의 시민 서비스·품절 시간 release 기준을 판정한다."""
    primary_metric_matches = result.get("primary_metric") == PRIMARY_METRIC
    rows = _require_sequence(result.get("rows"), "aggregate rows")
    evaluation_minutes = tuple(
        _require_positive_int(value, "aggregate evaluation_minutes")
        for value in _require_sequence(
            result.get("evaluation_minutes"),
            "aggregate evaluation_minutes",
        )
    )
    required_horizons_present = evaluation_minutes == PRODUCTION_EVALUATION_MINUTES
    expected_evidence_scope = production_evidence_scope()
    actual_dates = list(_require_sequence(result.get("dates"), "aggregate dates"))
    actual_operation_contract = _require_mapping(
        result.get("operation_contract"),
        "aggregate operation_contract",
    )
    source_contract = _require_mapping(
        result.get("source_contract"),
        "aggregate source_contract",
    )
    input_provenance_by_date = _require_mapping(
        result.get("input_provenance_sha256_by_date"),
        "aggregate input_provenance_sha256_by_date",
    )
    station_surface_by_date = _require_mapping(
        result.get("station_surface_sha256_by_date"),
        "aggregate station_surface_sha256_by_date",
    )
    station_count_by_date = _require_mapping(
        result.get("station_count_by_date"),
        "aggregate station_count_by_date",
    )
    expected_operation_contract = {
        "start_hour": expected_evidence_scope["start_hour"],
        **expected_evidence_scope["operation_contract"],
    }
    evidence_scope_checks = {
        "exact_result_count": (
            result.get("result_count") == expected_evidence_scope["result_count"]
        ),
        "exact_target_dates": actual_dates == expected_evidence_scope["target_dates"],
        "exact_center_id": (
            result.get("center_id") == expected_evidence_scope["center_id"]
        ),
        "exact_start_hour": (
            result.get("start_hour") == expected_evidence_scope["start_hour"]
        ),
        "exact_evaluation_minutes": required_horizons_present,
        "exact_operation_contract": (
            _canonical_json(
                actual_operation_contract,
                "aggregate operation_contract",
            )
            == _canonical_json(
                expected_operation_contract,
                "production operation_contract",
            )
        ),
        "exact_model_bundle_sha256": (
            result.get("model_bundle_sha256")
            == expected_evidence_scope["model_bundle_sha256"]
        ),
        "exact_input_provenance_sha256_by_date": (
            _canonical_json(input_provenance_by_date, "input provenance map")
            == _canonical_json(
                expected_evidence_scope["input_provenance_sha256_by_date"],
                "production input provenance map",
            )
        ),
        "exact_station_surface_sha256_by_date": (
            _canonical_json(station_surface_by_date, "station surface map")
            == _canonical_json(
                expected_evidence_scope["station_surface_sha256_by_date"],
                "production station surface map",
            )
        ),
        "exact_station_count_by_date": (
            _canonical_json(station_count_by_date, "station count map")
            == _canonical_json(
                expected_evidence_scope["station_count_by_date"],
                "production station count map",
            )
        ),
        "exact_weather_csv_sha256": (
            source_contract.get("weather_csv_sha256")
            == expected_evidence_scope["weather_csv_sha256"]
        ),
    }
    production_evidence_scope_matches = all(evidence_scope_checks.values())
    configurations = _require_mapping(
        result.get("policy_configurations"),
        "aggregate policy_configurations",
    )
    expected_configuration = production_policy_configuration()
    actual_production_configuration = configurations.get(PRODUCTION_POLICY_NAME)
    production_policy_present = actual_production_configuration is not None
    production_policy_configuration_matches = (
        isinstance(actual_production_configuration, Mapping)
        and _canonical_json(
            actual_production_configuration,
            f"{PRODUCTION_POLICY_NAME} policy_configuration",
        )
        == _canonical_json(expected_configuration, "production policy_configuration")
    )
    by_policy: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for index, value in enumerate(rows):
        row = _require_mapping(value, f"aggregate rows[{index}]")
        policy = _require_nonblank(row, "policy", f"aggregate rows[{index}]")
        if policy != _BASELINE_POLICY:
            by_policy[policy].append(row)
    policy_gates = []
    for policy, policy_rows in sorted(by_policy.items()):
        per_date = [
            comparison
            for row in policy_rows
            for comparison in _require_sequence(
                row.get("per_date_comparison"),
                f"{policy} per_date_comparison",
            )
        ]
        unmet_non_worsening = all(
            comparison["unfulfilled_delta"] <= 0 for comparison in per_date
        )
        empty_non_worsening = all(
            comparison["empty_station_minutes_delta"] <= _FLOAT_TOLERANCE
            for comparison in per_date
        )
        no_new_unfulfilled_requests = all(
            comparison["new_unfulfilled_request_count"] == 0
            and not comparison["new_unfulfilled_request_keys"]
            for comparison in per_date
        )
        row_180 = next(
            (row for row in policy_rows if row["evaluation_minutes"] == 180),
            None,
        )
        unmet_180_strict = (
            row_180 is not None
            and row_180["unfulfilled_change_vs_no_rebalance"] < 0
        )
        aggregate_empty_by_horizon = {
            str(row["evaluation_minutes"]): (
                row["empty_station_minutes_change_vs_no_rebalance_pct"] is not None
                and row["empty_station_minutes"]
                < _baseline_empty_minutes(rows, row["evaluation_minutes"])
                - _FLOAT_TOLERANCE
            )
            for row in policy_rows
        }
        aggregate_empty_strict = bool(aggregate_empty_by_horizon) and all(
            aggregate_empty_by_horizon.values()
        )
        checks = {
            "all_date_horizon_unfulfilled_non_worsening": unmet_non_worsening,
            "all_date_horizon_new_unfulfilled_request_set_empty": (
                no_new_unfulfilled_requests
            ),
            "all_date_horizon_empty_non_worsening": empty_non_worsening,
            "aggregate_180_unfulfilled_strict_improvement": unmet_180_strict,
            "aggregate_empty_strict_improvement_by_horizon": aggregate_empty_strict,
        }
        candidate_checks_passed = all(checks.values())
        production_policy_name_matches = policy == PRODUCTION_POLICY_NAME
        production_release_passed = (
            candidate_checks_passed
            and production_policy_name_matches
            and production_policy_configuration_matches
            and production_evidence_scope_matches
            and primary_metric_matches
        )
        policy_gates.append(
            {
                "policy": policy,
                "passed": production_release_passed,
                "candidate_checks_passed": candidate_checks_passed,
                "production_policy_name_matches": production_policy_name_matches,
                "production_policy_configuration_matches": (
                    production_policy_configuration_matches
                    if production_policy_name_matches
                    else False
                ),
                **checks,
                "aggregate_empty_by_horizon": aggregate_empty_by_horizon,
            }
        )
    passing = [gate["policy"] for gate in policy_gates if gate["passed"]]
    return {
        "version": ACCEPTANCE_GATE_VERSION,
        "passed": bool(passing),
        "passing_policies": passing,
        "primary_metric": result.get("primary_metric"),
        "required_primary_metric": PRIMARY_METRIC,
        "primary_metric_matches": primary_metric_matches,
        "required_policy": PRODUCTION_POLICY_NAME,
        "required_policy_configuration": expected_configuration,
        "required_evidence_scope": expected_evidence_scope,
        "evidence_scope_checks": evidence_scope_checks,
        "production_evidence_scope_matches": production_evidence_scope_matches,
        "required_evaluation_minutes": list(PRODUCTION_EVALUATION_MINUTES),
        "required_horizons_present": required_horizons_present,
        "production_policy_present": production_policy_present,
        "production_policy_configuration_matches": (
            production_policy_configuration_matches
        ),
        "policies": policy_gates,
    }


def evaluate_candidate_gate(
    acceptance_gate: Mapping[str, Any],
) -> dict[str, Any]:
    """탐색 후보의 서비스·품절 진단을 production release 판정과 분리한다."""
    policy_values = _require_sequence(
        acceptance_gate.get("policies"),
        "acceptance policies",
    )
    policies = []
    for index, value in enumerate(policy_values):
        policy = _require_mapping(value, f"acceptance policies[{index}]")
        name = _require_nonblank(policy, "policy", f"acceptance policies[{index}]")
        passed = policy.get("candidate_checks_passed") is True
        policies.append({"policy": name, "passed": passed})
    passing = [policy["policy"] for policy in policies if policy["passed"]]
    return {
        "version": CANDIDATE_GATE_VERSION,
        "passed": bool(passing),
        "passing_policies": passing,
        "policies": policies,
    }


def _baseline_empty_minutes(rows: Sequence[Any], minutes: int) -> float:
    """한 horizon의 no_rebalance 품절 대여소-분을 유일하게 찾는다."""
    matches = [
        row
        for row in rows
        if row["policy"] == _BASELINE_POLICY and row["evaluation_minutes"] == minutes
    ]
    if len(matches) != 1:
        raise ValueError(f"{minutes}분 no_rebalance 집계 행이 유일하지 않습니다.")
    return float(matches[0]["empty_station_minutes"])


def aggregate_markdown(result: dict[str, Any]) -> str:
    """집계 결과를 날짜별 악화 횟수까지 보이는 Markdown 표로 만든다."""
    acceptance = result["acceptance_gate"]
    passing = ", ".join(acceptance["passing_policies"]) or "없음"
    candidate = result["candidate_gate"]
    candidate_passing = ", ".join(candidate["passing_policies"]) or "없음"
    lines = [
        "# 재배치 정책 held-out 날짜 집계",
        "",
        f"- 날짜: {', '.join(result['dates'])}",
        f"- 모델 SHA-256: `{result['model_bundle_sha256']}`",
        f"- Primary metric: `{result['primary_metric']}`",
        f"- Production release gate 통과: **{acceptance['passed']}** ({passing})",
        f"- 탐색 후보 진단 통과: **{candidate['passed']}** ({candidate_passing})",
        f"- 필수 평가 구간 60·120·180분 완비: "
        f"**{acceptance['required_horizons_present']}**",
        "- 발표용 인과 주장 허용: **False**",
        "",
        "## 추정 기존 운영",
        "",
        "| 구간 | 균형 이동 예산 | 품절 대여소-분 범위 | 음수 재고 대여소-분 범위 | 종료 재고 최대 오차 |",
        "|---:|---:|---:|---:|---:|",
    ]
    for legacy in result["legacy_summaries"]:
        lines.append(
            f"| {legacy['evaluation_minutes']}분 | "
            f"{legacy['balanced_movement_budget']} | "
            f"{legacy['empty_station_minutes_min']:.1f}~"
            f"{legacy['empty_station_minutes_max']:.1f} | "
            f"{legacy['negative_station_minutes_min']:.1f}~"
            f"{legacy['negative_station_minutes_max']:.1f} | "
            f"{legacy['endpoint_max_absolute_error']} |"
        )
    lines.extend(
        (
            "",
            "## 정책 결과",
            "",
            (
                "| 구간 | 정책 | 요청 | 충족률 | 미충족 변화 | 신규 실패 | "
                "해소 실패 | 품절 대여소-분 변화 | 날짜별 충족률 "
                "개선/동일/악화 | 이동 | 차량 분 |"
            ),
            "|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
        )
    )
    for row in result["rows"]:
        empty_change = row["empty_station_minutes_change_vs_no_rebalance_pct"]
        empty_text = "기준" if empty_change is None else f"{empty_change:+.2f}%"
        lines.append(
            f"| {row['evaluation_minutes']}분 | {row['policy']} | "
            f"{row['observed_requests']:,} | "
            f"{row['observed_demand_fulfillment_rate']:.4%} | "
            f"{row['unfulfilled_change_vs_no_rebalance']:+d} | "
            f"{row['new_unfulfilled_request_count']} | "
            f"{row['resolved_unfulfilled_request_count']} | {empty_text} | "
            f"{row['dates_fulfillment_better']}/"
            f"{row['dates_fulfillment_equal']}/"
            f"{row['dates_fulfillment_worse']} | "
            f"{row['moved_bikes']} | {row['vehicle_busy_minutes']:.1f} |"
        )
    lines.extend(
        (
            "",
            "> 여러 held-out 날짜 집계도 관측 성공 수요 replay다. 실패 수요와 기존 운영 "
            "작업 로그가 없으므로 실제 운영 대비 인과적 개선율로 인용하면 안 된다.",
            "",
        )
    )
    return "\n".join(lines)


def write_aggregate(
    result: dict[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> None:
    """집계 JSON과 Markdown을 저장한다."""
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(aggregate_markdown(result), encoding="utf-8")


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """날짜별 결과 파일과 출력 경로를 파싱한다."""
    parser = argparse.ArgumentParser(description="point-in-time 백테스트 결과 집계")
    parser.add_argument("--inputs", nargs="+", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    parser.add_argument("--output-markdown", type=Path, required=True)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """날짜별 JSON을 검증·집계하고 채택 정책이 없으면 실패한다."""
    args = parse_args(argv)
    documents = [json.loads(path.read_text(encoding="utf-8")) for path in args.inputs]
    result = aggregate_results(documents)
    write_aggregate(
        result,
        json_path=args.output_json,
        markdown_path=args.output_markdown,
    )
    print(aggregate_markdown(result))
    return 0 if result["acceptance_gate"]["passed"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
