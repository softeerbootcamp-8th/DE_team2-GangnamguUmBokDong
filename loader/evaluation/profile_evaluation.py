"""공통 백테스트 결과를 단일 profile 결과 스키마로 집계하고 판정한다."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any

from .backtest_contract import EvaluationContract, PRIMARY_METRIC
from .evaluation_profiles import EvaluationCell, EvaluationProfile

EVALUATION_RESULT_SCHEMA_VERSION = "point-in-time-policy-evaluation-v1"
_FLOAT_TOLERANCE = 1e-9
_SOURCE_VERSION_FIELDS = frozenset(
    {
        "backtest_contract_version",
        "route_algorithm_version",
        "urgency_scoring_config_version",
    }
)


def load_backtest_result(path: Path) -> dict[str, Any]:
    """일반 raw JSON과 과거 confirmatory envelope에서 백테스트 결과를 읽는다."""
    try:
        document = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ValueError(f"백테스트 JSON을 읽을 수 없습니다: {path}") from exc
    if not isinstance(document, dict):
        raise ValueError(f"백테스트 JSON 최상위는 object여야 합니다: {path}")
    nested = document.get("result")
    if isinstance(nested, dict):
        return nested
    return document


def evaluate_profile(
    profile: EvaluationProfile,
    documents: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Profile의 exact 셀 결과를 공통 스키마로 집계하고 gate를 판정한다."""
    if not documents:
        raise ValueError("평가할 백테스트 결과가 없습니다.")
    expected_cells = {cell.key: cell for cell in profile.cells}
    summaries: list[dict[str, Any]] = []
    seen: set[str] = set()
    model_roots: set[str] = set()
    for index, value in enumerate(documents):
        if not isinstance(value, Mapping):
            raise ValueError(f"documents[{index}]는 object여야 합니다.")
        key = _document_cell_key(value)
        if key not in expected_cells:
            raise ValueError(f"profile에 없는 평가 셀입니다: {key}")
        if key in seen:
            raise ValueError(f"중복 평가 셀입니다: {key}")
        seen.add(key)
        summary = _summarize_cell(profile, expected_cells[key], value)
        summaries.append(summary)
        model_roots.add(_nonblank(value.get("model_bundle_root"), f"{key} model root"))
    missing = set(expected_cells) - seen
    if missing:
        raise ValueError(f"profile 평가 셀이 누락됐습니다: {sorted(missing)}")
    if len(model_roots) != 1:
        raise ValueError("평가 셀마다 model_bundle_root가 다릅니다.")
    summaries.sort(key=lambda row: row["cell_key"].encode("utf-8"))
    aggregates = _aggregate_horizons(profile, summaries)
    evidence_checks = _evaluate_evidence_scope(profile, summaries)
    gate = _evaluate_gate(profile, summaries, aggregates, evidence_checks)
    return {
        "schema_version": EVALUATION_RESULT_SCHEMA_VERSION,
        "profile": profile.audit_document(),
        "primary_metric": PRIMARY_METRIC,
        "policy": profile.policy_name,
        "model_bundle_root": next(iter(model_roots)),
        "model_bundle_sha256": profile.model_bundle_sha256,
        "cell_count": len(summaries),
        "cells": summaries,
        "aggregates": aggregates,
        "acceptance_gate": gate,
        "publication_grade_system_claim_allowed": False,
    }


def _document_cell_key(document: Mapping[str, Any]) -> str:
    """Raw 문서에서 profile 셀 식별자를 만든다."""
    center_id = _nonblank(document.get("center_id"), "center_id")
    target_date = _nonblank(document.get("target_date"), "target_date")
    start_hour = document.get("start_hour")
    if type(start_hour) is not int or not 0 <= start_hour <= 23:
        raise ValueError("start_hour는 0..23 정수여야 합니다.")
    return f"{center_id}|{target_date}|{start_hour:02d}"


def _summarize_cell(
    profile: EvaluationProfile,
    cell: EvaluationCell,
    document: Mapping[str, Any],
) -> dict[str, Any]:
    """한 raw 결과에서 공통 KPI와 실행 완결 지표를 추출한다."""
    key = cell.key
    model_sha = _nonblank(document.get("model_bundle_sha256"), f"{key} model SHA")
    if model_sha != profile.model_bundle_sha256:
        raise ValueError(f"{key} model bundle SHA가 profile과 다릅니다.")
    evidence_gate = _mapping(document.get("evidence_gate"), f"{key} evidence gate")
    for name in (
        "point_in_time_feature_inputs",
        "operation_contract_passed",
        "legacy_endpoint_reconciliation_passed",
        "heldout_day_of_month",
    ):
        if evidence_gate.get(name) is not True:
            raise ValueError(f"{key} evidence gate가 실패했습니다: {name}")
    expected_contracts = [
        EvaluationContract(
            target_date=cell.target_date,
            start_hour=cell.start_hour,
            evaluation_minutes=minutes,
            fleet_size=profile.fleet_size,
        ).audit_document()
        for minutes in profile.evaluation_minutes
    ]
    if _canonical(document.get("contracts")) != _canonical(expected_contracts):
        raise ValueError(f"{key} 운영 계약이 profile과 다릅니다.")
    duration_values = _sequence(document.get("durations"), f"{key} durations")
    duration_by_minutes: dict[int, Mapping[str, Any]] = {}
    for value in duration_values:
        duration = _mapping(value, f"{key} duration")
        minutes = duration.get("evaluation_minutes")
        if type(minutes) is not int or minutes not in profile.evaluation_minutes:
            raise ValueError(f"{key} evaluation_minutes가 profile과 다릅니다.")
        if minutes in duration_by_minutes:
            raise ValueError(f"{key}에 중복 평가 구간이 있습니다: {minutes}")
        duration_by_minutes[minutes] = duration
    if set(duration_by_minutes) != set(profile.evaluation_minutes):
        raise ValueError(f"{key} 평가 구간 exact set이 profile과 다릅니다.")
    summaries = [
        _summarize_duration(profile, key, duration_by_minutes[minutes])
        for minutes in profile.evaluation_minutes
    ]
    source = _mapping(document.get("source_provenance"), f"{key} source provenance")
    station_counts = {
        _positive_int(duration.get("station_count"), f"{key} station_count")
        for duration in duration_by_minutes.values()
    }
    if len(station_counts) != 1:
        raise ValueError(f"{key} 평가 구간별 station_count가 다릅니다.")
    return {
        "cell_key": key,
        **cell.audit_document(),
        "center_name": _nonblank(document.get("center_name"), f"{key} center name"),
        "model_bundle_sha256": model_sha,
        "input_provenance_sha256": _input_provenance_sha256(source),
        "weather_csv_sha256": _source_sha(source.get("weather_csv"), f"{key} weather"),
        "station_master_content_sha256": _nonblank(
            source.get("station_master_content_sha256"),
            f"{key} station master SHA",
        ),
        "station_count": next(iter(station_counts)),
        "durations": summaries,
    }


def _summarize_duration(
    profile: EvaluationProfile,
    cell_key: str,
    duration: Mapping[str, Any],
) -> dict[str, Any]:
    """한 평가 구간의 baseline·후보·추정 기존 운영을 같은 행으로 만든다."""
    minutes = _positive_int(duration.get("evaluation_minutes"), "evaluation minutes")
    baseline = _mapping(duration.get("no_rebalance"), f"{cell_key} baseline")
    policies = _sequence(duration.get("model_policies"), f"{cell_key} model policies")
    if len(policies) != 1:
        raise ValueError(f"{cell_key} {minutes}분 후보 정책은 정확히 하나여야 합니다.")
    candidate = _mapping(policies[0], f"{cell_key} candidate")
    if baseline.get("policy") != "no_rebalance":
        raise ValueError(f"{cell_key} {minutes}분 baseline 이름이 잘못됐습니다.")
    if candidate.get("policy") != profile.policy_name:
        raise ValueError(f"{cell_key} {minutes}분 후보 정책이 profile과 다릅니다.")
    if _canonical(candidate.get("policy_configuration")) != _canonical(
        profile.audit_document()["policy_configuration"]
    ):
        raise ValueError(f"{cell_key} {minutes}분 후보 설정이 profile과 다릅니다.")
    baseline_requests = _nonnegative_int(
        baseline.get("observed_requests"), f"{cell_key} baseline requests"
    )
    candidate_requests = _nonnegative_int(
        candidate.get("observed_requests"), f"{cell_key} candidate requests"
    )
    if baseline_requests != candidate_requests:
        raise ValueError(f"{cell_key} {minutes}분 baseline과 후보 요청 수가 다릅니다.")
    baseline_unfulfilled = _event_keys(baseline, f"{cell_key} baseline")
    candidate_unfulfilled = _event_keys(candidate, f"{cell_key} candidate")
    new_unfulfilled = candidate_unfulfilled - baseline_unfulfilled
    resolved_unfulfilled = baseline_unfulfilled - candidate_unfulfilled
    legacy = _mapping(duration.get("legacy_movement"), f"{cell_key} legacy movement")
    legacy_timing = _sequence(duration.get("legacy_timing"), f"{cell_key} legacy timing")
    legacy_empty = [
        _nonnegative_float(
            _mapping(value, f"{cell_key} legacy timing").get("empty_station_minutes"),
            f"{cell_key} legacy empty minutes",
        )
        for value in legacy_timing
    ]
    if not legacy_empty:
        raise ValueError(f"{cell_key} legacy timing 결과가 없습니다.")
    baseline_empty = _nonnegative_float(
        baseline.get("empty_station_minutes"), f"{cell_key} baseline empty"
    )
    candidate_empty = _nonnegative_float(
        candidate.get("empty_station_minutes"), f"{cell_key} candidate empty"
    )
    baseline_fulfilled = _nonnegative_int(
        baseline.get("fulfilled_requests"), f"{cell_key} baseline fulfilled"
    )
    candidate_fulfilled = _nonnegative_int(
        candidate.get("fulfilled_requests"), f"{cell_key} candidate fulfilled"
    )
    if baseline_fulfilled + len(baseline_unfulfilled) != baseline_requests:
        raise ValueError(f"{cell_key} {minutes}분 baseline 요청 산술이 맞지 않습니다.")
    if candidate_fulfilled + len(candidate_unfulfilled) != candidate_requests:
        raise ValueError(f"{cell_key} {minutes}분 후보 요청 산술이 맞지 않습니다.")
    planned_bikes = _nonnegative_int(candidate.get("planned_bikes"), "planned bikes")
    moved_bikes = _nonnegative_int(candidate.get("moved_bikes"), "moved bikes")
    dispatched_routes = _nonnegative_int(
        candidate.get("dispatched_routes"), "dispatched routes"
    )
    completed_routes = _nonnegative_int(
        candidate.get("completed_routes_by_cutoff"), "completed routes"
    )
    busy_trucks = _nonnegative_int(
        candidate.get("trucks_still_busy_at_cutoff"), "busy trucks"
    )
    return {
        "evaluation_minutes": minutes,
        "observed_requests": baseline_requests,
        "baseline_fulfilled_requests": baseline_fulfilled,
        "candidate_fulfilled_requests": candidate_fulfilled,
        "baseline_observed_demand_fulfillment_rate": (
            baseline_fulfilled / baseline_requests if baseline_requests else 1.0
        ),
        "candidate_observed_demand_fulfillment_rate": (
            candidate_fulfilled / candidate_requests if candidate_requests else 1.0
        ),
        "baseline_unfulfilled_requests": len(baseline_unfulfilled),
        "candidate_unfulfilled_requests": len(candidate_unfulfilled),
        "unfulfilled_delta": len(candidate_unfulfilled) - len(baseline_unfulfilled),
        "new_unfulfilled_request_count": len(new_unfulfilled),
        "resolved_unfulfilled_request_count": len(resolved_unfulfilled),
        "new_unfulfilled_station_nos": sorted({key[2] for key in new_unfulfilled}),
        "baseline_empty_station_minutes": baseline_empty,
        "candidate_empty_station_minutes": candidate_empty,
        "empty_station_minutes_delta": round(candidate_empty - baseline_empty, 3),
        "legacy_empty_station_minutes_min": min(legacy_empty),
        "legacy_empty_station_minutes_max": max(legacy_empty),
        "legacy_movement_budget": _nonnegative_int(
            legacy.get("balanced_movement_budget"), "legacy movement budget"
        ),
        "planned_bikes": planned_bikes,
        "moved_bikes": moved_bikes,
        "dispatched_routes": dispatched_routes,
        "completed_routes_by_cutoff": completed_routes,
        "trucks_still_busy_at_cutoff": busy_trucks,
        "finished_by_cutoff": (
            dispatched_routes == completed_routes and busy_trucks == 0
        ),
        "max_pickup_dispatch_lag_minutes": _max_pickup_dispatch_lag(candidate),
    }


def _event_keys(
    policy: Mapping[str, Any],
    label: str,
) -> frozenset[tuple[str, str, int]]:
    """미충족 요청을 이용자 전가 여부까지 비교할 수 있는 key 집합으로 만든다."""
    rows = _sequence(policy.get("unfulfilled_request_log"), f"{label} failures")
    result = set()
    for value in rows:
        event = _mapping(value, f"{label} failure")
        bike_id = _nonblank(event.get("bike_id"), f"{label} bike_id")
        rented_at = _nonblank(event.get("rented_at"), f"{label} rented_at")
        try:
            parsed = datetime.fromisoformat(rented_at)
        except ValueError as exc:
            raise ValueError(f"{label} rented_at이 ISO-8601이 아닙니다.") from exc
        if parsed.tzinfo is None or parsed.utcoffset() is None:
            raise ValueError(f"{label} rented_at에 timezone offset이 없습니다.")
        station_no = _positive_int(event.get("station_no"), f"{label} station_no")
        result.add((bike_id, parsed.isoformat(), station_no))
    expected = _nonnegative_int(policy.get("unfulfilled_requests"), f"{label} count")
    if len(result) != len(rows) or len(result) != expected:
        raise ValueError(f"{label} 미충족 요청 수와 event log가 다릅니다.")
    return frozenset(result)


def _max_pickup_dispatch_lag(policy: Mapping[str, Any]) -> float:
    """후보의 route 배차부터 pickup 실행까지 최대 지연을 계산한다."""
    maximum = 0.0
    for value in _sequence(policy.get("job_audits"), "candidate job audits"):
        job = _mapping(value, "candidate job audit")
        dispatched = _aware_datetime(job.get("dispatched_at"), "job dispatched_at")
        for stop_value in _sequence(job.get("stops"), "candidate job stops"):
            stop = _mapping(stop_value, "candidate job stop")
            if stop.get("action") != "pickup":
                continue
            executed = _aware_datetime(stop.get("executed_at"), "pickup executed_at")
            lag = (executed - dispatched).total_seconds() / 60.0
            if lag < -_FLOAT_TOLERANCE:
                raise ValueError("pickup 실행 시각이 배차 시각보다 빠릅니다.")
            maximum = max(maximum, lag)
    return round(maximum, 6)


def _aggregate_horizons(
    profile: EvaluationProfile,
    cells: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """모든 셀을 horizon별 micro-average와 운영 지표로 합친다."""
    result = []
    for minutes in profile.evaluation_minutes:
        rows = [
            duration
            for cell in cells
            for duration in cell["durations"]
            if duration["evaluation_minutes"] == minutes
        ]
        requests = sum(row["observed_requests"] for row in rows)
        baseline_fulfilled = sum(row["baseline_fulfilled_requests"] for row in rows)
        candidate_fulfilled = sum(row["candidate_fulfilled_requests"] for row in rows)
        baseline_empty = sum(row["baseline_empty_station_minutes"] for row in rows)
        candidate_empty = sum(row["candidate_empty_station_minutes"] for row in rows)
        legacy_low = sum(row["legacy_empty_station_minutes_min"] for row in rows)
        legacy_high = sum(row["legacy_empty_station_minutes_max"] for row in rows)
        result.append(
            {
                "evaluation_minutes": minutes,
                "observed_requests": requests,
                "baseline_fulfilled_requests": baseline_fulfilled,
                "candidate_fulfilled_requests": candidate_fulfilled,
                "baseline_observed_demand_fulfillment_rate": (
                    baseline_fulfilled / requests if requests else 1.0
                ),
                "candidate_observed_demand_fulfillment_rate": (
                    candidate_fulfilled / requests if requests else 1.0
                ),
                "fulfillment_rate_delta_percentage_points": (
                    (candidate_fulfilled - baseline_fulfilled) / requests * 100.0
                    if requests
                    else 0.0
                ),
                "baseline_unfulfilled_requests": sum(
                    row["baseline_unfulfilled_requests"] for row in rows
                ),
                "candidate_unfulfilled_requests": sum(
                    row["candidate_unfulfilled_requests"] for row in rows
                ),
                "new_unfulfilled_request_count": sum(
                    row["new_unfulfilled_request_count"] for row in rows
                ),
                "resolved_unfulfilled_request_count": sum(
                    row["resolved_unfulfilled_request_count"] for row in rows
                ),
                "baseline_empty_station_minutes": round(baseline_empty, 3),
                "candidate_empty_station_minutes": round(candidate_empty, 3),
                "empty_station_minutes_reduction_pct": round(
                    (
                        (baseline_empty - candidate_empty) / baseline_empty * 100.0
                        if baseline_empty > _FLOAT_TOLERANCE
                        else 0.0
                    ),
                    6,
                ),
                "legacy_empty_station_minutes_min": round(legacy_low, 3),
                "legacy_empty_station_minutes_max": round(legacy_high, 3),
                "empty_change_vs_legacy_timing_range_pct": (
                    None
                    if legacy_low <= _FLOAT_TOLERANCE
                    else [
                        round((candidate_empty - legacy_high) / legacy_high * 100.0, 3),
                        round((candidate_empty - legacy_low) / legacy_low * 100.0, 3),
                    ]
                ),
                "improved_cell_count": sum(
                    row["empty_station_minutes_delta"] < -_FLOAT_TOLERANCE
                    for row in rows
                ),
                "equal_cell_count": sum(
                    abs(row["empty_station_minutes_delta"]) <= _FLOAT_TOLERANCE
                    for row in rows
                ),
                "worse_cell_count": sum(
                    row["empty_station_minutes_delta"] > _FLOAT_TOLERANCE
                    for row in rows
                ),
                "moved_bikes": sum(row["moved_bikes"] for row in rows),
                "legacy_movement_budget": sum(
                    row["legacy_movement_budget"] for row in rows
                ),
                "dispatched_routes": sum(row["dispatched_routes"] for row in rows),
            }
        )
    return result


def _evaluate_evidence_scope(
    profile: EvaluationProfile,
    cells: Sequence[Mapping[str, Any]],
) -> dict[str, bool]:
    """Profile이 고정한 모델·원천 범위를 공통 방식으로 확인한다."""
    checks = {
        "exact_cells": [cell["cell_key"] for cell in cells]
        == sorted((cell.key for cell in profile.cells), key=lambda value: value.encode("utf-8")),
        "exact_model_bundle_sha256": all(
            cell["model_bundle_sha256"] == profile.model_bundle_sha256 for cell in cells
        ),
        "exact_policy_configuration": True,
    }
    expected = profile.expected_evidence_scope
    if expected is None:
        return checks
    by_date = {cell["target_date"]: cell for cell in cells}
    checks.update(
        {
            "exact_input_provenance": {
                target: by_date[target]["input_provenance_sha256"]
                for target in sorted(by_date)
            }
            == expected["input_provenance_sha256_by_date"],
            "exact_station_surface": {
                target: by_date[target]["station_master_content_sha256"]
                for target in sorted(by_date)
            }
            == expected["station_surface_sha256_by_date"],
            "exact_station_count": {
                target: by_date[target]["station_count"] for target in sorted(by_date)
            }
            == expected["station_count_by_date"],
            "exact_weather_csv_sha256": all(
                cell["weather_csv_sha256"] == expected["weather_csv_sha256"]
                for cell in cells
            ),
        }
    )
    return checks


def _evaluate_gate(
    profile: EvaluationProfile,
    cells: Sequence[Mapping[str, Any]],
    aggregates: Sequence[Mapping[str, Any]],
    evidence_checks: Mapping[str, bool],
) -> dict[str, Any]:
    """공통 no-harm 조건과 profile별 최소 조건만 조합해 판정한다."""
    durations = [duration for cell in cells for duration in cell["durations"]]
    no_new_unfulfilled = all(
        row["new_unfulfilled_request_count"] == 0 for row in durations
    )
    unfulfilled_no_worse = all(row["unfulfilled_delta"] <= 0 for row in durations)
    empty_no_worse = all(
        row["empty_station_minutes_delta"] <= _FLOAT_TOLERANCE for row in durations
    )
    by_minutes = {row["evaluation_minutes"]: row for row in aggregates}
    row_180 = by_minutes[180]
    aggregate_180_unfulfilled_strict = (
        row_180["candidate_unfulfilled_requests"]
        < row_180["baseline_unfulfilled_requests"]
    )
    strict_empty_checks = {
        str(minutes): (
            by_minutes[minutes]["candidate_empty_station_minutes"]
            < by_minutes[minutes]["baseline_empty_station_minutes"] - _FLOAT_TOLERANCE
        )
        for minutes in profile.gate.strict_empty_improvement_horizons
    }
    minimum_reduction = profile.gate.aggregate_180_empty_reduction_min_pct
    reduction_passed = (
        True
        if minimum_reduction is None
        else row_180["empty_station_minutes_reduction_pct"] + _FLOAT_TOLERANCE
        >= minimum_reduction
    )
    improved_minimum = profile.gate.improved_180_cells_min
    improved_cells_passed = (
        True
        if improved_minimum is None
        else row_180["improved_cell_count"] >= improved_minimum
    )
    observed_max_lag = max(
        (row["max_pickup_dispatch_lag_minutes"] for row in durations),
        default=0.0,
    )
    lag_limit = profile.gate.max_pickup_dispatch_lag_minutes
    pickup_lag_passed = (
        True
        if lag_limit is None
        else observed_max_lag <= lag_limit + _FLOAT_TOLERANCE
    )
    planned_equals_moved = all(
        row["planned_bikes"] == row["moved_bikes"] for row in durations
    )
    all_routes_finished = all(row["finished_by_cutoff"] for row in durations)
    checks = {
        "evidence_scope_matches": all(evidence_checks.values()),
        "every_cell_and_duration_unfulfilled_no_worse": unfulfilled_no_worse,
        "every_cell_and_duration_empty_station_minutes_no_worse": empty_no_worse,
        "aggregate_180m_unfulfilled_requirement": (
            aggregate_180_unfulfilled_strict
            if profile.gate.require_aggregate_180_unfulfilled_strict_improvement
            else True
        ),
        "strict_empty_improvement_horizons": all(strict_empty_checks.values()),
        "aggregate_180m_empty_reduction_requirement": reduction_passed,
        "improved_180m_cells_requirement": improved_cells_passed,
        "pickup_dispatch_lag_requirement": pickup_lag_passed,
        "planned_bikes_equal_moved_bikes": (
            planned_equals_moved
            if profile.gate.require_planned_bikes_equal_moved_bikes
            else True
        ),
        "all_routes_finished_by_cutoff": (
            all_routes_finished
            if profile.gate.require_all_routes_finished_by_cutoff
            else True
        ),
    }
    return {
        "passed": all(checks.values()),
        "kind": "release" if profile.release_gate else "diagnostic",
        "checks": checks,
        "diagnostics": {
            "every_cell_and_duration_new_unfulfilled_request_set_empty": (
                no_new_unfulfilled
            ),
        },
        "evidence_scope_checks": dict(evidence_checks),
        "strict_empty_improvement_by_horizon": strict_empty_checks,
        "aggregate_180m_unfulfilled_strict_improvement": (
            aggregate_180_unfulfilled_strict
        ),
        "aggregate_180m_empty_station_minutes_reduction_pct": row_180[
            "empty_station_minutes_reduction_pct"
        ],
        "aggregate_180m_empty_station_minutes_reduction_min_pct": minimum_reduction,
        "improved_180m_cell_count": row_180["improved_cell_count"],
        "improved_180m_cells_min": improved_minimum,
        "observed_max_pickup_dispatch_lag_minutes": round(observed_max_lag, 6),
        "max_pickup_dispatch_lag_minutes": lag_limit,
    }


def result_markdown(result: Mapping[str, Any]) -> str:
    """단일 결과 스키마를 profile에 관계없이 같은 표로 렌더링한다."""
    profile = _mapping(result.get("profile"), "result profile")
    gate = _mapping(result.get("acceptance_gate"), "result acceptance gate")
    lines = [
        f"# 재배치 정책 평가: {profile['name']}",
        "",
        f"- 목적: {profile['purpose']}",
        f"- 평가 셀: {result['cell_count']}개",
        f"- Primary metric: `{result['primary_metric']}`",
        f"- Gate 종류: `{gate['kind']}`",
        f"- Gate 통과: **{gate['passed']}**",
        "",
        (
            "| 구간 | 요청 | 충족률 baseline→후보 | 미충족 baseline→후보 | "
            "신규/해결 | 품절 분 baseline→후보 | 감소율 | 추정 기존 운영 범위 | "
            "이동/기존 예산 |"
        ),
        "|---:|---:|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for row in result["aggregates"]:
        lines.append(
            f"| {row['evaluation_minutes']}분 | {row['observed_requests']:,} | "
            f"{row['baseline_observed_demand_fulfillment_rate']:.4%}→"
            f"{row['candidate_observed_demand_fulfillment_rate']:.4%} | "
            f"{row['baseline_unfulfilled_requests']}→"
            f"{row['candidate_unfulfilled_requests']} | "
            f"{row['new_unfulfilled_request_count']}/"
            f"{row['resolved_unfulfilled_request_count']} | "
            f"{row['baseline_empty_station_minutes']:.1f}→"
            f"{row['candidate_empty_station_minutes']:.1f} | "
            f"{row['empty_station_minutes_reduction_pct']:.3f}% | "
            f"{row['legacy_empty_station_minutes_min']:.1f}~"
            f"{row['legacy_empty_station_minutes_max']:.1f} | "
            f"{row['moved_bikes']}/{row['legacy_movement_budget']} |"
        )
    lines.extend(
        (
            "",
            (
                "> 모든 profile은 같은 point-in-time 엔진과 결과 스키마를 사용한다. "
                "추정 기존 운영은 작업 시각과 route 로그가 없는 민감도 범위이며 "
                "실제 운영 대비 인과적 우월성 근거가 아니다."
            ),
            "",
        )
    )
    return "\n".join(lines)


def write_evaluation_result(
    result: Mapping[str, Any],
    output_dir: Path,
) -> tuple[Path, Path]:
    """Profile 결과를 이름이 고정된 JSON과 Markdown 한 쌍으로 저장한다."""
    profile = _mapping(result.get("profile"), "result profile")
    name = _nonblank(profile.get("name"), "profile name")
    output_dir.mkdir(parents=True, exist_ok=True)
    json_path = output_dir / f"{name}-evaluation.json"
    markdown_path = output_dir / f"{name}-evaluation.md"
    json_path.write_text(
        json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    markdown_path.write_text(result_markdown(result), encoding="utf-8")
    return json_path, markdown_path


def _input_provenance_sha256(source: Mapping[str, Any]) -> str:
    """코드 버전을 제외한 raw 입력 provenance의 canonical SHA-256을 계산한다."""
    audit = {
        "backtest_contract_version": source.get("backtest_contract_version"),
        "route_algorithm_version": source.get("route_algorithm_version"),
        "urgency_scoring_config_version": source.get(
            "urgency_scoring_config_version"
        ),
        "rental_csv_sha256": _source_sha(source.get("rental_csv"), "rental source"),
        "stock_csv_sha256": _source_sha(source.get("stock_csv"), "stock source"),
        "weather_csv_sha256": _source_sha(source.get("weather_csv"), "weather source"),
        "population_csv_sha256": [
            _source_sha(value, "population source")
            for value in _sequence(source.get("population_csvs"), "population sources")
        ],
        "station_master_content_sha256": source.get(
            "station_master_content_sha256"
        ),
        "station_crosswalk_count": source.get("station_crosswalk_count"),
        "station_crosswalk_sha256": source.get("station_crosswalk_sha256"),
        "population_excluded_station_count": source.get(
            "population_excluded_station_count"
        ),
        "population_excluded_grid_ids": list(
            _sequence(
                source.get("population_excluded_grid_ids"),
                "population excluded grid ids",
            )
        ),
    }
    value = {
        name: item for name, item in audit.items() if name not in _SOURCE_VERSION_FIELDS
    }
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def _source_sha(value: object, label: str) -> str:
    """SourceFile 문서에서 SHA-256 문자열을 추출한다."""
    source = _mapping(value, label)
    return _nonblank(source.get("sha256"), f"{label} SHA")


def _canonical(value: object) -> str:
    """설정과 provenance를 결정적으로 비교할 JSON 문자열을 만든다."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("JSON canonical 비교가 불가능한 값입니다.") from exc


def _mapping(value: object, label: str) -> Mapping[str, Any]:
    """값을 JSON object로 제한한다."""
    if not isinstance(value, Mapping):
        raise ValueError(f"{label}은 object여야 합니다.")
    return value


def _sequence(value: object, label: str) -> Sequence[Any]:
    """값을 문자열이 아닌 JSON array로 제한한다."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label}은 array여야 합니다.")
    return value


def _nonblank(value: object, label: str) -> str:
    """값을 trim된 nonblank 문자열로 제한한다."""
    if type(value) is not str or not value.strip() or value != value.strip():
        raise ValueError(f"{label}은 trim된 nonblank 문자열이어야 합니다.")
    return value


def _nonnegative_int(value: object, label: str) -> int:
    """bool이 아닌 0 이상 정수만 반환한다."""
    if type(value) is not int or value < 0:
        raise ValueError(f"{label}은 0 이상 정수여야 합니다.")
    return value


def _positive_int(value: object, label: str) -> int:
    """bool이 아닌 양의 정수만 반환한다."""
    result = _nonnegative_int(value, label)
    if result == 0:
        raise ValueError(f"{label}은 양의 정수여야 합니다.")
    return result


def _nonnegative_float(value: object, label: str) -> float:
    """bool이 아닌 0 이상 유한 숫자만 반환한다."""
    if type(value) not in (int, float) or float(value) < 0.0:
        raise ValueError(f"{label}은 0 이상 숫자여야 합니다.")
    result = float(value)
    if result == float("inf") or result != result:
        raise ValueError(f"{label}은 유한 숫자여야 합니다.")
    return result


def _aware_datetime(value: object, label: str) -> datetime:
    """Timezone offset이 있는 ISO-8601 시각만 반환한다."""
    text = _nonblank(value, label)
    try:
        result = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{label}이 ISO-8601이 아닙니다.") from exc
    if result.tzinfo is None or result.utcoffset() is None:
        raise ValueError(f"{label}에 timezone offset이 없습니다.")
    return result
