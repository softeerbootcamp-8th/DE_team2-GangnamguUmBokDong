"""사전 등록 confirmatory matrix와 결과를 fail-closed로 검증한다."""

from __future__ import annotations

import hashlib
import json
import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY, LEGACY_REBALANCE_POLICY
from gold.rebalance_route import MAX_STOPS_PER_ROUTE, ROUTE_ALGORITHM_VERSION

from .backtest_contract import (
    BACKTEST_CONTRACT_VERSION,
    EVIDENCE_GRADE,
    EvaluationContract,
)
from .production_policy_contract import (
    PRODUCTION_MODEL_BUNDLE_SHA256,
    PRODUCTION_POLICY_NAME,
    PRODUCTION_WEATHER_SHA256,
    production_policy_configuration,
)

MANIFEST_SCHEMA_VERSION = "confirmatory-matrix-v2"
CANDIDATE_LOCK_SCHEMA_VERSION = "confirmatory-candidate-lock-v2"
RUN_CLAIM_SCHEMA_VERSION = "confirmatory-run-claim-v2"
RESULT_SCHEMA_VERSION = "confirmatory-matrix-result-v2"
REGISTERED_MANIFEST_FILENAME = "confirmatory-matrix-v2.json"
REGISTERED_SIDECAR_FILENAME = "confirmatory-matrix-v2.sha256"
REGISTERED_MANIFEST_SHA256 = (
    "91f2bac169832fc7c39b855349d376d50deee93250c298fd0ba6fb6290ee1c97"
)
SUPERSEDED_MANIFEST_SHA256 = (
    "5949e4305ae33294a7b5a07efc1bd45063ae4e9f40c2d6349b3c956e51b0faf0"
)
REGISTERED_BRANCH = "feature/rebalance-policy-v3"
REGISTERED_DEVELOP_BASE_COMMIT = "3b2c87f69d3e4ee404cb1351a3f3cf883ff54da5"
SEOUL = ZoneInfo("Asia/Seoul")
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_GIT_COMMIT = re.compile(r"[0-9a-f]{40}\Z")
_FLOAT_TOLERANCE = 1e-9
_EXPECTED_CELLS = frozenset(
    {
        ("sangam", "2025-03-17", 7),
        ("sangam", "2025-06-17", 13),
        ("sangam", "2025-10-17", 18),
        ("jungnang", "2025-05-17", 7),
        ("jungnang", "2025-09-17", 13),
        ("jungnang", "2025-11-17", 18),
        ("cheonwang", "2025-03-17", 13),
        ("cheonwang", "2025-06-17", 18),
        ("cheonwang", "2025-10-17", 7),
        ("cheonho", "2025-05-17", 13),
        ("cheonho", "2025-09-17", 18),
        ("cheonho", "2025-11-17", 7),
    }
)
_EXPECTED_SELECTION_POLICY = {
    "calibration_centers_excluded": ["gaehwa", "hangnyeoul", "isu", "yeongnam"],
    "centers": ["cheonho", "cheonwang", "jungnang", "sangam"],
    "hours": [7, 13, 18],
    "candidate_must_be_locked_before_first_run": True,
    "matrix_must_not_change_after_registration": True,
}
_EXPECTED_EVALUATION_CONTRACT = {
    "evaluation_minutes": [60, 120, 180],
    "tick_minutes": 5,
    "fleet_size": 3,
    "truck_capacity": 20,
    "speed_kmh": 20.0,
    "service_minutes_per_stop": 3.0,
    "approval_delay_minutes": 0,
    "weather_publication_lag_minutes": 60,
    "population_lookback_weeks": 4,
    "model_bundle": "aws-temporary-model-2025-d20-h12-r20",
    "heldout_rule": "2025년 17일은 고정 모델 학습에서 제외된 test split이다.",
}
_EXPECTED_ACCEPTANCE_GATE = {
    "primary_metric": "observed_demand_fulfillment_rate",
    "every_cell_and_duration_new_unfulfilled_request_count_max": 0,
    "every_cell_and_duration_unfulfilled_delta_max": 0,
    "aggregate_180m_unfulfilled_delta_max_exclusive": 0,
    "every_cell_and_duration_max_pickup_dispatch_lag_minutes": 30.0,
    "every_cell_and_duration_empty_station_minutes_delta_max": 0.0,
    "aggregate_180m_empty_station_minutes_reduction_min_pct": 5.0,
    "improved_180m_cells_min": 8,
    "planned_bikes_must_equal_moved_bikes": True,
    "all_routes_must_finish_by_cutoff": True,
    "single_confirmatory_run": True,
}
_MANIFEST_KEYS = frozenset(
    {
        "schema_version",
        "registered_at_utc",
        "registration_state",
        "supersedes_manifest_sha256",
        "purpose",
        "branch",
        "develop_base_commit",
        "selection_policy",
        "evaluation_contract",
        "acceptance_gate",
        "cells",
    }
)
_SOURCE_PROVENANCE_KEYS = frozenset(
    {
        "rental_csv",
        "stock_csv",
        "weather_csv",
        "population_csvs",
        "station_master_content_sha256",
        "station_crosswalk_count",
        "station_crosswalk_sha256",
        "population_excluded_station_count",
        "population_excluded_grid_ids",
        "backtest_contract_version",
        "route_algorithm_version",
        "urgency_scoring_config_version",
    }
)
_RAW_RESULT_KEYS = frozenset(
    {
        "evidence_grade",
        "target_date",
        "center_id",
        "center_name",
        "start_hour",
        "model_bundle_root",
        "model_bundle_sha256",
        "source_trip_count",
        "source_provenance",
        "evidence_gate",
        "contracts",
        "durations",
    }
)
_DURATION_KEYS = frozenset(
    {
        "evaluation_minutes",
        "station_count",
        "legacy_movement",
        "legacy_timing",
        "no_rebalance",
        "model_policies",
    }
)
_POLICY_METRIC_KEYS = frozenset(
    {
        "policy",
        "policy_configuration",
        "window_start",
        "window_end",
        "observed_requests",
        "fulfilled_requests",
        "unfulfilled_requests",
        "observed_demand_fulfillment_rate",
        "empty_station_minutes",
        "moved_bikes",
        "planned_bikes",
        "dispatched_routes",
        "completed_routes_by_cutoff",
        "trucks_still_busy_at_cutoff",
        "executed_stops",
        "vehicle_busy_minutes",
        "decision_ticks",
        "movement_budget",
        "movement_budget_used",
        "cold_start_stock_history_minutes",
        "unfulfilled_request_log",
        "job_audits",
        "tick_audits",
    }
)
_UNFULFILLED_REQUEST_KEYS = frozenset({"bike_id", "rented_at", "station_no"})
_JOB_AUDIT_KEYS = frozenset(
    {
        "route_id",
        "truck_id",
        "dispatched_at",
        "completed_at",
        "return_at",
        "planned_bikes",
        "moved_bikes",
        "stop_count",
        "stops",
    }
)
_STOP_AUDIT_KEYS = frozenset(
    {
        "visit_no",
        "station_no",
        "station_id",
        "action",
        "executed_at",
        "planned_quantity",
        "actual_quantity",
    }
)
_REQUIRED_TRUE_EVIDENCE = frozenset(
    {
        "point_in_time_feature_inputs",
        "operation_contract_passed",
        "legacy_endpoint_reconciliation_passed",
        "heldout_day_of_month",
        "same_bike_movement_budget_cap_enforced",
    }
)


@dataclass(frozen=True, slots=True)
class ConfirmatoryCell:
    """사전 등록된 센터·날짜·시각 한 셀을 표현한다."""

    center_id: str
    target_date: date
    start_hour: int

    @property
    def key(self) -> tuple[str, str, int]:
        """Manifest 및 raw 결과 비교용 JSON key tuple을 반환한다."""
        return (self.center_id, self.target_date.isoformat(), self.start_hour)

    @property
    def slug(self) -> str:
        """충돌 없는 결과 파일 stem을 반환한다."""
        return f"{self.target_date.isoformat()}-{self.center_id}-{self.start_hour:02d}h"


@dataclass(frozen=True, slots=True)
class ConfirmatoryManifest:
    """검증된 사전 등록 matrix와 원본 SHA를 묶는다."""

    sha256: str
    document: Mapping[str, Any]
    cells: tuple[ConfirmatoryCell, ...]


@dataclass(frozen=True, slots=True)
class CandidateLock:
    """한 번 고정된 실행 코드와 단일 정책 후보를 표현한다."""

    path: str
    sha256: str
    git_commit: str
    document: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RawResultArtifact:
    """중복 key 검사를 통과한 raw 결과와 파일 내용을 묶는다."""

    path: str
    sha256: str
    document: Mapping[str, Any]


@dataclass(frozen=True, slots=True)
class RunClaim:
    """Candidate lock에 결속된 단 한 번의 confirmatory 실행 claim을 표현한다."""

    path: str
    sha256: str
    document: Mapping[str, Any]


def load_confirmatory_manifest(
    manifest_path: Path,
    sidecar_path: Path | None = None,
) -> ConfirmatoryManifest:
    """등록 SHA sidecar와 exact semantic 계약을 모두 검증해 manifest를 읽는다."""
    actual_sidecar = (
        manifest_path.with_suffix(".sha256")
        if sidecar_path is None
        else sidecar_path
    )
    if manifest_path.name != REGISTERED_MANIFEST_FILENAME:
        raise ValueError(
            f"confirmatory manifest 이름은 {REGISTERED_MANIFEST_FILENAME}이어야 합니다."
        )
    if actual_sidecar.name != REGISTERED_SIDECAR_FILENAME:
        raise ValueError(
            f"confirmatory sidecar 이름은 {REGISTERED_SIDECAR_FILENAME}이어야 합니다."
        )
    payload = manifest_path.read_bytes()
    actual_sha256 = hashlib.sha256(payload).hexdigest()
    expected_sidecar = (
        f"{REGISTERED_MANIFEST_SHA256}  {REGISTERED_MANIFEST_FILENAME}\n"
    ).encode("ascii")
    if actual_sidecar.read_bytes() != expected_sidecar:
        raise ValueError("confirmatory manifest SHA sidecar가 등록값과 exact하지 않습니다.")
    if actual_sha256 != REGISTERED_MANIFEST_SHA256:
        raise ValueError(
            "confirmatory manifest 내용 SHA가 사전 등록값과 다릅니다: "
            f"expected={REGISTERED_MANIFEST_SHA256}, actual={actual_sha256}"
        )
    document = _loads_strict_json(payload, "confirmatory manifest")
    cells = _validate_manifest_document(document)
    return ConfirmatoryManifest(actual_sha256, document, cells)


def candidate_lock_document(manifest_sha256: str, git_commit: str) -> dict[str, Any]:
    """현재 production 후보 하나만 표현하는 exact lock 문서를 만든다."""
    _require_sha256(manifest_sha256, "candidate lock manifest_sha256")
    _require_git_commit(git_commit, "candidate lock git_commit")
    return {
        "schema_version": CANDIDATE_LOCK_SCHEMA_VERSION,
        "manifest_sha256": manifest_sha256,
        "git_commit": git_commit,
        "candidate": {
            "policy": PRODUCTION_POLICY_NAME,
            "policy_configuration": DEFAULT_REBALANCE_POLICY.audit_document(),
            "max_stops_per_route": MAX_STOPS_PER_ROUTE,
        },
    }


def write_candidate_lock(
    path: Path,
    *,
    manifest_sha256: str,
    git_commit: str,
) -> CandidateLock:
    """후보 lock을 배타적으로 한 번만 생성하고 다시 읽어 검증한다."""
    document = candidate_lock_document(manifest_sha256, git_commit)
    payload = _pretty_json_bytes(document)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise ValueError(f"candidate lock이 이미 존재합니다: {path}") from exc
    return load_candidate_lock(
        path,
        expected_manifest_sha256=manifest_sha256,
        expected_git_commit=git_commit,
    )


def load_candidate_lock(
    path: Path,
    *,
    expected_manifest_sha256: str,
    expected_git_commit: str,
) -> CandidateLock:
    """후보 lock의 duplicate key·manifest·commit·정책 exact 값을 검증한다."""
    payload = path.read_bytes()
    document = _loads_strict_json(payload, "candidate lock")
    _validate_candidate_lock_document(
        document,
        expected_manifest_sha256=expected_manifest_sha256,
        expected_git_commit=expected_git_commit,
    )
    return CandidateLock(
        path=str(path),
        sha256=hashlib.sha256(payload).hexdigest(),
        git_commit=expected_git_commit,
        document=document,
    )


def run_claim_document(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> dict[str, Any]:
    """Manifest와 candidate lock에 결속된 exact 단일 실행 claim을 만든다."""
    return {
        "schema_version": RUN_CLAIM_SCHEMA_VERSION,
        "manifest_sha256": manifest.sha256,
        "candidate_lock_sha256": candidate_lock.sha256,
        "git_commit": candidate_lock.git_commit,
        "candidate": candidate_lock.document["candidate"],
    }


def load_run_claim(
    path: Path,
    *,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> RunClaim:
    """실행 claim의 duplicate key와 manifest·candidate exact 결속을 검증한다."""
    payload = path.read_bytes()
    document = _loads_strict_json(payload, "confirmatory run claim")
    _require_canonical_equal(
        document,
        run_claim_document(manifest, candidate_lock),
        "confirmatory run claim",
    )
    return RunClaim(
        path=str(path),
        sha256=hashlib.sha256(payload).hexdigest(),
        document=document,
    )


def load_raw_result(path: Path) -> RawResultArtifact:
    """Raw JSON의 byte SHA와 duplicate key 검증 결과를 반환한다."""
    payload = path.read_bytes()
    return RawResultArtifact(
        path=str(path),
        sha256=hashlib.sha256(payload).hexdigest(),
        document=_loads_strict_json(payload, f"raw result {path}"),
    )


def validate_confirmatory_results(
    artifacts: Sequence[RawResultArtifact],
    *,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> dict[str, Any]:
    """Raw 셀 exact set과 KPI gate를 검증해 결정적 최종 문서를 만든다."""
    current_lock = load_candidate_lock(
        Path(candidate_lock.path),
        expected_manifest_sha256=manifest.sha256,
        expected_git_commit=candidate_lock.git_commit,
    )
    if current_lock.sha256 != candidate_lock.sha256:
        raise ValueError("검증 중 candidate lock byte SHA가 변경됐습니다.")
    current_claim = load_run_claim(
        Path(run_claim.path),
        manifest=manifest,
        candidate_lock=current_lock,
    )
    if current_claim.sha256 != run_claim.sha256:
        raise ValueError("검증 중 confirmatory run claim byte SHA가 변경됐습니다.")
    if not isinstance(artifacts, Sequence) or isinstance(
        artifacts, (str, bytes, bytearray)
    ):
        raise ValueError("raw result artifacts는 sequence여야 합니다.")
    summaries: list[dict[str, Any]] = []
    seen_cells: set[tuple[str, str, int]] = set()
    source_by_date: dict[str, str] = {}
    model_root: str | None = None
    for artifact in artifacts:
        if type(artifact) is not RawResultArtifact:
            raise ValueError("raw result artifact 타입이 잘못됐습니다.")
        _require_nonblank(artifact.path, "raw result artifact path")
        _require_sha256(artifact.sha256, "raw result artifact sha256")
        key = _raw_cell_key(artifact.document)
        if key not in _EXPECTED_CELLS:
            raise ValueError(f"사전 등록되지 않은 confirmatory 셀입니다: {key}")
        if key in seen_cells:
            raise ValueError(f"중복 confirmatory raw 셀입니다: {key}")
        seen_cells.add(key)
        cell = ConfirmatoryCell(key[0], date.fromisoformat(key[1]), key[2])
        summary, source_signature, current_model_root = (
            _validate_raw_result(
                artifact.document,
                artifact=artifact,
                expected_cell=cell,
                manifest=manifest,
            )
        )
        previous_source = source_by_date.setdefault(key[1], source_signature)
        if previous_source != source_signature:
            raise ValueError(
                f"같은 날짜 raw 결과의 source provenance가 다릅니다: {key[1]}"
            )
        if model_root is None:
            model_root = current_model_root
        elif model_root != current_model_root:
            raise ValueError("raw 결과의 model_bundle_root가 셀마다 다릅니다.")
        summaries.append(summary)
    if seen_cells != _EXPECTED_CELLS:
        missing = sorted(_EXPECTED_CELLS - seen_cells)
        extra = sorted(seen_cells - _EXPECTED_CELLS)
        raise ValueError(
            "confirmatory raw 셀 집합이 manifest와 다릅니다: "
            f"missing={missing}, extra={extra}"
        )
    summaries.sort(
        key=lambda row: (
            row["center_id"].encode("utf-8"),
            row["target_date"],
            row["start_hour"],
        )
    )
    gate = _evaluate_acceptance_gate(summaries, manifest.document["acceptance_gate"])
    assert model_root is not None
    return {
        "schema_version": RESULT_SCHEMA_VERSION,
        "manifest_sha256": manifest.sha256,
        "candidate_lock_sha256": candidate_lock.sha256,
        "run_claim_sha256": run_claim.sha256,
        "git_commit": candidate_lock.git_commit,
        "candidate": candidate_lock.document["candidate"],
        "model_bundle_root": model_root,
        "model_bundle_sha256": PRODUCTION_MODEL_BUNDLE_SHA256,
        "source_provenance_sha256_by_cell": {
            (
                f"{row['center_id']}|{row['target_date']}|"
                f"{row['start_hour']:02d}"
            ): row["source_provenance_sha256"]
            for row in summaries
        },
        "station_surface_sha256_by_cell": {
            (
                f"{row['center_id']}|{row['target_date']}|"
                f"{row['start_hour']:02d}"
            ): row["station_master_content_sha256"]
            for row in summaries
        },
        "cell_count": len(summaries),
        "cells": summaries,
        "acceptance_gate": gate,
    }


def result_markdown(result: Mapping[str, Any]) -> str:
    """Confirmatory gate와 셀별 delta를 검토 가능한 Markdown으로 만든다."""
    gate = _require_mapping(result.get("acceptance_gate"), "result acceptance_gate")
    lines = [
        "# 재배치 정책 Confirmatory Matrix",
        "",
        f"- Manifest SHA-256: `{result['manifest_sha256']}`",
        f"- Candidate lock SHA-256: `{result['candidate_lock_sha256']}`",
        f"- Run claim SHA-256: `{result['run_claim_sha256']}`",
        f"- Git commit: `{result['git_commit']}`",
        f"- Candidate: `{result['candidate']['policy']}`",
        f"- 최종 통과: **{gate['passed']}**",
        (
            "- 모든 셀·구간 신규 미충족 요청 0건: "
            f"**{gate['every_cell_and_duration_new_unfulfilled_request_set_empty']}**"
        ),
        (
            "- 180분 aggregate 미충족 요청 변화: "
            f"**{gate['aggregate_180m_unfulfilled_delta']:+d}** "
            f"(엄격 개선: {gate['aggregate_180m_unfulfilled_strict_improvement']})"
        ),
        (
            "- 180분 aggregate 품절 대여소-분 감소율: "
            f"**{gate['aggregate_180m_empty_station_minutes_reduction_pct']:.3f}%**"
        ),
        (
            "- Pickup dispatch→실행 최대 지연: "
            f"**{gate['observed_max_pickup_dispatch_lag_minutes']:.3f}분** "
            "(상한 "
            f"{gate['every_cell_and_duration_max_pickup_dispatch_lag_minutes']:.1f}분)"
        ),
        (
            "- 180분 품절 시간이 엄격히 개선된 셀: "
            f"**{gate['improved_180m_cell_count']}/{result['cell_count']}**"
        ),
        "",
        (
            "| 센터 | 날짜 | 시작 | 구간 | 미충족 Δ | 신규/해결 요청 | "
            "신규 실패 대여소 | pickup 지연 | 품절 대여소-분 Δ | 계획/실행 | cutoff 완료 |"
        ),
        "|---|---|---:|---:|---:|---:|---|---:|---:|---:|---|",
    ]
    for cell in result["cells"]:
        for duration in cell["durations"]:
            new_stations = ", ".join(
                str(value) for value in duration["new_unfulfilled_station_nos"]
            ) or "-"
            lines.append(
                f"| {cell['center_id']} | {cell['target_date']} | "
                f"{cell['start_hour']:02d}:00 | {duration['evaluation_minutes']} | "
                f"{duration['unfulfilled_delta']:+d} | "
                f"{duration['new_unfulfilled_request_count']}/"
                f"{duration['resolved_unfulfilled_request_count']} | "
                f"{new_stations} | "
                f"{duration['max_pickup_dispatch_lag_minutes']:.3f}분 | "
                f"{duration['empty_station_minutes_delta']:+.3f} | "
                f"{duration['planned_bikes']}/{duration['moved_bikes']} | "
                f"{duration['finished_by_cutoff']} |"
            )
    lines.extend(
        (
            "",
            (
                "> 신규 미충족 요청은 `(bike_id, rented_at, station_no)` event가 "
                "candidate에만 존재하는 경우다. 총 미충족이 같아도 실패 요청을 다른 "
                "대여소나 이용자로 전가하면 통과하지 않는다. Pickup 지연은 route "
                "배차시각부터 pickup stop 실행시각까지만 계산하며 완료·센터 복귀시간은 "
                "섞지 않는다. 개선 셀은 180분 candidate의 "
                "품절 대여소-분이 no-rebalance보다 엄격히 작은 셀로 계산한다."
            ),
            "",
        )
    )
    return "\n".join(lines)


def write_confirmatory_result(
    result: Mapping[str, Any],
    *,
    json_path: Path,
    markdown_path: Path,
) -> tuple[Path, Path]:
    """최종 JSON과 Markdown을 기존 파일을 덮지 않고 한 번만 기록한다."""
    if json_path.exists() or markdown_path.exists():
        raise ValueError("confirmatory 최종 결과 파일이 이미 존재합니다.")
    json_path.parent.mkdir(parents=True, exist_ok=True)
    markdown_path.parent.mkdir(parents=True, exist_ok=True)
    json_payload = _pretty_json_bytes(result)
    markdown_payload = result_markdown(result).encode("utf-8")
    with json_path.open("xb") as stream:
        stream.write(json_payload)
    try:
        with markdown_path.open("xb") as stream:
            stream.write(markdown_payload)
    except Exception:
        json_path.unlink(missing_ok=True)
        raise
    return json_path, markdown_path


def _validate_manifest_document(
    document: Mapping[str, Any],
) -> tuple[ConfirmatoryCell, ...]:
    """Manifest의 exact selection·운영·acceptance 계약을 검증한다."""
    _require_exact_keys(document, _MANIFEST_KEYS, "confirmatory manifest")
    if document.get("schema_version") != MANIFEST_SCHEMA_VERSION:
        raise ValueError("confirmatory manifest schema_version이 잘못됐습니다.")
    if document.get("supersedes_manifest_sha256") != SUPERSEDED_MANIFEST_SHA256:
        raise ValueError("confirmatory manifest가 등록된 v1 SHA를 계승하지 않습니다.")
    if document.get("registration_state") != "locked_before_candidate_evaluation":
        raise ValueError("confirmatory manifest registration_state가 잠기지 않았습니다.")
    if document.get("branch") != REGISTERED_BRANCH:
        raise ValueError("confirmatory manifest branch가 등록값과 다릅니다.")
    if document.get("develop_base_commit") != REGISTERED_DEVELOP_BASE_COMMIT:
        raise ValueError("confirmatory manifest develop base commit이 다릅니다.")
    _require_nonblank(document.get("purpose"), "confirmatory manifest purpose")
    registered_at = _require_nonblank(
        document.get("registered_at_utc"), "confirmatory manifest registered_at_utc"
    )
    try:
        parsed_registered_at = datetime.fromisoformat(
            registered_at.replace("Z", "+00:00")
        )
    except ValueError as exc:
        raise ValueError("confirmatory registered_at_utc가 ISO-8601이 아닙니다.") from exc
    if (
        not registered_at.endswith("Z")
        or parsed_registered_at.utcoffset() != timedelta(0)
    ):
        raise ValueError("confirmatory registered_at_utc는 UTC Z 시각이어야 합니다.")
    selection = _require_mapping(
        document.get("selection_policy"), "confirmatory selection_policy"
    )
    _require_exact_keys(
        selection,
        frozenset({*_EXPECTED_SELECTION_POLICY, "rationale"}),
        "confirmatory selection_policy",
    )
    for name, expected in _EXPECTED_SELECTION_POLICY.items():
        _require_canonical_equal(
            selection.get(name), expected, f"selection_policy {name}"
        )
    _require_nonblank(selection.get("rationale"), "selection_policy rationale")
    contract = _require_mapping(
        document.get("evaluation_contract"), "confirmatory evaluation_contract"
    )
    _require_canonical_equal(
        contract,
        _EXPECTED_EVALUATION_CONTRACT,
        "confirmatory evaluation_contract",
    )
    acceptance = _require_mapping(
        document.get("acceptance_gate"), "confirmatory acceptance_gate"
    )
    _require_canonical_equal(
        acceptance,
        _EXPECTED_ACCEPTANCE_GATE,
        "confirmatory acceptance_gate",
    )
    values = _require_sequence(document.get("cells"), "confirmatory cells")
    cells: list[ConfirmatoryCell] = []
    keys: list[tuple[str, str, int]] = []
    for index, value in enumerate(values):
        row = _require_mapping(value, f"confirmatory cells[{index}]")
        _require_exact_keys(
            row,
            frozenset({"center_id", "target_date", "start_hour"}),
            f"confirmatory cells[{index}]",
        )
        center_id = _require_nonblank(row.get("center_id"), "cell center_id")
        target_text = _require_nonblank(row.get("target_date"), "cell target_date")
        try:
            target = date.fromisoformat(target_text)
        except ValueError as exc:
            raise ValueError(f"confirmatory cell 날짜가 잘못됐습니다: {target_text}") from exc
        start_hour = row.get("start_hour")
        if type(start_hour) is not int or not 0 <= start_hour <= 23:
            raise ValueError("confirmatory cell start_hour가 0..23 정수가 아닙니다.")
        cell = ConfirmatoryCell(center_id, target, start_hour)
        cells.append(cell)
        keys.append(cell.key)
    if len(keys) != len(set(keys)):
        raise ValueError("confirmatory manifest에 중복 셀이 있습니다.")
    if frozenset(keys) != _EXPECTED_CELLS:
        raise ValueError(
            "confirmatory manifest 셀 exact set이 등록값과 다릅니다: "
            f"missing={sorted(_EXPECTED_CELLS - set(keys))}, "
            f"extra={sorted(set(keys) - _EXPECTED_CELLS)}"
        )
    return tuple(cells)


def _validate_candidate_lock_document(
    document: Mapping[str, Any],
    *,
    expected_manifest_sha256: str,
    expected_git_commit: str,
) -> None:
    """Candidate lock이 현재 code·manifest·production 정책 하나와 같은지 검증한다."""
    expected = candidate_lock_document(expected_manifest_sha256, expected_git_commit)
    _require_canonical_equal(document, expected, "candidate lock")


def _raw_cell_key(document: Mapping[str, Any]) -> tuple[str, str, int]:
    """Raw top-level에서 검증 가능한 confirmatory cell key를 추출한다."""
    center_id = _require_nonblank(document.get("center_id"), "raw center_id")
    target_date = _require_nonblank(document.get("target_date"), "raw target_date")
    try:
        date.fromisoformat(target_date)
    except ValueError as exc:
        raise ValueError(f"raw target_date가 잘못됐습니다: {target_date}") from exc
    start_hour = document.get("start_hour")
    if type(start_hour) is not int or not 0 <= start_hour <= 23:
        raise ValueError("raw start_hour가 0..23 정수가 아닙니다.")
    return (center_id, target_date, start_hour)


def _validate_raw_result(
    document: Mapping[str, Any],
    *,
    artifact: RawResultArtifact,
    expected_cell: ConfirmatoryCell,
    manifest: ConfirmatoryManifest,
) -> tuple[dict[str, Any], str, str]:
    """한 raw 결과의 셀·모델·source·정책·운영 계약을 exact 검증한다."""
    _require_exact_keys(document, _RAW_RESULT_KEYS, f"raw {expected_cell.slug}")
    if _raw_cell_key(document) != expected_cell.key:
        raise ValueError(f"raw 결과와 기대 셀이 다릅니다: {expected_cell.slug}")
    if document.get("evidence_grade") != EVIDENCE_GRADE:
        raise ValueError(f"{expected_cell.slug} evidence_grade가 다릅니다.")
    _require_nonblank(document.get("center_name"), f"{expected_cell.slug} center_name")
    source_trip_count = document.get("source_trip_count")
    if type(source_trip_count) is not int or source_trip_count <= 0:
        raise ValueError(f"{expected_cell.slug} source_trip_count가 양수가 아닙니다.")
    model_root = _require_nonblank(
        document.get("model_bundle_root"), f"{expected_cell.slug} model_bundle_root"
    )
    expected_model_name = manifest.document["evaluation_contract"]["model_bundle"]
    if Path(model_root).name != expected_model_name:
        raise ValueError(
            f"{expected_cell.slug} model bundle 이름이 manifest와 다릅니다."
        )
    if document.get("model_bundle_sha256") != PRODUCTION_MODEL_BUNDLE_SHA256:
        raise ValueError(f"{expected_cell.slug} model bundle SHA가 고정값과 다릅니다.")
    _validate_evidence_gate(document.get("evidence_gate"), expected_cell)
    expected_contracts = tuple(
        EvaluationContract(
            target_date=expected_cell.target_date,
            start_hour=expected_cell.start_hour,
            evaluation_minutes=minutes,
            tick_minutes=manifest.document["evaluation_contract"]["tick_minutes"],
            fleet_size=manifest.document["evaluation_contract"]["fleet_size"],
            truck_capacity=manifest.document["evaluation_contract"]["truck_capacity"],
            speed_kmh=manifest.document["evaluation_contract"]["speed_kmh"],
            service_minutes_per_stop=manifest.document["evaluation_contract"][
                "service_minutes_per_stop"
            ],
            approval_delay_minutes=manifest.document["evaluation_contract"][
                "approval_delay_minutes"
            ],
            weather_publication_lag_minutes=manifest.document[
                "evaluation_contract"
            ]["weather_publication_lag_minutes"],
            population_lookback_weeks=manifest.document["evaluation_contract"][
                "population_lookback_weeks"
            ],
        ).audit_document()
        for minutes in manifest.document["evaluation_contract"]["evaluation_minutes"]
    )
    _require_canonical_equal(
        document.get("contracts"),
        expected_contracts,
        f"{expected_cell.slug} contracts",
    )
    source_signature, source_sha256, station_sha = _validate_source_provenance(
        document.get("source_provenance"), expected_cell
    )
    durations = _require_sequence(
        document.get("durations"), f"{expected_cell.slug} durations"
    )
    expected_minutes = tuple(
        manifest.document["evaluation_contract"]["evaluation_minutes"]
    )
    summaries: list[dict[str, Any]] = []
    seen_minutes: set[int] = set()
    station_count: int | None = None
    for index, value in enumerate(durations):
        duration = _require_mapping(value, f"{expected_cell.slug} durations[{index}]")
        _require_exact_keys(
            duration,
            _DURATION_KEYS,
            f"{expected_cell.slug} durations[{index}]",
        )
        minutes = duration.get("evaluation_minutes")
        if type(minutes) is not int or minutes not in expected_minutes:
            raise ValueError(f"{expected_cell.slug} evaluation_minutes가 잘못됐습니다.")
        if minutes in seen_minutes:
            raise ValueError(f"{expected_cell.slug}에 중복 duration이 있습니다: {minutes}")
        seen_minutes.add(minutes)
        current_station_count = duration.get("station_count")
        if type(current_station_count) is not int or current_station_count <= 0:
            raise ValueError(f"{expected_cell.slug} station_count가 양수가 아닙니다.")
        if station_count is None:
            station_count = current_station_count
        elif station_count != current_station_count:
            raise ValueError(f"{expected_cell.slug} duration별 station_count가 다릅니다.")
        _require_mapping(
            duration.get("legacy_movement"),
            f"{expected_cell.slug} {minutes}분 legacy_movement",
        )
        _require_sequence(
            duration.get("legacy_timing"),
            f"{expected_cell.slug} {minutes}분 legacy_timing",
        )
        baseline = _require_mapping(
            duration.get("no_rebalance"),
            f"{expected_cell.slug} {minutes}분 no_rebalance",
        )
        candidates = _require_sequence(
            duration.get("model_policies"),
            f"{expected_cell.slug} {minutes}분 model_policies",
        )
        if len(candidates) != 1:
            raise ValueError(
                f"{expected_cell.slug} {minutes}분에는 candidate가 정확히 하나여야 합니다."
            )
        candidate = _require_mapping(
            candidates[0], f"{expected_cell.slug} {minutes}분 candidate"
        )
        baseline_summary = _validate_policy_metrics(
            baseline,
            expected_cell=expected_cell,
            minutes=minutes,
            expected_policy="no_rebalance",
            expected_configuration={
                **LEGACY_REBALANCE_POLICY.audit_document(),
                "max_stops_per_route": MAX_STOPS_PER_ROUTE,
            },
        )
        candidate_summary = _validate_policy_metrics(
            candidate,
            expected_cell=expected_cell,
            minutes=minutes,
            expected_policy=PRODUCTION_POLICY_NAME,
            expected_configuration=production_policy_configuration(),
        )
        if (
            candidate_summary["observed_requests"]
            != baseline_summary["observed_requests"]
        ):
            raise ValueError(
                f"{expected_cell.slug} {minutes}분 candidate와 baseline 관측 수요가 다릅니다."
            )
        baseline_unfulfilled = baseline_summary["unfulfilled_request_keys"]
        candidate_unfulfilled = candidate_summary["unfulfilled_request_keys"]
        new_unfulfilled = candidate_unfulfilled - baseline_unfulfilled
        resolved_unfulfilled = baseline_unfulfilled - candidate_unfulfilled
        summaries.append(
            {
                "evaluation_minutes": minutes,
                "baseline_unfulfilled_requests": baseline_summary[
                    "unfulfilled_requests"
                ],
                "candidate_unfulfilled_requests": candidate_summary[
                    "unfulfilled_requests"
                ],
                "unfulfilled_delta": (
                    candidate_summary["unfulfilled_requests"]
                    - baseline_summary["unfulfilled_requests"]
                ),
                "new_unfulfilled_request_count": len(new_unfulfilled),
                "resolved_unfulfilled_request_count": len(resolved_unfulfilled),
                "new_unfulfilled_station_nos": sorted(
                    {key[2] for key in new_unfulfilled}
                ),
                "resolved_unfulfilled_station_nos": sorted(
                    {key[2] for key in resolved_unfulfilled}
                ),
                "max_pickup_dispatch_lag_minutes": candidate_summary[
                    "max_pickup_dispatch_lag_minutes"
                ],
                "baseline_empty_station_minutes": baseline_summary[
                    "empty_station_minutes"
                ],
                "candidate_empty_station_minutes": candidate_summary[
                    "empty_station_minutes"
                ],
                "empty_station_minutes_delta": round(
                    candidate_summary["empty_station_minutes"]
                    - baseline_summary["empty_station_minutes"],
                    3,
                ),
                "planned_bikes": candidate_summary["planned_bikes"],
                "moved_bikes": candidate_summary["moved_bikes"],
                "dispatched_routes": candidate_summary["dispatched_routes"],
                "completed_routes_by_cutoff": candidate_summary[
                    "completed_routes_by_cutoff"
                ],
                "trucks_still_busy_at_cutoff": candidate_summary[
                    "trucks_still_busy_at_cutoff"
                ],
                "finished_by_cutoff": (
                    candidate_summary["completed_routes_by_cutoff"]
                    == candidate_summary["dispatched_routes"]
                    and candidate_summary["trucks_still_busy_at_cutoff"] == 0
                ),
            }
        )
    if seen_minutes != set(expected_minutes):
        raise ValueError(f"{expected_cell.slug} duration exact set이 다릅니다.")
    summaries.sort(key=lambda row: row["evaluation_minutes"])
    assert station_count is not None
    return (
        {
            "center_id": expected_cell.center_id,
            "target_date": expected_cell.target_date.isoformat(),
            "start_hour": expected_cell.start_hour,
            "station_count": station_count,
            "raw_result_path": artifact.path,
            "raw_result_sha256": artifact.sha256,
            "source_provenance_sha256": source_sha256,
            "station_master_content_sha256": station_sha,
            "durations": summaries,
        },
        source_signature,
        model_root,
    )


def _validate_evidence_gate(value: object, cell: ConfirmatoryCell) -> None:
    """Raw point-in-time 필수 근거 gate가 exact true인지 검증한다."""
    gate = _require_mapping(value, f"{cell.slug} evidence_gate")
    for name in _REQUIRED_TRUE_EVIDENCE:
        if gate.get(name) is not True:
            raise ValueError(f"{cell.slug} evidence gate가 false입니다: {name}")


def _validate_source_provenance(
    value: object,
    cell: ConfirmatoryCell,
) -> tuple[str, str, str]:
    """Raw 원천 hash·코드 version을 검증하고 날짜 공통 signature를 만든다."""
    source = _require_mapping(value, f"{cell.slug} source_provenance")
    _require_exact_keys(
        source,
        _SOURCE_PROVENANCE_KEYS,
        f"{cell.slug} source_provenance",
    )
    expected_versions = {
        "backtest_contract_version": BACKTEST_CONTRACT_VERSION,
        "route_algorithm_version": ROUTE_ALGORITHM_VERSION,
        "urgency_scoring_config_version": URGENCY_SCORING_CONFIG_VERSION,
    }
    for name, expected in expected_versions.items():
        if source.get(name) != expected:
            raise ValueError(f"{cell.slug} source provenance {name}이 다릅니다.")
    rental = _validate_source_file(source.get("rental_csv"), f"{cell.slug} rental_csv")
    stock = _validate_source_file(source.get("stock_csv"), f"{cell.slug} stock_csv")
    weather = _validate_source_file(
        source.get("weather_csv"), f"{cell.slug} weather_csv"
    )
    month = f"{cell.target_date.year % 100:02d}{cell.target_date.month:02d}"
    expected_rental_name = f"서울특별시 공공자전거 대여이력 정보_{month}.csv"
    expected_stock_name = f"대여소별 공공자전거 대여가능 수량_{month}.csv"
    if Path(rental["path"]).name != expected_rental_name:
        raise ValueError(f"{cell.slug} rental CSV 월이 target_date와 다릅니다.")
    if Path(stock["path"]).name != expected_stock_name:
        raise ValueError(f"{cell.slug} stock CSV 월이 target_date와 다릅니다.")
    if Path(weather["path"]).name != "weather_realtime_2025.csv":
        raise ValueError(f"{cell.slug} weather CSV 파일명이 고정 입력과 다릅니다.")
    if weather["sha256"] != PRODUCTION_WEATHER_SHA256:
        raise ValueError(f"{cell.slug} weather CSV SHA가 production 고정값과 다릅니다.")
    population_values = _require_sequence(
        source.get("population_csvs"), f"{cell.slug} population_csvs"
    )
    if not population_values:
        raise ValueError(f"{cell.slug} population_csvs가 비어 있습니다.")
    populations = [
        _validate_source_file(row, f"{cell.slug} population_csvs[{index}]")
        for index, row in enumerate(population_values)
    ]
    if len({row["path"] for row in populations}) != len(populations):
        raise ValueError(f"{cell.slug} population_csvs 경로가 중복됩니다.")
    if [row["path"] for row in populations] != sorted(
        row["path"] for row in populations
    ):
        raise ValueError(f"{cell.slug} population_csvs 경로가 정렬되지 않았습니다.")
    if any(
        re.fullmatch(r"250_LOCAL_RESD_[0-9]{8}\.csv", Path(row["path"]).name)
        is None
        for row in populations
    ):
        raise ValueError(f"{cell.slug} population CSV 파일명이 고정 형식과 다릅니다.")
    population_dates = tuple(
        datetime.strptime(
            Path(row["path"]).stem.removeprefix("250_LOCAL_RESD_"),
            "%Y%m%d",
        ).date()
        for row in populations
    )
    if any(
        not 1 <= (cell.target_date - source_date).days <= 60
        for source_date in population_dates
    ):
        raise ValueError(f"{cell.slug} population CSV가 point-in-time 과거 범위 밖입니다.")
    station_sha = _require_sha256(
        source.get("station_master_content_sha256"),
        f"{cell.slug} station_master_content_sha256",
    )
    crosswalk_sha = _require_sha256(
        source.get("station_crosswalk_sha256"),
        f"{cell.slug} station_crosswalk_sha256",
    )
    crosswalk_count = source.get("station_crosswalk_count")
    if type(crosswalk_count) is not int or crosswalk_count <= 0:
        raise ValueError(f"{cell.slug} station_crosswalk_count가 양수가 아닙니다.")
    excluded_count = source.get("population_excluded_station_count")
    if type(excluded_count) is not int or excluded_count < 0:
        raise ValueError(f"{cell.slug} population 제외 개수가 잘못됐습니다.")
    excluded_ids = _require_sequence(
        source.get("population_excluded_grid_ids"),
        f"{cell.slug} population_excluded_grid_ids",
    )
    if any(type(row) is not str or not row for row in excluded_ids):
        raise ValueError(f"{cell.slug} population 제외 grid ID가 잘못됐습니다.")
    if list(excluded_ids) != sorted(set(excluded_ids)):
        raise ValueError(f"{cell.slug} population 제외 grid ID가 중복·비정렬입니다.")
    date_shared_source = {
        "rental_csv": rental,
        "stock_csv": stock,
        "weather_csv": weather,
        "population_csvs": populations,
        "station_crosswalk_count": crosswalk_count,
        "station_crosswalk_sha256": crosswalk_sha,
        **expected_versions,
    }
    full_source_sha256 = hashlib.sha256(
        _canonical_json(source).encode("utf-8")
    ).hexdigest()
    return _canonical_json(date_shared_source), full_source_sha256, station_sha


def _validate_source_file(value: object, label: str) -> dict[str, Any]:
    """Source file audit의 exact path·size·SHA를 검증한다."""
    source = _require_mapping(value, label)
    _require_exact_keys(source, frozenset({"path", "size_bytes", "sha256"}), label)
    path = _require_nonblank(source.get("path"), f"{label} path")
    size = source.get("size_bytes")
    if type(size) is not int or size <= 0:
        raise ValueError(f"{label} size_bytes가 양수가 아닙니다.")
    sha256 = _require_sha256(source.get("sha256"), f"{label} sha256")
    return {"path": path, "size_bytes": size, "sha256": sha256}


def _validate_policy_metrics(
    value: Mapping[str, Any],
    *,
    expected_cell: ConfirmatoryCell,
    minutes: int,
    expected_policy: str,
    expected_configuration: Mapping[str, Any],
) -> dict[str, Any]:
    """정책 결과의 exact identity·창·수치 항등식을 검증한다."""
    label = f"{expected_cell.slug} {minutes}분 {expected_policy}"
    _require_exact_keys(value, _POLICY_METRIC_KEYS, label)
    if value.get("policy") != expected_policy:
        raise ValueError(f"{label} policy 이름이 다릅니다.")
    _require_canonical_equal(
        value.get("policy_configuration"),
        expected_configuration,
        f"{label} policy_configuration",
    )
    expected_start = datetime.combine(
        expected_cell.target_date,
        datetime.min.time(),
        tzinfo=SEOUL,
    ) + timedelta(hours=expected_cell.start_hour)
    expected_end = expected_start + timedelta(minutes=minutes)
    if value.get("window_start") != expected_start.isoformat():
        raise ValueError(f"{label} window_start가 셀과 다릅니다.")
    if value.get("window_end") != expected_end.isoformat():
        raise ValueError(f"{label} window_end가 구간과 다릅니다.")
    integer_names = (
        "observed_requests",
        "fulfilled_requests",
        "unfulfilled_requests",
        "moved_bikes",
        "planned_bikes",
        "dispatched_routes",
        "completed_routes_by_cutoff",
        "trucks_still_busy_at_cutoff",
        "executed_stops",
        "decision_ticks",
        "movement_budget_used",
        "cold_start_stock_history_minutes",
    )
    values: dict[str, Any] = {}
    for name in integer_names:
        current = value.get(name)
        if type(current) is not int or current < 0:
            raise ValueError(f"{label} {name}이 비음수 정수가 아닙니다.")
        values[name] = current
    for name in (
        "observed_demand_fulfillment_rate",
        "empty_station_minutes",
        "vehicle_busy_minutes",
    ):
        current = value.get(name)
        if (
            type(current) not in (int, float)
            or not math.isfinite(current)
            or current < 0
        ):
            raise ValueError(f"{label} {name}이 finite 비음수가 아닙니다.")
        values[name] = float(current)
    if values["fulfilled_requests"] + values["unfulfilled_requests"] != values[
        "observed_requests"
    ]:
        raise ValueError(f"{label} 요청 수 항등식이 깨졌습니다.")
    expected_rate = (
        values["fulfilled_requests"] / values["observed_requests"]
        if values["observed_requests"]
        else 1.0
    )
    if (
        abs(values["observed_demand_fulfillment_rate"] - expected_rate)
        > _FLOAT_TOLERANCE
    ):
        raise ValueError(f"{label} 충족률이 요청 수와 다릅니다.")
    movement_budget = value.get("movement_budget")
    if movement_budget is not None and (
        type(movement_budget) is not int or movement_budget < 0
    ):
        raise ValueError(f"{label} movement_budget가 잘못됐습니다.")
    if movement_budget is not None and values["movement_budget_used"] > movement_budget:
        raise ValueError(f"{label} movement budget을 초과했습니다.")
    if values["movement_budget_used"] != values["planned_bikes"]:
        raise ValueError(f"{label} 계획 이동량과 budget 사용량이 다릅니다.")
    if values["moved_bikes"] > values["planned_bikes"]:
        raise ValueError(f"{label} 실제 이동량이 계획 이동량보다 큽니다.")
    values["unfulfilled_request_keys"] = _validate_unfulfilled_request_log(
        value.get("unfulfilled_request_log"),
        expected_count=values["unfulfilled_requests"],
        window_start=expected_start,
        window_end=expected_end,
        label=f"{label} unfulfilled_request_log",
    )
    values["max_pickup_dispatch_lag_minutes"] = _validate_job_audits(
        value.get("job_audits"),
        expected_routes=values["dispatched_routes"],
        expected_planned_bikes=values["planned_bikes"],
        expected_moved_bikes=values["moved_bikes"],
        expected_executed_stops=values["executed_stops"],
        window_start=expected_start,
        window_end=expected_end,
        label=f"{label} job_audits",
    )
    _require_sequence(value.get("tick_audits"), f"{label} tick_audits")
    return values


def _validate_unfulfilled_request_log(
    value: object,
    *,
    expected_count: int,
    window_start: datetime,
    window_end: datetime,
    label: str,
) -> frozenset[tuple[str, str, int]]:
    """미충족 요청 log를 exact event key 집합으로 검증한다."""
    rows = _require_sequence(value, label)
    if len(rows) != expected_count:
        raise ValueError(
            f"{label} 개수가 unfulfilled_requests와 다릅니다: "
            f"expected={expected_count}, actual={len(rows)}"
        )
    keys: list[tuple[str, str, int]] = []
    for index, value_row in enumerate(rows):
        row_label = f"{label}[{index}]"
        row = _require_mapping(value_row, row_label)
        _require_exact_keys(row, _UNFULFILLED_REQUEST_KEYS, row_label)
        bike_id = _require_nonblank(row.get("bike_id"), f"{row_label} bike_id")
        rented_at = _require_nonblank(
            row.get("rented_at"), f"{row_label} rented_at"
        )
        try:
            rented_moment = datetime.fromisoformat(rented_at)
        except ValueError as exc:
            raise ValueError(f"{row_label} rented_at이 ISO-8601이 아닙니다.") from exc
        if rented_moment.utcoffset() is None:
            raise ValueError(f"{row_label} rented_at에 timezone offset이 없습니다.")
        canonical_rented_at = rented_moment.astimezone(SEOUL).isoformat()
        if (
            rented_at != canonical_rented_at
            or not window_start <= rented_moment < window_end
        ):
            raise ValueError(
                f"{row_label} rented_at이 canonical KST 평가 창 안에 있지 않습니다."
            )
        station_no = row.get("station_no")
        if type(station_no) is not int or station_no <= 0:
            raise ValueError(f"{row_label} station_no가 양의 정수가 아닙니다.")
        keys.append((bike_id, canonical_rented_at, station_no))
    if len(keys) != len(set(keys)):
        raise ValueError(f"{label}에 중복 미충족 요청 event가 있습니다.")
    return frozenset(keys)


def _validate_job_audits(
    value: object,
    *,
    expected_routes: int,
    expected_planned_bikes: int,
    expected_moved_bikes: int,
    expected_executed_stops: int,
    window_start: datetime,
    window_end: datetime,
    label: str,
) -> float:
    """Job audit에서 pickup dispatch lag를 누락 없이 계산한다."""
    jobs = _require_sequence(value, label)
    if len(jobs) != expected_routes:
        raise ValueError(
            f"{label} 개수가 dispatched_routes와 다릅니다: "
            f"expected={expected_routes}, actual={len(jobs)}"
        )
    route_ids: set[str] = set()
    total_planned = 0
    total_moved = 0
    total_executed_stops = 0
    max_pickup_lag = 0.0
    for job_index, job_value in enumerate(jobs):
        job_label = f"{label}[{job_index}]"
        job = _require_mapping(job_value, job_label)
        _require_exact_keys(job, _JOB_AUDIT_KEYS, job_label)
        route_id = _require_nonblank(job.get("route_id"), f"{job_label} route_id")
        if route_id in route_ids:
            raise ValueError(f"{label}에 중복 route_id가 있습니다: {route_id}")
        route_ids.add(route_id)
        truck_id = job.get("truck_id")
        if type(truck_id) is not int or truck_id < 0:
            raise ValueError(f"{job_label} truck_id가 비음수 정수가 아닙니다.")
        dispatched_at = _validate_audit_moment(
            job.get("dispatched_at"),
            label=f"{job_label} dispatched_at",
            window_start=window_start,
            window_end=window_end,
        )
        completed_value = job.get("completed_at")
        completed_at = (
            None
            if completed_value is None
            else _validate_audit_moment(
                completed_value,
                label=f"{job_label} completed_at",
                window_start=window_start,
                window_end=window_end,
            )
        )
        return_at = _validate_audit_moment(
            job.get("return_at"),
            label=f"{job_label} return_at",
            window_start=window_start,
            window_end=window_end,
            allow_window_end=True,
        )
        if completed_at is not None and not dispatched_at <= completed_at <= return_at:
            raise ValueError(f"{job_label} 완료·복귀 시각 순서가 잘못됐습니다.")
        job_counts: dict[str, int] = {}
        for name in ("planned_bikes", "moved_bikes", "stop_count"):
            current = job.get(name)
            if type(current) is not int or current < 0:
                raise ValueError(f"{job_label} {name}이 비음수 정수가 아닙니다.")
            job_counts[name] = current
        stops = _require_sequence(job.get("stops"), f"{job_label} stops")
        if len(stops) != job_counts["stop_count"]:
            raise ValueError(f"{job_label} stop_count와 stops 개수가 다릅니다.")
        pickup_planned = 0
        dropoff_planned = 0
        pickup_actual = 0
        dropoff_actual = 0
        previous_executed_at = dispatched_at
        for stop_index, stop_value in enumerate(stops, start=1):
            stop_label = f"{job_label} stops[{stop_index - 1}]"
            stop = _require_mapping(stop_value, stop_label)
            _require_exact_keys(stop, _STOP_AUDIT_KEYS, stop_label)
            if stop.get("visit_no") != stop_index:
                raise ValueError(f"{stop_label} visit_no가 연속 1-base가 아닙니다.")
            station_no = stop.get("station_no")
            if type(station_no) is not int or station_no <= 0:
                raise ValueError(f"{stop_label} station_no가 양의 정수가 아닙니다.")
            _require_nonblank(stop.get("station_id"), f"{stop_label} station_id")
            action = stop.get("action")
            if action not in ("pickup", "dropoff"):
                raise ValueError(f"{stop_label} action이 pickup/dropoff가 아닙니다.")
            executed_at = _validate_audit_moment(
                stop.get("executed_at"),
                label=f"{stop_label} executed_at",
                window_start=window_start,
                window_end=window_end,
            )
            if executed_at <= previous_executed_at:
                raise ValueError(f"{stop_label} 실행 시각이 이전 stop보다 늦지 않습니다.")
            previous_executed_at = executed_at
            planned_quantity = stop.get("planned_quantity")
            if type(planned_quantity) is not int or planned_quantity <= 0:
                raise ValueError(f"{stop_label} planned_quantity가 양의 정수가 아닙니다.")
            actual_quantity = stop.get("actual_quantity")
            if actual_quantity is not None and (
                type(actual_quantity) is not int
                or not 0 <= actual_quantity <= planned_quantity
            ):
                raise ValueError(f"{stop_label} actual_quantity가 잘못됐습니다.")
            if actual_quantity is not None:
                total_executed_stops += 1
            if action == "pickup":
                pickup_planned += planned_quantity
                pickup_actual += actual_quantity or 0
                lag = (executed_at - dispatched_at).total_seconds() / 60.0
                max_pickup_lag = max(max_pickup_lag, lag)
            else:
                dropoff_planned += planned_quantity
                dropoff_actual += actual_quantity or 0
        if pickup_planned != dropoff_planned or dropoff_planned != job_counts[
            "planned_bikes"
        ]:
            raise ValueError(f"{job_label} pickup/dropoff 계획 수량이 완결되지 않았습니다.")
        if dropoff_actual != job_counts["moved_bikes"]:
            raise ValueError(f"{job_label} stop 실행량과 moved_bikes가 다릅니다.")
        if completed_at is not None:
            if stops and previous_executed_at != completed_at:
                raise ValueError(f"{job_label} completed_at이 마지막 stop과 다릅니다.")
            if pickup_actual != dropoff_actual:
                raise ValueError(f"{job_label} 완료 경로의 실제 이동 수량이 불완결합니다.")
        total_planned += job_counts["planned_bikes"]
        total_moved += job_counts["moved_bikes"]
    if total_planned != expected_planned_bikes:
        raise ValueError(f"{label} 합계와 top-level planned_bikes가 다릅니다.")
    if total_moved != expected_moved_bikes:
        raise ValueError(f"{label} 합계와 top-level moved_bikes가 다릅니다.")
    if total_executed_stops != expected_executed_stops:
        raise ValueError(f"{label} 합계와 top-level executed_stops가 다릅니다.")
    return round(max_pickup_lag, 6)


def _validate_audit_moment(
    value: object,
    *,
    label: str,
    window_start: datetime,
    window_end: datetime,
    allow_window_end: bool = False,
) -> datetime:
    """Audit 시각을 canonical KST와 평가 창 범위로 검증한다."""
    text = _require_nonblank(value, label)
    try:
        moment = datetime.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{label}이 ISO-8601이 아닙니다.") from exc
    if moment.utcoffset() is None:
        raise ValueError(f"{label}에 timezone offset이 없습니다.")
    if text != moment.astimezone(SEOUL).isoformat():
        raise ValueError(f"{label}이 canonical KST가 아닙니다.")
    in_window = (
        window_start <= moment <= window_end
        if allow_window_end
        else window_start <= moment < window_end
    )
    if not in_window:
        raise ValueError(f"{label}이 평가 창 안에 있지 않습니다.")
    return moment


def _evaluate_acceptance_gate(
    cells: Sequence[Mapping[str, Any]],
    contract: Mapping[str, Any],
) -> dict[str, Any]:
    """Manifest의 요청 event no-harm·180분 개선·실행 완결 기준을 계산한다."""
    durations = [duration for cell in cells for duration in cell["durations"]]
    new_unfulfilled_limit = contract[
        "every_cell_and_duration_new_unfulfilled_request_count_max"
    ]
    unfulfilled_limit = contract["every_cell_and_duration_unfulfilled_delta_max"]
    pickup_lag_limit = contract[
        "every_cell_and_duration_max_pickup_dispatch_lag_minutes"
    ]
    empty_limit = contract[
        "every_cell_and_duration_empty_station_minutes_delta_max"
    ]
    no_new_unfulfilled = all(
        duration["new_unfulfilled_request_count"] <= new_unfulfilled_limit
        for duration in durations
    )
    pickup_lag_within_limit = all(
        duration["max_pickup_dispatch_lag_minutes"]
        <= pickup_lag_limit + _FLOAT_TOLERANCE
        for duration in durations
    )
    observed_max_pickup_lag = max(
        (duration["max_pickup_dispatch_lag_minutes"] for duration in durations),
        default=0.0,
    )
    unfulfilled_no_worse = all(
        duration["unfulfilled_delta"] <= unfulfilled_limit for duration in durations
    )
    empty_no_worse = all(
        duration["empty_station_minutes_delta"] <= empty_limit + _FLOAT_TOLERANCE
        for duration in durations
    )
    planned_equals_moved = all(
        duration["planned_bikes"] == duration["moved_bikes"]
        for duration in durations
    )
    all_routes_finish = all(duration["finished_by_cutoff"] for duration in durations)
    duration_180 = [
        duration
        for cell in cells
        for duration in cell["durations"]
        if duration["evaluation_minutes"] == 180
    ]
    baseline_unfulfilled = sum(
        duration["baseline_unfulfilled_requests"] for duration in duration_180
    )
    candidate_unfulfilled = sum(
        duration["candidate_unfulfilled_requests"] for duration in duration_180
    )
    aggregate_unfulfilled_delta = candidate_unfulfilled - baseline_unfulfilled
    aggregate_unfulfilled_limit = contract[
        "aggregate_180m_unfulfilled_delta_max_exclusive"
    ]
    aggregate_unfulfilled_strict = (
        aggregate_unfulfilled_delta < aggregate_unfulfilled_limit
    )
    baseline_empty = sum(
        duration["baseline_empty_station_minutes"] for duration in duration_180
    )
    candidate_empty = sum(
        duration["candidate_empty_station_minutes"] for duration in duration_180
    )
    reduction_pct = (
        0.0
        if baseline_empty <= _FLOAT_TOLERANCE
        else (baseline_empty - candidate_empty) / baseline_empty * 100.0
    )
    minimum_reduction = contract[
        "aggregate_180m_empty_station_minutes_reduction_min_pct"
    ]
    aggregate_reduction_passed = reduction_pct + _FLOAT_TOLERANCE >= minimum_reduction
    improved_count = sum(
        duration["candidate_empty_station_minutes"]
        < duration["baseline_empty_station_minutes"] - _FLOAT_TOLERANCE
        for duration in duration_180
    )
    minimum_improved = contract["improved_180m_cells_min"]
    improved_cells_passed = improved_count >= minimum_improved
    exact_cell_count = len(cells) == len(_EXPECTED_CELLS)
    exact_duration_count = len(durations) == len(_EXPECTED_CELLS) * 3
    single_run_claimed = True
    passed = all(
        (
            exact_cell_count,
            exact_duration_count,
            no_new_unfulfilled,
            unfulfilled_no_worse,
            aggregate_unfulfilled_strict,
            pickup_lag_within_limit,
            empty_no_worse,
            aggregate_reduction_passed,
            improved_cells_passed,
            planned_equals_moved,
            all_routes_finish,
            single_run_claimed,
        )
    )
    return {
        "passed": passed,
        "exact_cell_count": exact_cell_count,
        "exact_duration_count": exact_duration_count,
        "every_cell_and_duration_new_unfulfilled_request_set_empty": (
            no_new_unfulfilled
        ),
        "every_cell_and_duration_new_unfulfilled_request_count_max": (
            new_unfulfilled_limit
        ),
        "every_cell_and_duration_unfulfilled_no_worse": unfulfilled_no_worse,
        "aggregate_180m_baseline_unfulfilled_requests": baseline_unfulfilled,
        "aggregate_180m_candidate_unfulfilled_requests": candidate_unfulfilled,
        "aggregate_180m_unfulfilled_delta": aggregate_unfulfilled_delta,
        "aggregate_180m_unfulfilled_delta_max_exclusive": (
            aggregate_unfulfilled_limit
        ),
        "aggregate_180m_unfulfilled_strict_improvement": (
            aggregate_unfulfilled_strict
        ),
        "every_cell_and_duration_pickup_dispatch_lag_within_limit": (
            pickup_lag_within_limit
        ),
        "observed_max_pickup_dispatch_lag_minutes": round(
            observed_max_pickup_lag,
            6,
        ),
        "every_cell_and_duration_max_pickup_dispatch_lag_minutes": (
            pickup_lag_limit
        ),
        "every_cell_and_duration_empty_station_minutes_no_worse": empty_no_worse,
        "aggregate_180m_baseline_empty_station_minutes": round(baseline_empty, 3),
        "aggregate_180m_candidate_empty_station_minutes": round(candidate_empty, 3),
        "aggregate_180m_empty_station_minutes_reduction_pct": round(
            reduction_pct, 6
        ),
        "aggregate_180m_empty_station_minutes_reduction_min_pct": minimum_reduction,
        "aggregate_180m_empty_station_minutes_reduction_passed": (
            aggregate_reduction_passed
        ),
        "improved_180m_cell_count": improved_count,
        "improved_180m_cells_min": minimum_improved,
        "improved_180m_cells_passed": improved_cells_passed,
        "planned_bikes_equal_moved_bikes": planned_equals_moved,
        "all_routes_finished_by_cutoff": all_routes_finish,
        "single_confirmatory_run_claimed": single_run_claimed,
    }


def _loads_strict_json(payload: bytes, label: str) -> Mapping[str, Any]:
    """UTF-8 JSON을 duplicate key와 NaN을 허용하지 않고 읽는다."""

    def reject_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        """한 object 안에서 같은 key가 두 번 나오면 즉시 거부한다."""
        result: dict[str, Any] = {}
        for name, value in pairs:
            if name in result:
                raise ValueError(f"{label}에 중복 JSON key가 있습니다: {name}")
            result[name] = value
        return result

    def reject_constant(value: str) -> None:
        """Python JSON 확장 NaN·Infinity를 거부한다."""
        raise ValueError(f"{label}에 비표준 JSON 숫자가 있습니다: {value}")

    try:
        text = payload.decode("utf-8")
        value = json.loads(
            text,
            object_pairs_hook=reject_duplicates,
            parse_constant=reject_constant,
        )
    except UnicodeDecodeError as exc:
        raise ValueError(f"{label}이 UTF-8이 아닙니다.") from exc
    except json.JSONDecodeError as exc:
        raise ValueError(f"{label}이 유효한 JSON이 아닙니다.") from exc
    return _require_mapping(value, label)


def _require_exact_keys(
    value: Mapping[str, Any], expected: frozenset[str], label: str
) -> None:
    """Object key 집합이 누락·추가 없이 exact인지 검증한다."""
    actual = frozenset(value)
    if actual != expected:
        raise ValueError(
            f"{label} key 집합이 다릅니다: "
            f"missing={sorted(expected - actual)}, extra={sorted(actual - expected)}"
        )


def _require_mapping(value: object, label: str) -> Mapping[str, Any]:
    """값이 JSON object가 아니면 검증 오류를 낸다."""
    if not isinstance(value, Mapping):
        raise ValueError(f"{label}은 object여야 합니다.")
    return value


def _require_sequence(value: object, label: str) -> Sequence[Any]:
    """문자열이 아닌 JSON array인지 검증한다."""
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ValueError(f"{label}은 array여야 합니다.")
    return value


def _require_nonblank(value: object, label: str) -> str:
    """값이 공백 아닌 문자열인지 검증해 반환한다."""
    if type(value) is not str or not value.strip():
        raise ValueError(f"{label}은 nonblank 문자열이어야 합니다.")
    return value


def _require_sha256(value: object, label: str) -> str:
    """값이 소문자 hexadecimal SHA-256인지 검증해 반환한다."""
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise ValueError(f"{label} 형식이 잘못됐습니다.")
    return value


def _require_git_commit(value: object, label: str) -> str:
    """값이 full lowercase Git commit인지 검증해 반환한다."""
    if type(value) is not str or _GIT_COMMIT.fullmatch(value) is None:
        raise ValueError(f"{label} 형식이 잘못됐습니다.")
    return value


def _canonical_json(value: object) -> str:
    """JSON 호환 값을 NaN 없이 결정적 문자열로 직렬화한다."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
            allow_nan=False,
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("canonical JSON으로 표현할 수 없는 값입니다.") from exc


def _require_canonical_equal(value: object, expected: object, label: str) -> None:
    """두 JSON 값이 타입과 key를 포함해 canonical exact인지 검증한다."""
    if _canonical_json(value) != _canonical_json(expected):
        raise ValueError(f"{label}이 등록된 exact 값과 다릅니다.")


def _pretty_json_bytes(value: object) -> bytes:
    """감사용 UTF-8 pretty JSON bytes를 결정적으로 만든다."""
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("결과를 JSON으로 직렬화할 수 없습니다.") from exc
