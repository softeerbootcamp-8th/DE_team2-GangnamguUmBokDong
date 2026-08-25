"""사전 등록 confirmatory matrix의 fail-closed 실행·판정을 검증한다."""

from __future__ import annotations

import copy
import hashlib
import json
from dataclasses import fields
from datetime import datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from zoneinfo import ZoneInfo

import pytest
from core.scoring_config import URGENCY_SCORING_CONFIG_VERSION
from evaluation.backtest_contract import BACKTEST_CONTRACT_VERSION, EvaluationContract
from evaluation.confirmatory_matrix import (
    CANDIDATE_LOCK_SCHEMA_VERSION,
    MANIFEST_SCHEMA_VERSION,
    REGISTERED_MANIFEST_FILENAME,
    REGISTERED_MANIFEST_SHA256,
    REGISTERED_SIDECAR_FILENAME,
    RESULT_SCHEMA_VERSION,
    RUN_CLAIM_SCHEMA_VERSION,
    SUPERSEDED_MANIFEST_SHA256,
    _DURATION_KEYS,
    _JOB_AUDIT_KEYS,
    _POLICY_METRIC_KEYS,
    _RAW_RESULT_KEYS,
    _SOURCE_PROVENANCE_KEYS,
    _STOP_AUDIT_KEYS,
    CandidateLock,
    ConfirmatoryCell,
    ConfirmatoryManifest,
    RawResultArtifact,
    RunClaim,
    _loads_strict_json,
    _validate_candidate_lock_document,
    _validate_job_audits,
    _validate_manifest_document,
    candidate_lock_document,
    load_candidate_lock,
    load_confirmatory_manifest,
    load_raw_result,
    load_run_claim,
    validate_confirmatory_results,
    write_candidate_lock,
    write_confirmatory_result,
)
from evaluation.production_policy_contract import (
    PRODUCTION_MODEL_BUNDLE_SHA256,
    PRODUCTION_POLICY_NAME,
    PRODUCTION_WEATHER_SHA256,
    production_policy_configuration,
)
from evaluation.policy_simulator import JobAudit, SimulationMetrics, StopAudit
from evaluation.run_policy_backtest import (
    DurationResult,
    PolicyBacktestResult,
    SourceProvenance,
)
from evaluation.run_confirmatory_matrix import (
    DEFAULT_MANIFEST_PATH,
    DEFAULT_SIDECAR_PATH,
    RESULT_JSON_FILENAME,
    RESULT_MARKDOWN_FILENAME,
    GitState,
    _require_outside_repo,
    candidate_run_claim_path,
    create_run_claim,
    execute_confirmatory_matrix,
    read_git_state,
    validate_git_state,
)
from evaluation import run_confirmatory_matrix as runner_module
from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY, LEGACY_REBALANCE_POLICY
from gold.rebalance_route import MAX_STOPS_PER_ROUTE, ROUTE_ALGORITHM_VERSION

SEOUL = ZoneInfo("Asia/Seoul")
REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = (
    REPO_ROOT / "loader/evaluation/manifests" / REGISTERED_MANIFEST_FILENAME
)
SIDECAR_PATH = (
    REPO_ROOT / "loader/evaluation/manifests" / REGISTERED_SIDECAR_FILENAME
)
ARCHIVED_MANIFEST_PATH = (
    REPO_ROOT / "loader/evaluation/manifests/confirmatory-matrix-v1.json"
)
ARCHIVED_SIDECAR_PATH = (
    REPO_ROOT / "loader/evaluation/manifests/confirmatory-matrix-v1.sha256"
)
GIT_COMMIT = "a" * 40


@pytest.fixture
def manifest() -> ConfirmatoryManifest:
    """Repository에 사전 등록된 실제 manifest 계약을 반환한다."""
    return load_confirmatory_manifest(MANIFEST_PATH, SIDECAR_PATH)


@pytest.fixture
def candidate_lock(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
) -> CandidateLock:
    """합성 결과 검증에 쓸 exact production candidate lock을 만든다."""
    return write_candidate_lock(
        tmp_path / "candidate-lock.json",
        manifest_sha256=manifest.sha256,
        git_commit=GIT_COMMIT,
    )


@pytest.fixture
def run_claim(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> RunClaim:
    """합성 결과가 한 번의 실행에서 나왔음을 고정하는 claim을 만든다."""
    path = candidate_run_claim_path(candidate_lock)
    create_run_claim(path, manifest=manifest, candidate_lock=candidate_lock)
    return load_run_claim(path, manifest=manifest, candidate_lock=candidate_lock)


def test_registered_manifest_sidecar_and_exact_cells_are_valid(
    manifest: ConfirmatoryManifest,
) -> None:
    """V2 sidecar SHA·v1 계승·12셀 exact set을 독립적으로 고정한다."""
    assert manifest.sha256 == REGISTERED_MANIFEST_SHA256
    assert manifest.document["schema_version"] == MANIFEST_SCHEMA_VERSION
    assert manifest.document["supersedes_manifest_sha256"] == (
        SUPERSEDED_MANIFEST_SHA256
    )
    assert len(manifest.cells) == 12
    assert {cell.center_id for cell in manifest.cells} == {
        "sangam",
        "jungnang",
        "cheonwang",
        "cheonho",
    }
    assert {cell.start_hour for cell in manifest.cells} == {7, 13, 18}
    assert DEFAULT_MANIFEST_PATH == MANIFEST_PATH
    assert DEFAULT_SIDECAR_PATH == SIDECAR_PATH
    assert RESULT_JSON_FILENAME == f"{RESULT_SCHEMA_VERSION}.json"
    assert RESULT_MARKDOWN_FILENAME == f"{RESULT_SCHEMA_VERSION}.md"


def test_v1_manifest_is_byte_preserved_and_v2_keeps_its_selection() -> None:
    """V1 bytes는 archival로 보존하고 v2는 동일 selection·12셀만 계승한다."""
    archived_payload = ARCHIVED_MANIFEST_PATH.read_bytes()
    assert hashlib.sha256(archived_payload).hexdigest() == SUPERSEDED_MANIFEST_SHA256
    assert ARCHIVED_SIDECAR_PATH.read_bytes() == (
        f"{SUPERSEDED_MANIFEST_SHA256}  confirmatory-matrix-v1.json\n"
    ).encode("ascii")
    archived = _loads_strict_json(archived_payload, "archived v1 manifest")
    registered = _loads_strict_json(
        MANIFEST_PATH.read_bytes(),
        "registered v2 manifest",
    )
    assert registered["selection_policy"] == archived["selection_policy"]
    assert registered["evaluation_contract"] == archived["evaluation_contract"]
    assert registered["cells"] == archived["cells"]


def test_validator_key_contract_tracks_actual_raw_dataclasses() -> None:
    """Validator exact key 집합은 실제 backtest 직렬화 dataclass와 결속된다."""
    assert _RAW_RESULT_KEYS == frozenset(
        field.name for field in fields(PolicyBacktestResult)
    )
    assert _DURATION_KEYS == frozenset(field.name for field in fields(DurationResult))
    assert _SOURCE_PROVENANCE_KEYS == frozenset(
        field.name for field in fields(SourceProvenance)
    )
    assert _POLICY_METRIC_KEYS == frozenset(
        field.name for field in fields(SimulationMetrics)
    )
    assert _JOB_AUDIT_KEYS == frozenset(field.name for field in fields(JobAudit))
    assert _STOP_AUDIT_KEYS == frozenset(field.name for field in fields(StopAudit))


def test_pickup_dispatch_lag_is_zero_when_no_pickup_job_exists() -> None:
    """배차와 pickup이 모두 없는 정책의 pickup 지연은 0분으로 정의한다."""
    start = datetime(2025, 1, 17, 6, tzinfo=SEOUL)
    assert _validate_job_audits(
        [],
        expected_routes=0,
        expected_planned_bikes=0,
        expected_moved_bikes=0,
        expected_executed_stops=0,
        window_start=start,
        window_end=start + timedelta(minutes=60),
        label="no-pickup fixture",
    ) == 0.0


def test_manifest_rejects_sidecar_change(tmp_path: Path) -> None:
    """Manifest가 같아도 sidecar byte가 등록값과 다르면 거부한다."""
    manifest_path, sidecar_path = _copy_registered_manifest(tmp_path)
    sidecar_path.write_text("0" * 64 + f"  {REGISTERED_MANIFEST_FILENAME}\n")

    with pytest.raises(ValueError, match="sidecar"):
        load_confirmatory_manifest(manifest_path, sidecar_path)


def test_manifest_rejects_joint_manifest_and_sidecar_rewrite(tmp_path: Path) -> None:
    """Manifest와 sidecar를 함께 바꿔도 사전 등록 SHA가 아니면 거부한다."""
    manifest_path, sidecar_path = _copy_registered_manifest(tmp_path)
    payload = manifest_path.read_bytes().replace(
        b'"start_hour": 7', b'"start_hour": 8', 1
    )
    manifest_path.write_bytes(payload)
    digest = hashlib.sha256(payload).hexdigest()
    sidecar_path.write_text(
        f"{digest}  {REGISTERED_MANIFEST_FILENAME}\n",
        encoding="ascii",
    )

    with pytest.raises(ValueError, match="sidecar|등록값"):
        load_confirmatory_manifest(manifest_path, sidecar_path)


def test_strict_json_rejects_duplicate_keys() -> None:
    """중첩 위치와 무관하게 duplicate JSON key를 즉시 거부한다."""
    with pytest.raises(ValueError, match="중복 JSON key"):
        _loads_strict_json(b'{"outer":{"cell":1,"cell":2}}', "fixture")


@pytest.mark.parametrize("surface", ("cells", "evaluation_contract"))
def test_manifest_semantics_reject_missing_cell_or_operation_change(
    manifest: ConfirmatoryManifest,
    surface: str,
) -> None:
    """Sidecar 검사 뒤에도 셀 exact set과 운영 계약을 별도로 fail-closed한다."""
    document = copy.deepcopy(manifest.document)
    if surface == "cells":
        document["cells"].pop()
    else:
        document["evaluation_contract"]["fleet_size"] = 2

    with pytest.raises(ValueError, match="셀 exact set|evaluation_contract"):
        _validate_manifest_document(document)


def test_manifest_rejects_wrong_superseded_v1_sha(
    manifest: ConfirmatoryManifest,
) -> None:
    """V2는 archival v1의 등록 SHA 외 다른 계약을 계승할 수 없다."""
    document = copy.deepcopy(manifest.document)
    document["supersedes_manifest_sha256"] = "0" * 64

    with pytest.raises(ValueError, match="v1 SHA"):
        _validate_manifest_document(document)


def test_candidate_lock_is_exact_and_cannot_be_created_twice(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
) -> None:
    """Policy·max stops·commit·manifest SHA lock은 한 경로에 한 번만 생성한다."""
    path = tmp_path / "candidate-lock.json"
    first = write_candidate_lock(
        path,
        manifest_sha256=manifest.sha256,
        git_commit=GIT_COMMIT,
    )
    assert first.document["schema_version"] == CANDIDATE_LOCK_SCHEMA_VERSION
    assert first.document["candidate"] == {
        "policy": PRODUCTION_POLICY_NAME,
        "policy_configuration": DEFAULT_REBALANCE_POLICY.audit_document(),
        "max_stops_per_route": MAX_STOPS_PER_ROUTE,
    }
    with pytest.raises(ValueError, match="이미 존재"):
        write_candidate_lock(
            path,
            manifest_sha256=manifest.sha256,
            git_commit=GIT_COMMIT,
        )


@pytest.mark.parametrize(
    "field",
    ("manifest_sha256", "git_commit", "policy_configuration", "max_stops_per_route"),
)
def test_candidate_lock_rejects_any_identity_change(
    manifest: ConfirmatoryManifest,
    field: str,
) -> None:
    """Candidate identity 네 축 중 하나라도 다르면 실행 전 거부한다."""
    document = candidate_lock_document(manifest.sha256, GIT_COMMIT)
    if field == "manifest_sha256":
        document[field] = "b" * 64
    elif field == "git_commit":
        document[field] = "b" * 40
    elif field == "policy_configuration":
        document["candidate"][field]["max_pickup_stock_fraction"] = 0.09
    else:
        document["candidate"][field] = MAX_STOPS_PER_ROUTE + 1

    with pytest.raises(ValueError, match="candidate lock"):
        _validate_candidate_lock_document(
            document,
            expected_manifest_sha256=manifest.sha256,
            expected_git_commit=GIT_COMMIT,
        )


def test_candidate_lock_loader_rejects_duplicate_key(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
) -> None:
    """Candidate lock JSON도 duplicate key 오염을 허용하지 않는다."""
    path = tmp_path / "candidate.json"
    path.write_text(
        f'{{"schema_version":"{CANDIDATE_LOCK_SCHEMA_VERSION}",'
        f'"schema_version":"{CANDIDATE_LOCK_SCHEMA_VERSION}"}}',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="중복 JSON key"):
        load_candidate_lock(
            path,
            expected_manifest_sha256=manifest.sha256,
            expected_git_commit=GIT_COMMIT,
        )


def test_result_validation_rejects_run_claim_changed_after_preflight(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Preflight 뒤 run claim byte가 바뀌면 raw 결과 검증도 중단한다."""
    assert run_claim.document["schema_version"] == RUN_CLAIM_SCHEMA_VERSION
    claim_path = Path(run_claim.path)
    document = json.loads(claim_path.read_text(encoding="utf-8"))
    document["candidate_lock_sha256"] = "b" * 64
    claim_path.write_text(json.dumps(document), encoding="utf-8")

    with pytest.raises(ValueError, match="run claim"):
        validate_confirmatory_results(
            _passing_artifacts(manifest),
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


@pytest.mark.parametrize(
    "state",
    (
        GitState("other", GIT_COMMIT, False),
        GitState("feature/rebalance-policy-v3", GIT_COMMIT, True),
        GitState("feature/rebalance-policy-v3", "short", False),
    ),
)
def test_git_preflight_rejects_wrong_branch_dirty_or_abbreviated_commit(
    manifest: ConfirmatoryManifest,
    state: GitState,
) -> None:
    """Commit lock 전에 branch·tracked cleanliness·full SHA를 강제한다."""
    with pytest.raises(ValueError, match="branch|dirty|commit"):
        validate_git_state(state, manifest)


def test_git_state_includes_untracked_files_in_repo_cleanliness(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Git preflight는 untracked 정책 코드까지 dirty로 인식한다."""
    calls: list[tuple[str, ...]] = []

    def fake_run(arguments, **kwargs):
        """Git subcommand별 고정 stdout을 반환한다."""
        del kwargs
        command = tuple(arguments)
        calls.append(command)
        if command[1:3] == ("branch", "--show-current"):
            return SimpleNamespace(stdout="feature/rebalance-policy-v3\n")
        if command[1:3] == ("rev-parse", "HEAD"):
            return SimpleNamespace(stdout=GIT_COMMIT + "\n")
        return SimpleNamespace(stdout="?? loader/evaluation/untracked_policy.py\n")

    monkeypatch.setattr(runner_module.subprocess, "run", fake_run)

    state = read_git_state(tmp_path)

    assert state.worktree_dirty is True
    assert (
        "git",
        "status",
        "--porcelain",
        "--untracked-files=all",
    ) in calls


def test_candidate_lock_and_output_paths_must_be_outside_repository(
    tmp_path: Path,
) -> None:
    """Preflight 산출물이 repo를 dirty하게 만들 경로를 거부한다."""
    repo = tmp_path / "repo"
    repo.mkdir()
    with pytest.raises(ValueError, match="repository 밖"):
        _require_outside_repo(repo / "candidate-lock.json", repo, "candidate lock")

    _require_outside_repo(tmp_path / "outside" / "candidate-lock.json", repo, "lock")


def test_complete_synthetic_matrix_passes_registered_gate(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """12셀 중 8셀의 180분 품절 10% 개선은 모든 no-harm gate를 통과한다."""
    artifacts = _passing_artifacts(manifest)

    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )

    gate = result["acceptance_gate"]
    assert gate["passed"] is True
    assert gate["every_cell_and_duration_new_unfulfilled_request_set_empty"] is True
    assert gate["aggregate_180m_unfulfilled_delta"] == -8
    assert gate["aggregate_180m_unfulfilled_strict_improvement"] is True
    assert gate["observed_max_pickup_dispatch_lag_minutes"] == 10.0
    assert gate["every_cell_and_duration_pickup_dispatch_lag_within_limit"] is True
    assert gate["improved_180m_cell_count"] == 8
    assert gate["aggregate_180m_empty_station_minutes_reduction_pct"] == pytest.approx(
        6.666667
    )
    assert gate["planned_bikes_equal_moved_bikes"] is True
    assert gate["all_routes_finished_by_cutoff"] is True


@pytest.mark.parametrize(
    ("mutation", "failed_check"),
    (
        (
            "new_unfulfilled_transfer",
            "every_cell_and_duration_new_unfulfilled_request_set_empty",
        ),
        ("unfulfilled_worse", "every_cell_and_duration_unfulfilled_no_worse"),
        (
            "aggregate_unfulfilled_not_strict",
            "aggregate_180m_unfulfilled_strict_improvement",
        ),
        (
            "pickup_dispatch_lag_worse",
            "every_cell_and_duration_pickup_dispatch_lag_within_limit",
        ),
        ("empty_worse", "every_cell_and_duration_empty_station_minutes_no_worse"),
        (
            "aggregate_reduction_below_five",
            "aggregate_180m_empty_station_minutes_reduction_passed",
        ),
        ("only_seven_improved", "improved_180m_cells_passed"),
        ("planned_moved_mismatch", "planned_bikes_equal_moved_bikes"),
        ("unfinished_at_cutoff", "all_routes_finished_by_cutoff"),
    ),
)
def test_acceptance_gate_fails_each_registered_condition(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
    mutation: str,
    failed_check: str,
) -> None:
    """Manifest의 각 acceptance 조건은 독립적으로 false를 만든다."""
    artifacts = _passing_artifacts(manifest)
    _mutate_gate_input(artifacts, mutation)

    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )

    assert result["acceptance_gate"]["passed"] is False
    assert result["acceptance_gate"][failed_check] is False


def test_equal_total_failure_transfer_is_reported_by_event_and_station(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """총 미충족이 같아도 새 요청·대여소로 전가하면 파생 결과와 gate에 남긴다."""
    artifacts = _passing_artifacts(manifest)
    _mutate_gate_input(artifacts, "new_unfulfilled_transfer")

    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )

    first = artifacts[0].document
    cell = next(
        row
        for row in result["cells"]
        if (row["center_id"], row["target_date"], row["start_hour"])
        == (first["center_id"], first["target_date"], first["start_hour"])
    )
    duration = cell["durations"][0]
    assert duration["unfulfilled_delta"] == 0
    assert duration["new_unfulfilled_request_count"] == 1
    assert duration["resolved_unfulfilled_request_count"] == 1
    assert duration["new_unfulfilled_station_nos"] == [9999]
    assert duration["resolved_unfulfilled_station_nos"] == [1009]
    assert result["acceptance_gate"][
        "every_cell_and_duration_new_unfulfilled_request_set_empty"
    ] is False


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing_key", "key 집합"),
        ("duplicate", "중복 미충족"),
        ("out_of_window", "평가 창"),
        ("count_mismatch", "개수가 unfulfilled_requests"),
        ("station_type", "station_no가 양의 정수"),
        ("naive_timestamp", "timezone offset"),
    ),
)
def test_raw_result_rejects_invalid_unfulfilled_event_log(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
    mutation: str,
    message: str,
) -> None:
    """미충족 log의 schema·고유성·창·개수·타입을 fail-closed 검증한다."""
    artifacts = _passing_artifacts(manifest)
    policy = artifacts[0].document["durations"][0]["no_rebalance"]
    log = policy["unfulfilled_request_log"]
    if mutation == "missing_key":
        log[0].pop("station_no")
    elif mutation == "duplicate":
        log[1] = copy.deepcopy(log[0])
    elif mutation == "out_of_window":
        log[0]["rented_at"] = policy["window_end"]
    elif mutation == "count_mismatch":
        log.pop()
    elif mutation == "station_type":
        log[0]["station_no"] = "1000"
    else:
        log[0]["rented_at"] = log[0]["rented_at"].removesuffix("+09:00")

    with pytest.raises(ValueError, match=message):
        validate_confirmatory_results(
            artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        ("missing_job", "개수가 dispatched_routes"),
        ("missing_stop_key", "key 집합"),
        ("hidden_pickup", "pickup/dropoff 계획 수량"),
    ),
)
def test_raw_result_rejects_incomplete_pickup_job_audit(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
    mutation: str,
    message: str,
) -> None:
    """Pickup audit를 누락하거나 action을 숨기면 lag 0으로 우회하지 못한다."""
    artifacts = _passing_artifacts(manifest)
    candidate = artifacts[0].document["durations"][0]["model_policies"][0]
    if mutation == "missing_job":
        candidate["job_audits"].clear()
    elif mutation == "missing_stop_key":
        candidate["job_audits"][0]["stops"][0].pop("executed_at")
    else:
        candidate["job_audits"][0]["stops"][0]["action"] = "dropoff"

    with pytest.raises(ValueError, match=message):
        validate_confirmatory_results(
            artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


def test_results_reject_missing_or_duplicate_cell(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Raw 결과는 manifest 12셀과 정확히 한 번씩 대응해야 한다."""
    artifacts = _passing_artifacts(manifest)
    with pytest.raises(ValueError, match="셀 집합"):
        validate_confirmatory_results(
            artifacts[:-1],
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )
    with pytest.raises(ValueError, match="중복 confirmatory"):
        validate_confirmatory_results(
            (*artifacts[:-1], artifacts[0]),
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


def test_raw_result_rejects_model_mismatch(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Raw 결과의 model SHA가 고정 production bundle과 다르면 거부한다."""
    model_artifacts = _passing_artifacts(manifest)
    model_artifacts[0].document["model_bundle_sha256"] = "b" * 64
    with pytest.raises(ValueError, match="model bundle SHA"):
        validate_confirmatory_results(
            model_artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


def test_raw_result_rejects_policy_mismatch(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Raw candidate config가 lock의 production 정책과 다르면 거부한다."""
    policy_artifacts = _passing_artifacts(manifest)
    policy_artifacts[0].document["durations"][0]["model_policies"][0][
        "policy_configuration"
    ]["max_pickup_stock_fraction"] = 0.09
    with pytest.raises(ValueError, match="policy_configuration"):
        validate_confirmatory_results(
            policy_artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


def test_cell_specific_station_surfaces_are_preserved_not_forced_equal(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """선택된 station surface SHA는 서로 다른 센터·날짜마다 독립 보존한다."""
    artifacts = _passing_artifacts(manifest)
    expected = {}
    for artifact in artifacts:
        document = artifact.document
        key = (
            f"{document['center_id']}|{document['target_date']}|"
            f"{document['start_hour']:02d}"
        )
        station_sha = hashlib.sha256(key.encode()).hexdigest()
        document["source_provenance"]["station_master_content_sha256"] = station_sha
        expected[key] = station_sha

    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )

    assert result["station_surface_sha256_by_cell"] == expected


def test_source_provenance_rejects_future_wrong_month_or_same_date_change(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Source는 target 월·과거 인구·같은 날짜 exact 입력을 강제한다."""
    future_artifacts = _passing_artifacts(manifest)
    cell = future_artifacts[0].document
    target = datetime.fromisoformat(cell["target_date"]).date()
    population = cell["source_provenance"]["population_csvs"][0]
    population["path"] = (
        f"/fixture/250_LOCAL_RESD_{target + timedelta(days=1):%Y%m%d}.csv"
    )
    with pytest.raises(ValueError, match="point-in-time"):
        validate_confirmatory_results(
            future_artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )

    wrong_month_artifacts = _passing_artifacts(manifest)
    wrong_month_artifacts[0].document["source_provenance"]["rental_csv"][
        "path"
    ] = "/fixture/서울특별시 공공자전거 대여이력 정보_2512.csv"
    with pytest.raises(ValueError, match="rental CSV 월"):
        validate_confirmatory_results(
            wrong_month_artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )

    source_artifacts = _passing_artifacts(manifest)
    first_date = source_artifacts[0].document["target_date"]
    same_date = next(
        artifact
        for artifact in source_artifacts[1:]
        if artifact.document["target_date"] == first_date
    )
    same_date.document["source_provenance"]["rental_csv"]["sha256"] = "b" * 64
    with pytest.raises(ValueError, match="source provenance"):
        validate_confirmatory_results(
            source_artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


@pytest.mark.parametrize("surface", ("cell", "duration", "contract"))
def test_raw_result_rejects_cell_duration_or_contract_mismatch(
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
    surface: str,
) -> None:
    """Raw center/date/hour·60/120/180·운영 계약은 manifest와 exact해야 한다."""
    artifacts = _passing_artifacts(manifest)
    if surface == "cell":
        artifacts[0].document["start_hour"] = 8
    elif surface == "duration":
        artifacts[0].document["durations"].pop()
    else:
        artifacts[0].document["contracts"][0]["contract"]["fleet_size"] = 2

    with pytest.raises(ValueError, match="등록되지 않은|duration|contracts"):
        validate_confirmatory_results(
            artifacts,
            manifest=manifest,
            candidate_lock=candidate_lock,
            run_claim=run_claim,
        )


def test_raw_loader_rejects_duplicate_json_key(tmp_path: Path) -> None:
    """Raw 결과 파일도 duplicate key를 이용한 identity 우회를 거부한다."""
    path = tmp_path / "raw.json"
    path.write_text('{"center_id":"a","center_id":"b"}', encoding="utf-8")
    with pytest.raises(ValueError, match="중복 JSON key"):
        load_raw_result(path)


def test_result_writes_json_and_markdown_once(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    run_claim: RunClaim,
) -> None:
    """Gate 결과는 JSON·Markdown으로 남고 기존 증거를 덮어쓰지 않는다."""
    result = validate_confirmatory_results(
        _passing_artifacts(manifest),
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )
    json_path = tmp_path / "result.json"
    markdown_path = tmp_path / "result.md"

    write_confirmatory_result(
        result,
        json_path=json_path,
        markdown_path=markdown_path,
    )

    document = json.loads(json_path.read_text(encoding="utf-8"))
    assert document["schema_version"] == RESULT_SCHEMA_VERSION
    assert document["acceptance_gate"]["passed"] is True
    markdown = markdown_path.read_text(encoding="utf-8")
    assert "최종 통과: **True**" in markdown
    assert "신규 미충족 요청 0건: **True**" in markdown
    assert "180분 aggregate 미충족 요청 변화: **-8**" in markdown
    assert "Pickup dispatch→실행 최대 지연: **10.000분**" in markdown
    assert "8/12" in markdown
    with pytest.raises(ValueError, match="이미 존재"):
        write_confirmatory_result(
            result,
            json_path=json_path,
            markdown_path=markdown_path,
        )


def test_runner_claim_prevents_second_execution_before_cell_access(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> None:
    """Run claim은 첫 셀 접근 전에 생기고 두 번째 실행 callback을 호출하지 않는다."""
    by_key = {
        cell.key: artifact.document
        for cell, artifact in zip(
            manifest.cells,
            _passing_artifacts(manifest),
            strict=True,
        )
    }
    calls: list[tuple[str, str, int]] = []

    def run(cell: ConfirmatoryCell) -> dict[str, Any]:
        """합성 셀 결과를 반환하며 호출 순서를 기록한다."""
        calls.append(cell.key)
        return copy.deepcopy(by_key[cell.key])

    result = execute_confirmatory_matrix(
        manifest=manifest,
        candidate_lock=candidate_lock,
        output_dir=tmp_path / "run",
        cell_runner=run,
    )
    assert result["acceptance_gate"]["passed"] is True
    assert len(calls) == 12
    assert (tmp_path / "run" / RESULT_JSON_FILENAME).is_file()
    assert (tmp_path / "run" / RESULT_MARKDOWN_FILENAME).is_file()
    with pytest.raises(ValueError, match="run claim"):
        execute_confirmatory_matrix(
            manifest=manifest,
            candidate_lock=candidate_lock,
            output_dir=tmp_path / "different-run-output",
            cell_runner=run,
        )
    assert len(calls) == 12


def test_runner_rejects_nonempty_output_before_consuming_run_claim(
    tmp_path: Path,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> None:
    """예측 가능한 output 충돌은 claim 생성이나 셀 접근 전에 거부한다."""
    output_dir = tmp_path / "nonempty"
    output_dir.mkdir()
    (output_dir / "existing.txt").write_text("existing", encoding="utf-8")
    called = False

    def forbidden(_: ConfirmatoryCell) -> dict[str, Any]:
        """Output preflight 실패 뒤 호출되면 테스트를 실패시킨다."""
        nonlocal called
        called = True
        raise AssertionError("cell runner를 호출하면 안 됩니다.")

    with pytest.raises(ValueError, match="output dir"):
        execute_confirmatory_matrix(
            manifest=manifest,
            candidate_lock=candidate_lock,
            output_dir=output_dir,
            cell_runner=forbidden,
        )

    assert called is False
    assert not candidate_run_claim_path(candidate_lock).exists()


def test_cli_rejects_head_lock_mismatch_before_building_data_runner(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    candidate_lock: CandidateLock,
) -> None:
    """CLI는 현재 HEAD와 lock commit이 다르면 어떤 셀 runner도 만들지 않는다."""
    repo = tmp_path / "repo"
    repo.mkdir()
    monkeypatch.setattr(
        runner_module,
        "read_git_state",
        lambda _: GitState("feature/rebalance-policy-v3", "b" * 40, False),
    )
    runner_built = False

    def forbidden_runner(*args, **kwargs):
        """Preflight 실패 뒤 호출되면 테스트를 실패시킨다."""
        del args, kwargs
        nonlocal runner_built
        runner_built = True
        raise AssertionError("data runner를 만들면 안 됩니다.")

    monkeypatch.setattr(runner_module, "_backtest_runner", forbidden_runner)

    with pytest.raises(ValueError, match="candidate lock"):
        runner_module.main(
            (
                "run",
                "--manifest",
                str(MANIFEST_PATH),
                "--sidecar",
                str(SIDECAR_PATH),
                "--repo-root",
                str(repo),
                "--candidate-lock",
                candidate_lock.path,
                "--output-dir",
                str(tmp_path / "output"),
            )
        )
    assert runner_built is False


def _copy_registered_manifest(tmp_path: Path) -> tuple[Path, Path]:
    """등록 manifest와 sidecar를 원래 basename으로 복사한다."""
    manifest_path = tmp_path / REGISTERED_MANIFEST_FILENAME
    sidecar_path = tmp_path / REGISTERED_SIDECAR_FILENAME
    manifest_path.write_bytes(MANIFEST_PATH.read_bytes())
    sidecar_path.write_bytes(SIDECAR_PATH.read_bytes())
    return manifest_path, sidecar_path


def _passing_artifacts(
    manifest: ConfirmatoryManifest,
) -> list[RawResultArtifact]:
    """8/12 셀 180분 empty 개선을 갖는 합성 raw artifact를 만든다."""
    artifacts = []
    for index, cell in enumerate(manifest.cells):
        document = _raw_document(cell, improves_180=index < 8)
        payload = json.dumps(document, sort_keys=True, default=str).encode("utf-8")
        artifacts.append(
            RawResultArtifact(
                path=f"/fixture/{cell.slug}-policy.json",
                sha256=hashlib.sha256(payload).hexdigest(),
                document=document,
            )
        )
    return artifacts


def _raw_document(cell: ConfirmatoryCell, *, improves_180: bool) -> dict[str, Any]:
    """실제 PolicyBacktestResult surface와 같은 합성 raw 문서를 만든다."""
    contracts = [
        EvaluationContract(
            target_date=cell.target_date,
            start_hour=cell.start_hour,
            evaluation_minutes=minutes,
        ).audit_document()
        for minutes in (60, 120, 180)
    ]
    durations = []
    for minutes in (60, 120, 180):
        baseline_empty = float(minutes)
        baseline_unfulfilled = 10
        candidate_unfulfilled = (
            9 if minutes == 180 and improves_180 else baseline_unfulfilled
        )
        candidate_empty = (
            baseline_empty - 18.0
            if minutes == 180 and improves_180
            else baseline_empty
        )
        durations.append(
            {
                "evaluation_minutes": minutes,
                "station_count": 20,
                "legacy_movement": {},
                "legacy_timing": [],
                "no_rebalance": _policy_metrics(
                    cell,
                    minutes,
                    policy="no_rebalance",
                    configuration={
                        **LEGACY_REBALANCE_POLICY.audit_document(),
                        "max_stops_per_route": MAX_STOPS_PER_ROUTE,
                    },
                    empty_minutes=baseline_empty,
                    unfulfilled_requests=baseline_unfulfilled,
                    planned=0,
                    moved=0,
                    routes=0,
                ),
                "model_policies": [
                    _policy_metrics(
                        cell,
                        minutes,
                        policy=PRODUCTION_POLICY_NAME,
                        configuration=production_policy_configuration(),
                        empty_minutes=candidate_empty,
                        unfulfilled_requests=candidate_unfulfilled,
                        planned=5,
                        moved=5,
                        routes=1,
                    )
                ],
            }
        )
    month = f"{cell.target_date.year % 100:02d}{cell.target_date.month:02d}"
    return {
        "evidence_grade": "retrospective_heldout_replay",
        "target_date": cell.target_date.isoformat(),
        "center_id": cell.center_id,
        "center_name": cell.center_id,
        "start_hour": cell.start_hour,
        "model_bundle_root": "/fixture/aws-temporary-model-2025-d20-h12-r20",
        "model_bundle_sha256": PRODUCTION_MODEL_BUNDLE_SHA256,
        "source_trip_count": 100,
        "source_provenance": {
            "rental_csv": _source_file(
                f"/fixture/서울특별시 공공자전거 대여이력 정보_{month}.csv"
            ),
            "stock_csv": _source_file(
                f"/fixture/대여소별 공공자전거 대여가능 수량_{month}.csv"
            ),
            "weather_csv": {
                **_source_file("/fixture/weather_realtime_2025.csv"),
                "sha256": PRODUCTION_WEATHER_SHA256,
            },
            "population_csvs": [
                _source_file(
                    "/fixture/250_LOCAL_RESD_"
                    f"{cell.target_date - timedelta(days=7):%Y%m%d}.csv"
                )
            ],
            "station_master_content_sha256": "c" * 64,
            "station_crosswalk_count": 100,
            "station_crosswalk_sha256": hashlib.sha256(month.encode()).hexdigest(),
            "population_excluded_station_count": 0,
            "population_excluded_grid_ids": [],
            "backtest_contract_version": BACKTEST_CONTRACT_VERSION,
            "route_algorithm_version": ROUTE_ALGORITHM_VERSION,
            "urgency_scoring_config_version": URGENCY_SCORING_CONFIG_VERSION,
        },
        "evidence_gate": {
            "point_in_time_feature_inputs": True,
            "operation_contract_passed": True,
            "legacy_endpoint_reconciliation_passed": True,
            "heldout_day_of_month": True,
            "same_bike_movement_budget_cap_enforced": True,
        },
        "contracts": contracts,
        "durations": durations,
    }


def _policy_metrics(
    cell: ConfirmatoryCell,
    minutes: int,
    *,
    policy: str,
    configuration: dict[str, Any],
    empty_minutes: float,
    unfulfilled_requests: int,
    planned: int,
    moved: int,
    routes: int,
) -> dict[str, Any]:
    """Raw validator의 exact SimulationMetrics surface를 만든다."""
    start = datetime.combine(cell.target_date, datetime.min.time(), tzinfo=SEOUL)
    start += timedelta(hours=cell.start_hour)
    end = start + timedelta(minutes=minutes)
    return {
        "policy": policy,
        "policy_configuration": configuration,
        "window_start": start.isoformat(),
        "window_end": end.isoformat(),
        "observed_requests": 100,
        "fulfilled_requests": 100 - unfulfilled_requests,
        "unfulfilled_requests": unfulfilled_requests,
        "observed_demand_fulfillment_rate": (
            (100 - unfulfilled_requests) / 100
        ),
        "empty_station_minutes": empty_minutes,
        "moved_bikes": moved,
        "planned_bikes": planned,
        "dispatched_routes": routes,
        "completed_routes_by_cutoff": routes,
        "trucks_still_busy_at_cutoff": 0,
        "executed_stops": routes * 2,
        "vehicle_busy_minutes": float(routes * 10),
        "decision_ticks": minutes // 5,
        "movement_budget": planned,
        "movement_budget_used": planned,
        "cold_start_stock_history_minutes": 25,
        "unfulfilled_request_log": _unfulfilled_request_log(
            cell,
            minutes,
            unfulfilled_requests,
        ),
        "job_audits": _job_audits(
            cell,
            minutes,
            planned=planned,
            moved=moved,
            routes=routes,
        ),
        "tick_audits": [],
    }


def _unfulfilled_request_log(
    cell: ConfirmatoryCell,
    minutes: int,
    count: int,
) -> list[dict[str, Any]]:
    """Baseline과 candidate가 공유할 canonical 미충족 event prefix를 만든다."""
    start = datetime.combine(cell.target_date, datetime.min.time(), tzinfo=SEOUL)
    start += timedelta(hours=cell.start_hour)
    return [
        {
            "bike_id": f"SPB-FIXTURE-{index:02d}",
            "rented_at": (start + timedelta(seconds=index + 1)).isoformat(),
            "station_no": 1000 + index,
        }
        for index in range(count)
    ]


def _job_audits(
    cell: ConfirmatoryCell,
    minutes: int,
    *,
    planned: int,
    moved: int,
    routes: int,
) -> list[dict[str, Any]]:
    """Pickup 지연 10분인 완결 합성 job audit를 만든다."""
    if routes == 0:
        return []
    if routes != 1:
        raise ValueError("합성 fixture는 단일 route만 지원합니다.")
    start = datetime.combine(cell.target_date, datetime.min.time(), tzinfo=SEOUL)
    start += timedelta(hours=cell.start_hour)
    pickup_at = start + timedelta(minutes=10)
    dropoff_at = start + timedelta(minutes=20)
    return_at = start + timedelta(minutes=25)
    if return_at > start + timedelta(minutes=minutes):
        raise ValueError("합성 route가 평가 창 안에 끝나지 않습니다.")
    return [
        {
            "route_id": f"fixture-{cell.slug}-{minutes}",
            "truck_id": 0,
            "dispatched_at": start.isoformat(),
            "completed_at": dropoff_at.isoformat(),
            "return_at": return_at.isoformat(),
            "planned_bikes": planned,
            "moved_bikes": moved,
            "stop_count": 2,
            "stops": [
                {
                    "visit_no": 1,
                    "station_no": 2001,
                    "station_id": "ST-2001",
                    "action": "pickup",
                    "executed_at": pickup_at.isoformat(),
                    "planned_quantity": planned,
                    "actual_quantity": moved,
                },
                {
                    "visit_no": 2,
                    "station_no": 2002,
                    "station_id": "ST-2002",
                    "action": "dropoff",
                    "executed_at": dropoff_at.isoformat(),
                    "planned_quantity": planned,
                    "actual_quantity": moved,
                },
            ],
        }
    ]


def _source_file(path: str) -> dict[str, Any]:
    """합성 source file provenance를 만든다."""
    return {
        "path": path,
        "size_bytes": 100,
        "sha256": hashlib.sha256(path.encode()).hexdigest(),
    }


def _mutate_gate_input(
    artifacts: list[RawResultArtifact],
    mutation: str,
) -> None:
    """구조 항등식은 유지하며 지정 acceptance 조건만 깨뜨린다."""
    if mutation == "aggregate_unfulfilled_not_strict":
        for artifact in artifacts:
            duration = artifact.document["durations"][2]
            candidate = duration["model_policies"][0]
            baseline = duration["no_rebalance"]
            candidate["fulfilled_requests"] = baseline["fulfilled_requests"]
            candidate["unfulfilled_requests"] = baseline["unfulfilled_requests"]
            candidate["observed_demand_fulfillment_rate"] = baseline[
                "observed_demand_fulfillment_rate"
            ]
            candidate["unfulfilled_request_log"] = copy.deepcopy(
                baseline["unfulfilled_request_log"]
            )
        return
    if mutation == "aggregate_reduction_below_five":
        for artifact in artifacts:
            candidate = artifact.document["durations"][2]["model_policies"][0]
            baseline = artifact.document["durations"][2]["no_rebalance"]
            candidate["empty_station_minutes"] = baseline["empty_station_minutes"]
        return
    if mutation == "only_seven_improved":
        improved = [
            artifact
            for artifact in artifacts
            if artifact.document["durations"][2]["model_policies"][0][
                "empty_station_minutes"
            ]
            < artifact.document["durations"][2]["no_rebalance"][
                "empty_station_minutes"
            ]
        ]
        candidate = improved[-1].document["durations"][2]["model_policies"][0]
        baseline = improved[-1].document["durations"][2]["no_rebalance"]
        candidate["empty_station_minutes"] = baseline["empty_station_minutes"]
        return
    candidate = artifacts[0].document["durations"][0]["model_policies"][0]
    baseline = artifacts[0].document["durations"][0]["no_rebalance"]
    if mutation == "pickup_dispatch_lag_worse":
        job = candidate["job_audits"][0]
        dispatched_at = datetime.fromisoformat(job["dispatched_at"])
        job["stops"][0]["executed_at"] = (
            dispatched_at + timedelta(minutes=31)
        ).isoformat()
        job["stops"][1]["executed_at"] = (
            dispatched_at + timedelta(minutes=40)
        ).isoformat()
        job["completed_at"] = job["stops"][1]["executed_at"]
        job["return_at"] = (dispatched_at + timedelta(minutes=50)).isoformat()
    elif mutation == "new_unfulfilled_transfer":
        replacement = copy.deepcopy(candidate["unfulfilled_request_log"][-1])
        replacement["bike_id"] = "SPB-FIXTURE-TRANSFER"
        replacement["station_no"] = 9999
        candidate["unfulfilled_request_log"][-1] = replacement
    elif mutation == "unfulfilled_worse":
        candidate["unfulfilled_requests"] = baseline["unfulfilled_requests"] + 1
        candidate["fulfilled_requests"] = candidate["observed_requests"] - candidate[
            "unfulfilled_requests"
        ]
        candidate["observed_demand_fulfillment_rate"] = (
            candidate["fulfilled_requests"] / candidate["observed_requests"]
        )
        additional = copy.deepcopy(candidate["unfulfilled_request_log"][-1])
        additional["bike_id"] = "SPB-FIXTURE-ADDITIONAL"
        additional["rented_at"] = (
            datetime.fromisoformat(candidate["window_start"]) + timedelta(seconds=30)
        ).isoformat()
        additional["station_no"] = 9998
        candidate["unfulfilled_request_log"].append(additional)
    elif mutation == "empty_worse":
        candidate["empty_station_minutes"] = baseline["empty_station_minutes"] + 1.0
    elif mutation == "planned_moved_mismatch":
        candidate["moved_bikes"] = candidate["planned_bikes"] - 1
        job = candidate["job_audits"][0]
        job["moved_bikes"] = candidate["moved_bikes"]
        for stop in job["stops"]:
            stop["actual_quantity"] = candidate["moved_bikes"]
    else:
        candidate["completed_routes_by_cutoff"] = 0
        candidate["trucks_still_busy_at_cutoff"] = 1
