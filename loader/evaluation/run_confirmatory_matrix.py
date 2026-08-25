"""사전 고정 confirmatory matrix를 단일 후보로 한 번 실행하는 CLI를 제공한다."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

from gold.rebalance_policy import DEFAULT_REBALANCE_POLICY
from gold.rebalance_route import MAX_STOPS_PER_ROUTE

from .confirmatory_matrix import (
    CandidateLock,
    ConfirmatoryCell,
    ConfirmatoryManifest,
    RawResultArtifact,
    RESULT_SCHEMA_VERSION,
    load_candidate_lock,
    load_confirmatory_manifest,
    load_raw_result,
    load_run_claim,
    run_claim_document,
    validate_confirmatory_results,
    write_candidate_lock,
    write_confirmatory_result,
)
from .production_policy_contract import PRODUCTION_POLICY_NAME
from .run_policy_backtest import PolicyVariant, run_policy_backtest

DEFAULT_MANIFEST_PATH = (
    Path(__file__).with_name("manifests") / "confirmatory-matrix-v2.json"
)
DEFAULT_SIDECAR_PATH = (
    Path(__file__).with_name("manifests") / "confirmatory-matrix-v2.sha256"
)
RESULT_JSON_FILENAME = f"{RESULT_SCHEMA_VERSION}.json"
RESULT_MARKDOWN_FILENAME = f"{RESULT_SCHEMA_VERSION}.md"


@dataclass(frozen=True, slots=True)
class GitState:
    """실행 코드를 고정하는 현재 Git branch·commit·worktree 상태를 표현한다."""

    branch: str
    commit: str
    worktree_dirty: bool


CellRunner = Callable[[ConfirmatoryCell], Mapping[str, Any]]


def read_git_state(repo_root: Path) -> GitState:
    """Shell 확장 없이 Git branch·commit·전체 worktree 변경 여부를 읽는다."""

    def run(*arguments: str) -> str:
        """한 Git 명령의 stdout을 반환하고 실패는 설명 가능한 오류로 바꾼다."""
        try:
            completed = subprocess.run(
                ("git", *arguments),
                cwd=repo_root,
                check=True,
                capture_output=True,
                text=True,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise ValueError(f"Git 상태를 읽을 수 없습니다: {' '.join(arguments)}") from exc
        return completed.stdout.strip()

    return GitState(
        branch=run("branch", "--show-current"),
        commit=run("rev-parse", "HEAD"),
        worktree_dirty=bool(run("status", "--porcelain", "--untracked-files=all")),
    )


def validate_git_state(state: GitState, manifest: ConfirmatoryManifest) -> None:
    """Candidate code가 등록 branch의 clean worktree commit인지 검증한다."""
    if state.branch != manifest.document["branch"]:
        raise ValueError(
            "현재 Git branch가 confirmatory manifest와 다릅니다: "
            f"expected={manifest.document['branch']}, actual={state.branch}"
        )
    if state.worktree_dirty:
        raise ValueError("worktree가 dirty여서 candidate commit을 고정할 수 없습니다.")
    if len(state.commit) != 40 or any(
        character not in "0123456789abcdef" for character in state.commit
    ):
        raise ValueError("현재 Git commit이 full lowercase SHA-1이 아닙니다.")


def create_run_claim(
    path: Path,
    *,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
) -> Path:
    """원천 접근 전에 단일 confirmatory 시도를 배타적으로 기록한다."""
    document = run_claim_document(manifest, candidate_lock)
    payload = _json_bytes(document)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        with path.open("xb") as stream:
            stream.write(payload)
    except FileExistsError as exc:
        raise ValueError(
            "confirmatory run claim이 이미 존재해 단일 실행 계약상 재실행할 수 없습니다: "
            f"{path}"
        ) from exc
    return path


def candidate_run_claim_path(candidate_lock: CandidateLock) -> Path:
    """Output dir 변경으로 재실행할 수 없도록 claim을 lock 옆에 고정한다."""
    lock_path = Path(candidate_lock.path)
    return lock_path.with_name(f"{lock_path.name}.run-claim.json")


def execute_confirmatory_matrix(
    *,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    output_dir: Path,
    cell_runner: CellRunner,
) -> dict[str, Any]:
    """Claim을 먼저 고정하고 exact 12셀을 실행·검증·기록한다."""
    current_lock = load_candidate_lock(
        Path(candidate_lock.path),
        expected_manifest_sha256=manifest.sha256,
        expected_git_commit=candidate_lock.git_commit,
    )
    if current_lock.sha256 != candidate_lock.sha256:
        raise ValueError("실행 전 candidate lock byte SHA가 변경됐습니다.")
    if output_dir.exists() and (
        not output_dir.is_dir() or any(output_dir.iterdir())
    ):
        raise ValueError("confirmatory output dir은 실행 전에 없거나 비어 있어야 합니다.")
    claim_path = candidate_run_claim_path(candidate_lock)
    create_run_claim(
        claim_path,
        manifest=manifest,
        candidate_lock=candidate_lock,
    )
    run_claim = load_run_claim(
        claim_path,
        manifest=manifest,
        candidate_lock=candidate_lock,
    )
    raw_dir = output_dir / "raw"
    artifacts: list[RawResultArtifact] = []
    for index, cell in enumerate(manifest.cells, start=1):
        print(
            f"[{index}/{len(manifest.cells)}] {cell.slug} confirmatory 시작",
            flush=True,
        )
        document = cell_runner(cell)
        if not isinstance(document, Mapping):
            raise ValueError(f"{cell.slug} runner가 JSON object를 반환하지 않았습니다.")
        raw_path = raw_dir / f"{cell.slug}-policy.json"
        raw_path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with raw_path.open("xb") as stream:
                stream.write(_json_bytes(document))
        except FileExistsError as exc:
            raise ValueError(f"confirmatory raw 결과가 이미 존재합니다: {raw_path}") from exc
        artifacts.append(load_raw_result(raw_path))
        print(
            f"[{index}/{len(manifest.cells)}] {cell.slug} confirmatory 완료",
            flush=True,
        )
    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )
    write_confirmatory_result(
        result,
        json_path=output_dir / RESULT_JSON_FILENAME,
        markdown_path=output_dir / RESULT_MARKDOWN_FILENAME,
    )
    return result


def validate_existing_results(
    *,
    manifest: ConfirmatoryManifest,
    candidate_lock: CandidateLock,
    raw_paths: Sequence[Path],
    output_dir: Path,
) -> dict[str, Any]:
    """이미 생성된 raw 파일을 실행 없이 검증하고 최종 결과를 기록한다."""
    artifacts = tuple(load_raw_result(path) for path in raw_paths)
    run_claim = load_run_claim(
        candidate_run_claim_path(candidate_lock),
        manifest=manifest,
        candidate_lock=candidate_lock,
    )
    result = validate_confirmatory_results(
        artifacts,
        manifest=manifest,
        candidate_lock=candidate_lock,
        run_claim=run_claim,
    )
    write_confirmatory_result(
        result,
        json_path=output_dir / RESULT_JSON_FILENAME,
        markdown_path=output_dir / RESULT_MARKDOWN_FILENAME,
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    """Candidate lock·실행·기존 raw 검증 subcommand 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="사전 등록 confirmatory matrix 실행기")
    subparsers = parser.add_subparsers(dest="command", required=True)

    lock_parser = subparsers.add_parser("lock", help="단일 production candidate 고정")
    _add_preflight_arguments(lock_parser)
    lock_parser.add_argument("--candidate-lock", required=True, type=Path)

    run_parser = subparsers.add_parser("run", help="고정 matrix를 한 번 실행")
    _add_preflight_arguments(run_parser)
    _add_lock_argument(run_parser)
    run_parser.add_argument("--output-dir", required=True, type=Path)
    run_parser.add_argument(
        "--bootstrap-dir",
        type=Path,
        default=Path("../data/issue163-full-year/bootstrap"),
    )
    run_parser.add_argument(
        "--weather-csv",
        type=Path,
        default=Path(
            "../data/issue163-full-year/bootstrap/weather_realtime_2025.csv"
        ),
    )
    run_parser.add_argument(
        "--population-dir",
        type=Path,
        default=Path("../data/issue163-full-year/population"),
    )
    run_parser.add_argument(
        "--model-bundle",
        type=Path,
        default=Path("../models/aws-temporary-model-2025-d20-h12-r20"),
    )
    run_parser.add_argument(
        "--center-seed",
        type=Path,
        default=Path("../docs/gold/dispatch-center-seed.yaml"),
    )
    run_parser.add_argument(
        "--s3-endpoint",
        default=os.environ.get("S3_ENDPOINT_URL", "http://localhost:9000"),
    )
    run_parser.add_argument("--s3-bucket", default="issue163-full-year")
    run_parser.add_argument(
        "--access-key",
        default=os.environ.get("AWS_ACCESS_KEY_ID", "minioadmin"),
    )
    run_parser.add_argument(
        "--secret-key",
        default=os.environ.get("AWS_SECRET_ACCESS_KEY", "minioadmin"),
    )

    validate_parser = subparsers.add_parser(
        "validate", help="기존 raw exact set과 acceptance gate 검증"
    )
    _add_preflight_arguments(validate_parser)
    _add_lock_argument(validate_parser)
    validate_parser.add_argument("--raw-results", nargs="+", required=True, type=Path)
    validate_parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    """Manifest·Git·candidate를 preflight한 뒤 선택한 작업을 수행한다."""
    args = parse_args(argv)
    manifest = load_confirmatory_manifest(args.manifest, args.sidecar)
    state = read_git_state(args.repo_root)
    validate_git_state(state, manifest)
    if args.command == "lock":
        _require_outside_repo(args.candidate_lock, args.repo_root, "candidate lock")
        lock = write_candidate_lock(
            args.candidate_lock,
            manifest_sha256=manifest.sha256,
            git_commit=state.commit,
        )
        print(f"Candidate lock: {args.candidate_lock}")
        print(f"Candidate lock SHA-256: {lock.sha256}")
        return 0
    _require_outside_repo(args.candidate_lock, args.repo_root, "candidate lock")
    candidate_lock = load_candidate_lock(
        args.candidate_lock,
        expected_manifest_sha256=manifest.sha256,
        expected_git_commit=state.commit,
    )
    if args.command == "validate":
        _require_outside_repo(args.output_dir, args.repo_root, "output dir")
        result = validate_existing_results(
            manifest=manifest,
            candidate_lock=candidate_lock,
            raw_paths=args.raw_results,
            output_dir=args.output_dir,
        )
    else:
        _require_outside_repo(args.output_dir, args.repo_root, "output dir")
        expected_model_name = manifest.document["evaluation_contract"]["model_bundle"]
        if args.model_bundle.name != expected_model_name:
            raise ValueError(
                "--model-bundle 이름이 confirmatory manifest와 다릅니다: "
                f"expected={expected_model_name}, actual={args.model_bundle.name}"
            )
        result = execute_confirmatory_matrix(
            manifest=manifest,
            candidate_lock=candidate_lock,
            output_dir=args.output_dir,
            cell_runner=_backtest_runner(args, manifest),
        )
    json_path = args.output_dir / RESULT_JSON_FILENAME
    markdown_path = args.output_dir / RESULT_MARKDOWN_FILENAME
    print(f"Confirmatory JSON: {json_path}")
    print(f"Confirmatory Markdown: {markdown_path}")
    if not result["acceptance_gate"]["passed"]:
        print("Confirmatory acceptance: FAILED")
        return 1
    print("Confirmatory acceptance: PASSED")
    return 0


def _add_preflight_arguments(parser: argparse.ArgumentParser) -> None:
    """모든 subcommand에 같은 manifest·Git 인자를 추가한다."""
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH)
    parser.add_argument("--sidecar", type=Path, default=DEFAULT_SIDECAR_PATH)
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[2],
    )


def _add_lock_argument(parser: argparse.ArgumentParser) -> None:
    """실행·검증 subcommand에 필수 candidate lock 인자를 추가한다."""
    parser.add_argument("--candidate-lock", required=True, type=Path)


def _require_outside_repo(path: Path, repo_root: Path, label: str) -> None:
    """Lock·산출물이 Git cleanliness를 오염시키지 않도록 repo 밖 경로를 강제한다."""
    resolved_path = path.resolve()
    resolved_repo = repo_root.resolve()
    if resolved_path == resolved_repo or resolved_repo in resolved_path.parents:
        raise ValueError(f"{label}은 repository 밖 경로여야 합니다: {path}")


def _backtest_runner(
    args: argparse.Namespace,
    manifest: ConfirmatoryManifest,
) -> CellRunner:
    """고정 CLI 입력으로 실제 point-in-time 셀 runner를 만든다."""
    contract = manifest.document["evaluation_contract"]

    def run(cell: ConfirmatoryCell) -> Mapping[str, Any]:
        """한 manifest 셀에서 단일 production 후보만 평가한다."""
        month = f"{cell.target_date.year % 100:02d}{cell.target_date.month:02d}"
        result = run_policy_backtest(
            target_date=cell.target_date,
            center_id=cell.center_id,
            start_hour=cell.start_hour,
            evaluation_minutes=tuple(contract["evaluation_minutes"]),
            fleet_size=contract["fleet_size"],
            max_stops_variants=(MAX_STOPS_PER_ROUTE,),
            rental_csv=(
                args.bootstrap_dir / f"서울특별시 공공자전거 대여이력 정보_{month}.csv"
            ),
            stock_csv=(
                args.bootstrap_dir / f"대여소별 공공자전거 대여가능 수량_{month}.csv"
            ),
            weather_csv=args.weather_csv,
            population_dir=args.population_dir,
            model_bundle_root=args.model_bundle,
            center_seed=args.center_seed,
            endpoint_url=args.s3_endpoint,
            bucket=args.s3_bucket,
            access_key=args.access_key,
            secret_key=args.secret_key,
            policy_variants=(
                PolicyVariant(
                    name=PRODUCTION_POLICY_NAME,
                    max_stops_per_route=MAX_STOPS_PER_ROUTE,
                    policy_config=DEFAULT_REBALANCE_POLICY,
                ),
            ),
        )
        return _json_mapping(asdict(result))

    return run


def _json_mapping(value: object) -> Mapping[str, Any]:
    """Dataclass 결과를 실제 JSON round-trip 형태의 mapping으로 정규화한다."""
    payload = json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        default=_json_default,
    )
    loaded = json.loads(payload)
    if not isinstance(loaded, Mapping):
        raise ValueError("backtest 결과가 JSON object가 아닙니다.")
    return loaded


def _json_default(value: object) -> str:
    """JSON 표준형이 아닌 date·datetime만 ISO 문자열로 직렬화한다."""
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    raise TypeError(f"JSON으로 직렬화할 수 없는 타입입니다: {type(value).__name__}")


def _json_bytes(value: object) -> bytes:
    """감사용 JSON bytes를 key 정렬·NaN 금지로 결정적으로 만든다."""
    try:
        return (
            json.dumps(
                value,
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
                allow_nan=False,
                default=_json_default,
            )
            + "\n"
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("confirmatory 문서를 JSON으로 직렬화할 수 없습니다.") from exc


if __name__ == "__main__":
    raise SystemExit(main())
