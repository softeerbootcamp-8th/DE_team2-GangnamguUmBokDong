"""로컬 Airflow realtime E2E를 fixture부터 결과 판정까지 한 번에 실행한다."""

from __future__ import annotations

import argparse
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
_DAG_ID = "e2e_realtime"
_TERMINAL_STATES = {"failed", "success"}


def _compose_command() -> list[str]:
    """현재 host platform과 .env를 반영한 Compose 명령 prefix를 만든다."""
    command = ["docker", "compose"]
    if (_ROOT / ".env").is_file():
        command.extend(("--env-file", ".env"))
    command.extend(("-f", "ops/compose/docker-compose.yml"))
    platform = subprocess.run(
        ["bash", "ops/compose/platform_args.sh"],
        cwd=_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if platform:
        command.extend(shlex.split(platform))
    return command


def _compose_exec(
    compose: list[str],
    command: str,
    *,
    capture: bool = False,
) -> str:
    """Airflow scheduler 컨테이너에서 shell command를 실행한다."""
    result = subprocess.run(
        [*compose, "exec", "-T", "airflow-scheduler", "sh", "-lc", command],
        cwd=_ROOT,
        check=True,
        capture_output=capture,
        text=True,
    )
    return result.stdout if capture else ""


def _window_start(now: datetime) -> datetime:
    """DAG Jinja template와 같은 방식으로 KST 5분 경계를 계산한다."""
    return now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)


def _dag_state(compose: list[str], run_id: str) -> str:
    """Airflow CLI 출력의 마지막 줄에서 DAG run 상태를 읽는다."""
    output = _compose_exec(
        compose,
        (
            "cd /workspace/airflow && "
            f"uv run airflow dags state {_DAG_ID} {shlex.quote(run_id)}"
        ),
        capture=True,
    )
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    if not lines:
        raise RuntimeError("Airflow DAG 상태 출력이 비었습니다.")
    return lines[-1]


def _airflow_port() -> str:
    """Process 환경과 로컬 .env 순서로 Airflow port를 결정한다."""
    configured = os.environ.get("AIRFLOW_WEBSERVER_PORT")
    if configured:
        return configured
    env_path = _ROOT / ".env"
    if env_path.is_file():
        for line in env_path.read_text(encoding="utf-8").splitlines():
            key, separator, value = line.partition("=")
            if separator and key.strip() == "AIRFLOW_WEBSERVER_PORT":
                return value.strip() or "8080"
    return "8080"


def run(timeout_seconds: int) -> int:
    """Fixture를 준비하고 DAG를 trigger한 뒤 terminal state까지 기다린다."""
    compose = _compose_command()
    running = subprocess.run(
        [*compose, "ps", "--status", "running", "--quiet", "airflow-scheduler"],
        cwd=_ROOT,
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if not running:
        raise RuntimeError("airflow-scheduler가 실행 중이 아닙니다. 먼저 `make up`을 실행하세요.")

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    window = _window_start(now)
    run_logical = now.replace(microsecond=0)
    run_id = f"local_e2e__{run_logical:%Y%m%dT%H%M%S}"
    window_text = window.isoformat()
    print(f"[e2e] fixture 준비: window={window_text}", flush=True)
    _compose_exec(
        compose,
        (
            "cd /workspace/loader && env -u VIRTUAL_ENV "
            "uv run --frozen python local_e2e.py seed "
            f"--logical-dttm {shlex.quote(window_text)}"
        ),
    )

    print(f"[e2e] DAG trigger: run_id={run_id}", flush=True)
    _compose_exec(
        compose,
        (
            "cd /workspace/airflow && uv run airflow dags trigger "
            f"{_DAG_ID} --logical-date {shlex.quote(run_logical.isoformat())} "
            f"--run-id {shlex.quote(run_id)}"
        ),
    )
    ui_url = f"http://localhost:{_airflow_port()}/dags/{_DAG_ID}"
    print(f"[e2e] Airflow UI: {ui_url}", flush=True)

    deadline = time.monotonic() + timeout_seconds
    previous = None
    while time.monotonic() < deadline:
        state = _dag_state(compose, run_id)
        if state != previous:
            print(f"[e2e] state={state}", flush=True)
            previous = state
        if state in _TERMINAL_STATES:
            _compose_exec(
                compose,
                (
                    "cd /workspace/airflow && uv run airflow tasks "
                    f"states-for-dag-run {_DAG_ID} {shlex.quote(run_id)}"
                ),
            )
            if state == "success":
                print("[e2e] SUCCESS: 13개 태스크가 모두 성공했습니다.", flush=True)
                return 0
            print(f"[e2e] FAILED: Airflow UI에서 {run_id} 로그를 확인하세요.", file=sys.stderr)
            return 1
        time.sleep(5)
    raise TimeoutError(f"E2E run이 {timeout_seconds}초 안에 끝나지 않았습니다: {run_id}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Smoke runner CLI 인자를 파싱한다."""
    parser = argparse.ArgumentParser(description="로컬 Airflow E2E smoke를 실행한다.")
    parser.add_argument("--timeout-seconds", type=int, default=900)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """예외 원인을 보존하면서 CLI 실패를 nonzero로 변환한다."""
    args = parse_args(argv)
    if args.timeout_seconds <= 0:
        print("--timeout-seconds는 양수여야 합니다.", file=sys.stderr)
        return 2
    try:
        return run(args.timeout_seconds)
    except (OSError, subprocess.CalledProcessError, RuntimeError, TimeoutError) as exc:
        print(f"[e2e] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
