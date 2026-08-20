"""로컬 Airflow realtime E2E를 fixture부터 결과 판정까지 한 번에 실행한다."""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

_ROOT = Path(__file__).resolve().parent.parent
_DAG_ID = "realtime_5min"


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
    timeout_seconds: int | None = None,
) -> str:
    """Airflow scheduler 컨테이너에서 shell command를 실행한다."""
    result = subprocess.run(
        [*compose, "exec", "-T", "airflow-scheduler", "sh", "-lc", command],
        cwd=_ROOT,
        check=True,
        capture_output=capture,
        text=True,
        timeout=timeout_seconds,
    )
    return result.stdout if capture else ""


def _window_start(now: datetime) -> datetime:
    """DAG Jinja template와 같은 방식으로 KST 5분 경계를 계산한다."""
    return now.replace(minute=(now.minute // 5) * 5, second=0, microsecond=0)


def _dag_is_paused(compose: list[str]) -> bool:
    """Airflow JSON 목록에서 운영 DAG의 현재 pause 상태를 읽는다."""
    output = _compose_exec(
        compose,
        "cd /workspace/airflow && uv run airflow dags list --output json",
        capture=True,
    )
    json_lines = [
        line.strip() for line in output.splitlines() if line.lstrip().startswith("[{")
    ]
    if not json_lines:
        raise RuntimeError("Airflow DAG JSON 목록을 찾을 수 없습니다.")
    dags = json.loads(json_lines[-1])
    for item in dags:
        if item.get("dag_id") == _DAG_ID:
            return str(item.get("is_paused")).lower() == "true"
    raise RuntimeError(f"Airflow DAG를 찾을 수 없습니다: {_DAG_ID}")


def _wait_for_paused_dag(compose: list[str], timeout_seconds: int) -> None:
    """초기 DAG parsing이 끝나고 paused 운영 DAG가 조회될 때까지 기다린다."""
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None
    print(f"[e2e] Airflow DAG 등록 대기: dag_id={_DAG_ID}", flush=True)
    while time.monotonic() < deadline:
        try:
            is_paused = _dag_is_paused(compose)
        except (subprocess.CalledProcessError, RuntimeError) as exc:
            last_error = exc
            time.sleep(2)
            continue
        if not is_paused:
            raise RuntimeError(
                "realtime_5min이 unpaused 상태입니다. 자동 스케줄과 writer 충돌을 "
                "피하려면 먼저 DAG를 pause하세요."
            )
        return
    raise RuntimeError(
        f"{timeout_seconds}초 안에 paused {_DAG_ID} DAG를 확인하지 못했습니다: "
        f"{last_error}"
    )


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
    """Fixture를 준비하고 paused 운영 DAG를 test run으로 실행한다."""
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
    _wait_for_paused_dag(compose, min(timeout_seconds, 120))

    now = datetime.now(ZoneInfo("Asia/Seoul"))
    window = _window_start(now)
    run_logical = now.replace(microsecond=0)
    run_id = f"manual__{run_logical.isoformat()}"
    window_text = window.isoformat()
    station_source_text = (window - timedelta(minutes=5)).isoformat()
    print(
        f"[e2e] 직전 station source 준비: window={station_source_text}",
        flush=True,
    )
    _compose_exec(
        compose,
        (
            "cd /workspace/collector && env -u VIRTUAL_ENV "
            "UV_PROJECT_ENVIRONMENT=/opt/venvs/modules/collector "
            "uv run --frozen python main.py --source bike_station_realtime "
            f"--window-start {shlex.quote(station_source_text)}"
        ),
    )
    print(f"[e2e] fixture 준비: window={window_text}", flush=True)
    _compose_exec(
        compose,
        (
            "cd /workspace/loader && env -u VIRTUAL_ENV "
            "LOCAL_E2E_ALLOW_FIXTURE=1 uv run --frozen python local_e2e.py seed "
            f"--logical-dttm {shlex.quote(window_text)}"
        ),
    )

    print(f"[e2e] paused 운영 DAG test run: run_id={run_id}", flush=True)
    _compose_exec(
        compose,
        (
            "cd /workspace/airflow && uv run python /workspace/ops/airflow_dag_test.py "
            f"--logical-dttm {shlex.quote(run_logical.isoformat())}"
        ),
        timeout_seconds=timeout_seconds,
    )
    ui_url = f"http://localhost:{_airflow_port()}/dags/{_DAG_ID}"
    print(f"[e2e] Airflow UI: {ui_url}", flush=True)

    if not _dag_is_paused(compose):
        raise RuntimeError("smoke 실행 중 realtime_5min pause 상태가 바뀌었습니다.")
    _compose_exec(
        compose,
        (
            "cd /workspace/airflow && uv run airflow tasks "
            f"states-for-dag-run {_DAG_ID} {shlex.quote(run_id)}"
        ),
    )
    print("[e2e] SUCCESS: realtime_5min 전체 태스크가 성공했습니다.", flush=True)
    return 0


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
    except (
        OSError,
        subprocess.CalledProcessError,
        subprocess.TimeoutExpired,
        RuntimeError,
    ) as exc:
        print(f"[e2e] ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
