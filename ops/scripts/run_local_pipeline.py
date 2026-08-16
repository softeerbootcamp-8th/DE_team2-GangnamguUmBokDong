#!/usr/bin/env python3
"""collector -> normalizer/nowcasting -> loader 로컬 통합 실행 오케스트레이터.

Airflow 없이, 소스별 실제 수집 주기(5분/10분/3시간/1일)에 맞춰 각 CLI를
직접 실행한다. 실 API(SEOUL_OPENAPI_KEY, KMA_APIHUB_KEY)와 로컬 docker-compose
(MinIO, Postgres)를 그대로 사용한다.

    python3 ops/scripts/run_local_pipeline.py

CUTOFF 이전까지 계속 돌며, 각 실행 결과는 ops/scripts/logs/ 아래에 남는다.
    - orchestrator.log : 사람이 읽는 전체 로그(스크립트 자체 stdout)
    - runs.jsonl        : 실행 1건당 1줄(JSON) 요약. 아침에 이것만 봐도 충분.

각 job의 collector 실행이 성공(exit 0)해야만 그 job에 연결된 downstream을
실행한다. 실패해도 오케스트레이터 자체는 죽지 않고 다음 tick으로 넘어간다.
"""

from __future__ import annotations

import json
import shlex
import subprocess
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

KST = ZoneInfo("Asia/Seoul")
REPO_ROOT = Path(__file__).resolve().parents[2]
ENV_FILE = REPO_ROOT / ".env"
LOG_DIR = Path(__file__).resolve().parent / "logs"
RUNS_LOG = LOG_DIR / "runs.jsonl"

# 사용자가 지정한 컷오프: 2026-08-16 07:00 KST. 다르면 이 한 줄만 바꾸면 된다.
CUTOFF = datetime(2026, 8, 16, 7, 0, tzinfo=KST)

_SHELL_PRELUDE = f"set -a; source {shlex.quote(str(ENV_FILE))}; set +a;"


def _log(message: str) -> None:
    line = f"[{datetime.now(tz=KST).isoformat()}] {message}"
    print(line, flush=True)


def _record_run(job: str, args: dict, returncode: int, duration: float, ok: bool) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    with RUNS_LOG.open("a", encoding="utf-8") as f:
        f.write(
            json.dumps(
                {
                    "ts": datetime.now(tz=KST).isoformat(),
                    "job": job,
                    "args": args,
                    "returncode": returncode,
                    "duration_sec": round(duration, 2),
                    "ok": ok,
                },
                ensure_ascii=False,
            )
            + "\n"
        )


def _run(job: str, cwd: Path, command: str, timeout: int, args: dict) -> bool:
    """`_SHELL_PRELUDE`로 .env를 로드한 뒤 command를 실행하고 성공 여부를 반환한다."""
    full_cmd = f"{_SHELL_PRELUDE} cd {shlex.quote(str(cwd))} && {command}"
    _log(f">>> [{job}] {command}")
    start = time.monotonic()
    try:
        proc = subprocess.run(
            ["bash", "-lc", full_cmd],
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        duration = time.monotonic() - start
        ok = proc.returncode == 0
        if proc.stdout.strip():
            _log(f"    stdout: {proc.stdout.strip()[-2000:]}")
        if proc.stderr.strip():
            _log(f"    stderr: {proc.stderr.strip()[-2000:]}")
        _log(f"<<< [{job}] exit={proc.returncode} ({duration:.1f}s) ok={ok}")
        _record_run(job, args, proc.returncode, duration, ok)
        return ok
    except subprocess.TimeoutExpired:
        duration = time.monotonic() - start
        _log(f"<<< [{job}] TIMEOUT after {timeout}s")
        _record_run(job, args, -1, duration, False)
        return False
    except Exception as exc:  # noqa: BLE001 - 오케스트레이터는 죽으면 안 되므로 넓게 잡는다
        duration = time.monotonic() - start
        _log(f"<<< [{job}] EXCEPTION: {exc}")
        _record_run(job, args, -2, duration, False)
        return False


def _iso(dt: datetime) -> str:
    return dt.isoformat()


# living_population_grid는 서울 전체 250m 격자 x 24시간(수십만 행)을 페이지네이션으로
# 받아오므로 다른 실시간 소스(4분)보다 훨씬 오래 걸린다(실측 약 5~10분).
_COLLECTOR_TIMEOUT_OVERRIDES = {"living_population_grid": 1200}


def run_collector(source_id: str, window_start: datetime) -> bool:
    return _run(
        f"collect:{source_id}",
        REPO_ROOT / "collector",
        f"uv run python main.py --source {shlex.quote(source_id)} --window-start {shlex.quote(_iso(window_start))}",
        timeout=_COLLECTOR_TIMEOUT_OVERRIDES.get(source_id, 240),
        args={"source_id": source_id, "window_start": _iso(window_start)},
    )


def run_normalizer(window_start: datetime) -> bool:
    return _run(
        "normalizer",
        REPO_ROOT / "seoul-pop-normalizer",
        f"uv run python main.py --window-start {shlex.quote(_iso(window_start))}",
        timeout=240,
        args={"window_start": _iso(window_start)},
    )


def run_nowcasting(target_date: datetime) -> bool:
    date_str = target_date.strftime("%Y-%m-%d")
    return _run(
        "nowcasting",
        REPO_ROOT / "seoul-pop-nowcasting",
        f"uv run python main.py estimate --target-date {date_str}",
        timeout=600,
        args={"target_date": date_str},
    )


def run_db_loader(table: str, window_start: datetime) -> bool:
    return _run(
        f"db_loader:{table}",
        REPO_ROOT / "loader",
        f"uv run python main.py --table {shlex.quote(table)} --window-start {shlex.quote(_iso(window_start))}",
        timeout=120,
        args={"table": table, "window_start": _iso(window_start)},
    )


def floor_to_interval(dt: datetime, minutes: int) -> datetime:
    """dt를 당일 00:00(KST) 기준 `minutes` 배수 경계로 내림한다."""
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elapsed_min = int((dt - midnight).total_seconds() // 60)
    floored_min = (elapsed_min // minutes) * minutes
    return midnight + timedelta(minutes=floored_min)


@dataclass
class Job:
    source_id: str
    interval_minutes: int
    downstream: list = field(default_factory=list)
    next_run: datetime = field(init=False)
    window_start: datetime = field(init=False)

    def __post_init__(self) -> None:
        now = datetime.now(tz=KST)
        self.window_start = floor_to_interval(now, self.interval_minutes)
        self.next_run = now  # 즉시 1회 실행(catch-up)부터 시작

    def execute(self) -> None:
        ok = run_collector(self.source_id, self.window_start)
        if ok:
            for step in self.downstream:
                step(self.window_start)
        self.window_start += timedelta(minutes=self.interval_minutes)
        self.next_run += timedelta(minutes=self.interval_minutes)


def _downstream_db_loader(table: str):
    return lambda window_start: run_db_loader(table, window_start)


def _downstream_normalizer(window_start: datetime) -> None:
    run_normalizer(window_start)


def _downstream_nowcasting(window_start: datetime) -> None:
    run_nowcasting(window_start)


JOBS: list[Job] = [
    Job("bike_station_realtime", 5, [_downstream_db_loader("stations"), _downstream_db_loader("station_stock")]),
    Job("population_realtime", 5, [_downstream_normalizer]),
    Job("bike_rental_history", 5, []),
    Job("weather_ultra_short_live", 10, [_downstream_db_loader("weather_current")]),
    Job("weather_short_term_forecast", 180, [_downstream_db_loader("weather_forecast")]),
    Job("cultural_event", 1440, [_downstream_db_loader("cultural_events")]),
    Job("living_population_grid", 1440, [_downstream_nowcasting]),
]


def main() -> int:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    _log(f"=== 로컬 파이프라인 오케스트레이터 시작 (cutoff={_iso(CUTOFF)}) ===")
    for job in JOBS:
        _log(f"job registered: {job.source_id} interval={job.interval_minutes}m first_window={_iso(job.window_start)}")

    while True:
        now = datetime.now(tz=KST)
        if now >= CUTOFF:
            _log("=== cutoff 도달, 오케스트레이터 종료 ===")
            return 0

        due = [j for j in JOBS if j.next_run <= now]
        if due:
            for job in due:
                job.execute()
            continue

        next_wake = min(j.next_run for j in JOBS)
        sleep_for = min((next_wake - now).total_seconds(), (CUTOFF - now).total_seconds())
        sleep_for = max(sleep_for, 1.0)
        time.sleep(sleep_for)


if __name__ == "__main__":
    sys.exit(main())
