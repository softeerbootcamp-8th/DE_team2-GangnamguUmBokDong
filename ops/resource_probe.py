"""장시간 명령의 process-tree·system·filesystem 자원 증거를 JSON으로 보존한다.

학습 프로세스 내부 ``ru_maxrss``만으로는 Spark JVM 같은 자식 프로세스, system
memory pressure, swap과 scratch filesystem 감소를 알 수 없다. 이 wrapper는 대상
명령을 별도 process group으로 실행하고 주기적으로 관측한 누적 peak를 manifest에
원자적으로 덮어쓴다. 대상이 SIGKILL/OOM으로 끝나도 wrapper가 살아 있으면 종료
코드와 마지막 관측값을 final manifest로 남기며, wrapper까지 죽어도 마지막 periodic
manifest는 디스크에 남는다.
"""

from __future__ import annotations

import argparse
import json
import os
import platform
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

_SCHEMA_VERSION = "resource-profile-v2"


def _utc_now() -> str:
    """현재 UTC 시각을 초 단위 ISO 8601 문자열로 반환한다."""
    return datetime.now(UTC).isoformat(timespec="seconds")


def _read_kib_fields(path: Path, names: tuple[str, ...]) -> dict[str, int]:
    """Linux key-value 파일에서 지정한 KiB 필드를 bytes로 읽는다."""
    wanted = set(names)
    values: dict[str, int] = {}
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError, ProcessLookupError):
        return values
    for line in lines:
        key, separator, remainder = line.partition(":")
        if not separator or key not in wanted:
            continue
        parts = remainder.split()
        if not parts:
            continue
        multiplier = 1024 if len(parts) == 1 or parts[1] == "kB" else 1
        values[key] = int(parts[0]) * multiplier
    return values


def _system_memory() -> dict[str, int]:
    """Linux 전체 memory와 swap의 현재 사용량·총량을 반환한다."""
    values = _read_kib_fields(
        Path("/proc/meminfo"),
        ("MemTotal", "MemAvailable", "SwapTotal", "SwapFree"),
    )
    required = {"MemTotal", "MemAvailable", "SwapTotal", "SwapFree"}
    if set(values) != required:
        raise RuntimeError(f"/proc/meminfo 필드가 부족합니다: {sorted(required - set(values))}")
    return {
        "memory_available_bytes": values["MemAvailable"],
        "memory_total_bytes": values["MemTotal"],
        "memory_used_bytes": values["MemTotal"] - values["MemAvailable"],
        "swap_free_bytes": values["SwapFree"],
        "swap_total_bytes": values["SwapTotal"],
        "swap_used_bytes": values["SwapTotal"] - values["SwapFree"],
    }


def _memory_pressure() -> dict[str, float | int]:
    """Linux memory PSI의 현재 평균과 누적 stall 시간을 반환한다."""
    result: dict[str, float | int] = {}
    try:
        lines = Path("/proc/pressure/memory").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        return result
    for line in lines:
        parts = line.split()
        if not parts or parts[0] not in {"some", "full"}:
            continue
        prefix = parts[0]
        for token in parts[1:]:
            key, separator, raw = token.partition("=")
            if not separator:
                continue
            result[f"{prefix}_{key}"] = int(raw) if key == "total" else float(raw)
    return result


def _read_scalar(path: Path) -> int | str | None:
    """cgroup scalar 파일을 정수 또는 ``max`` 문자열로 읽는다."""
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except (FileNotFoundError, PermissionError):
        return None
    if raw == "max":
        return raw
    try:
        return int(raw)
    except ValueError:
        return None


def _cgroup_memory() -> dict[str, int | str]:
    """현재 cgroup v2의 RAM·swap 사용량과 kernel peak/limit을 반환한다."""
    root = Path("/sys/fs/cgroup")
    names = {
        "memory_current_bytes": "memory.current",
        "memory_peak_bytes": "memory.peak",
        "swap_current_bytes": "memory.swap.current",
        "swap_peak_bytes": "memory.swap.peak",
        "memory_max_bytes": "memory.max",
        "swap_max_bytes": "memory.swap.max",
    }
    result: dict[str, int | str] = {}
    for key, filename in names.items():
        value = _read_scalar(root / filename)
        if value is not None:
            result[key] = value
    try:
        event_lines = (root / "memory.events").read_text(encoding="utf-8").splitlines()
    except (FileNotFoundError, PermissionError):
        event_lines = []
    for line in event_lines:
        key, separator, raw = line.partition(" ")
        if separator and raw.isdigit():
            result[f"event_{key}"] = int(raw)
    return result


def _process_parent_map() -> dict[int, int]:
    """현재 ``/proc`` snapshot의 process PID→PPID mapping을 반환한다."""
    parents: dict[int, int] = {}
    for entry in Path("/proc").iterdir():
        if not entry.name.isdigit():
            continue
        try:
            raw = (entry / "stat").read_text(encoding="utf-8")
        except (FileNotFoundError, PermissionError, ProcessLookupError):
            continue
        closing = raw.rfind(")")
        if closing < 0:
            continue
        fields = raw[closing + 1 :].split()
        if len(fields) < 2:
            continue
        try:
            parents[int(entry.name)] = int(fields[1])
        except ValueError:
            continue
    return parents


def _process_tree_pids(root_pid: int) -> set[int]:
    """한 snapshot에서 root PID와 모든 재귀 자식 PID를 반환한다."""
    parents = _process_parent_map()
    tree = {root_pid}
    changed = True
    while changed:
        changed = False
        for pid, parent in parents.items():
            if parent in tree and pid not in tree:
                tree.add(pid)
                changed = True
    return tree


def _process_tree_memory(root_pid: int) -> dict[str, int]:
    """대상 process tree의 현재 RSS/PSS와 각 swap 사용량을 합산한다.

    RSS는 공유 페이지를 process마다 중복 계산하므로 실제 전용 footprint 판단에는
    ``smaps_rollup``의 PSS/SwapPss를 우선 사용한다. 일부 커널이나 권한 환경에서
    PSS를 읽을 수 없으면 RSS/Swap 합계를 fallback logical footprint로 사용한다.
    """
    rss = 0
    swap = 0
    pss = 0
    swap_pss = 0
    pss_observed = 0
    observed = 0
    for pid in _process_tree_pids(root_pid):
        values = _read_kib_fields(Path(f"/proc/{pid}/status"), ("VmRSS", "VmSwap"))
        if not values:
            continue
        observed += 1
        rss += values.get("VmRSS", 0)
        swap += values.get("VmSwap", 0)
        proportional = _read_kib_fields(Path(f"/proc/{pid}/smaps_rollup"), ("Pss", "SwapPss"))
        if "Pss" in proportional:
            pss_observed += 1
            pss += proportional["Pss"]
            swap_pss += proportional.get("SwapPss", 0)
    pss_plus_swap = pss + swap_pss if pss_observed else rss + swap
    return {
        "process_count": observed,
        "rss_bytes": rss,
        "swap_bytes": swap,
        "rss_plus_swap_bytes": rss + swap,
        "pss_bytes": pss if pss_observed else 0,
        "swap_pss_bytes": swap_pss if pss_observed else 0,
        "pss_plus_swap_pss_bytes": pss_plus_swap,
        "pss_process_count": pss_observed,
    }


def _filesystem_snapshot(paths: tuple[str, ...]) -> dict[str, dict[str, int | str]]:
    """지정 경로들이 속한 filesystem의 총량과 현재 available bytes를 반환한다."""
    snapshots: dict[str, dict[str, int | str]] = {}
    for raw_path in paths:
        path = Path(raw_path).resolve()
        stats = os.statvfs(path)
        snapshots[raw_path] = {
            "resolved_path": str(path),
            "total_bytes": stats.f_blocks * stats.f_frsize,
            "available_bytes": stats.f_bavail * stats.f_frsize,
        }
    return snapshots


def _write_manifest(path: Path, manifest: dict) -> None:
    """JSON manifest를 같은 디렉터리 임시 파일에서 원자 교체한다."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp-{os.getpid()}")
    temporary.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    os.replace(temporary, path)


def _initial_manifest(
    *,
    label: str,
    command: list[str],
    metadata: dict[str, str],
    filesystem_paths: tuple[str, ...],
    minimum_system_memory_available_bytes: int,
) -> dict:
    """명령 시작 전 system/filesystem baseline을 가진 manifest를 만든다."""
    memory = _system_memory()
    pressure = _memory_pressure()
    cgroup = _cgroup_memory()
    filesystems = _filesystem_snapshot(filesystem_paths)
    return {
        "command": command,
        "ended_at": None,
        "exit_code": None,
        "filesystems": {
            name: {
                **snapshot,
                "end_available_bytes": None,
                "minimum_available_bytes": snapshot["available_bytes"],
                "peak_consumed_bytes": 0,
                "start_available_bytes": snapshot["available_bytes"],
            }
            for name, snapshot in filesystems.items()
        },
        "host": {
            "logical_cpu_count": os.cpu_count(),
            "machine": platform.machine(),
            "node": platform.node(),
            "platform": platform.platform(),
        },
        "label": label,
        "metadata": metadata,
        "memory_guard": {
            "minimum_available_bytes": (
                minimum_system_memory_available_bytes or None
            ),
            "triggered": False,
            "triggered_at": None,
            "triggered_available_bytes": None,
        },
        "peak_process_tree_process_count": 0,
        "peak_process_tree_rss_bytes": 0,
        "peak_process_tree_swap_bytes": 0,
        "peak_process_tree_rss_plus_swap_bytes": 0,
        "peak_process_tree_pss_bytes": 0,
        "peak_process_tree_swap_pss_bytes": 0,
        "peak_process_tree_pss_plus_swap_pss_bytes": 0,
        "peak_process_tree_logical_memory_sample": None,
        "peak_system_memory_used_bytes": memory["memory_used_bytes"],
        "peak_system_swap_used_bytes": memory["swap_used_bytes"],
        "peak_system_memory_plus_swap_used_bytes": (
            memory["memory_used_bytes"] + memory["swap_used_bytes"]
        ),
        "peak_system_logical_memory_sample": {
            "observed_at": _utc_now(),
            **memory,
        },
        "memory_pressure_at_start": pressure,
        "memory_pressure_end": None,
        "memory_pressure_peak_avg10": {
            "some": float(pressure.get("some_avg10", 0.0)),
            "full": float(pressure.get("full_avg10", 0.0)),
        },
        "memory_pressure_total_stall_delta_us": {"some": 0, "full": 0},
        "cgroup_memory_at_start": cgroup,
        "cgroup_memory_end": None,
        "peak_cgroup_memory_plus_swap_bytes": 0,
        "peak_cgroup_logical_memory_sample": None,
        "sample_count": 0,
        "schema_version": _SCHEMA_VERSION,
        "started_at": _utc_now(),
        "status": "starting",
        "system_memory_total_bytes": memory["memory_total_bytes"],
        "system_memory_available_bytes_at_start": memory[
            "memory_available_bytes"
        ],
        "system_memory_used_bytes_at_start": memory["memory_used_bytes"],
        "system_swap_used_bytes_at_start": memory["swap_used_bytes"],
        "system_swap_total_bytes": memory["swap_total_bytes"],
        "termination_signal": None,
        "wall_time_seconds": None,
    }


def _sample_manifest(
    manifest: dict,
    *,
    root_pid: int,
    filesystem_paths: tuple[str, ...],
) -> dict[str, dict]:
    """현재 process/system/filesystem 값을 manifest에 반영하고 동시 snapshot을 반환한다."""
    process = _process_tree_memory(root_pid)
    memory = _system_memory()
    pressure = _memory_pressure()
    cgroup = _cgroup_memory()
    observed_at = _utc_now()
    manifest["sample_count"] += 1
    manifest["peak_process_tree_process_count"] = max(
        manifest["peak_process_tree_process_count"], process["process_count"]
    )
    manifest["peak_process_tree_rss_bytes"] = max(
        manifest["peak_process_tree_rss_bytes"], process["rss_bytes"]
    )
    manifest["peak_process_tree_swap_bytes"] = max(
        manifest["peak_process_tree_swap_bytes"], process["swap_bytes"]
    )
    manifest["peak_process_tree_rss_plus_swap_bytes"] = max(
        manifest["peak_process_tree_rss_plus_swap_bytes"], process["rss_plus_swap_bytes"]
    )
    manifest["peak_process_tree_pss_bytes"] = max(
        manifest["peak_process_tree_pss_bytes"], process["pss_bytes"]
    )
    manifest["peak_process_tree_swap_pss_bytes"] = max(
        manifest["peak_process_tree_swap_pss_bytes"], process["swap_pss_bytes"]
    )
    if process["pss_plus_swap_pss_bytes"] >= manifest["peak_process_tree_pss_plus_swap_pss_bytes"]:
        manifest["peak_process_tree_pss_plus_swap_pss_bytes"] = process["pss_plus_swap_pss_bytes"]
        manifest["peak_process_tree_logical_memory_sample"] = {
            "observed_at": observed_at,
            **process,
        }
    manifest["peak_system_memory_used_bytes"] = max(
        manifest["peak_system_memory_used_bytes"], memory["memory_used_bytes"]
    )
    manifest["peak_system_swap_used_bytes"] = max(
        manifest["peak_system_swap_used_bytes"], memory["swap_used_bytes"]
    )
    combined_system = memory["memory_used_bytes"] + memory["swap_used_bytes"]
    if combined_system >= manifest["peak_system_memory_plus_swap_used_bytes"]:
        manifest["peak_system_memory_plus_swap_used_bytes"] = combined_system
        manifest["peak_system_logical_memory_sample"] = {
            "observed_at": observed_at,
            **memory,
        }
    manifest["memory_pressure_peak_avg10"]["some"] = max(
        manifest["memory_pressure_peak_avg10"]["some"],
        float(pressure.get("some_avg10", 0.0)),
    )
    manifest["memory_pressure_peak_avg10"]["full"] = max(
        manifest["memory_pressure_peak_avg10"]["full"],
        float(pressure.get("full_avg10", 0.0)),
    )
    for category in ("some", "full"):
        start_total = int(manifest["memory_pressure_at_start"].get(f"{category}_total", 0))
        current_total = int(pressure.get(f"{category}_total", start_total))
        manifest["memory_pressure_total_stall_delta_us"][category] = max(
            0, current_total - start_total
        )
    cgroup_memory = cgroup.get("memory_current_bytes")
    cgroup_swap = cgroup.get("swap_current_bytes")
    if isinstance(cgroup_memory, int) and isinstance(cgroup_swap, int):
        combined_cgroup = cgroup_memory + cgroup_swap
        if combined_cgroup >= manifest["peak_cgroup_memory_plus_swap_bytes"]:
            manifest["peak_cgroup_memory_plus_swap_bytes"] = combined_cgroup
            manifest["peak_cgroup_logical_memory_sample"] = {
                "observed_at": observed_at,
                **cgroup,
            }
    current_filesystems = _filesystem_snapshot(filesystem_paths)
    for name, snapshot in current_filesystems.items():
        recorded = manifest["filesystems"][name]
        available = snapshot["available_bytes"]
        recorded["minimum_available_bytes"] = min(
            recorded["minimum_available_bytes"], available
        )
        recorded["peak_consumed_bytes"] = max(
            0,
            recorded["start_available_bytes"] - recorded["minimum_available_bytes"],
        )
    return {"process": process, "memory": memory, "pressure": pressure, "cgroup": cgroup}


def run_profiled_command(
    *,
    command: list[str],
    manifest_path: Path,
    label: str,
    sample_seconds: float,
    filesystem_paths: tuple[str, ...],
    metadata: dict[str, str],
    minimum_system_memory_available_bytes: int = 0,
) -> int:
    """명령을 실행하며 periodic resource manifest를 남기고 같은 종료 코드를 반환한다."""
    if not command:
        raise ValueError("실행할 command가 필요합니다.")
    if sample_seconds <= 0:
        raise ValueError("sample_seconds는 양수여야 합니다.")
    if minimum_system_memory_available_bytes < 0:
        raise ValueError("minimum system memory available bytes는 0 이상이어야 합니다.")
    manifest = _initial_manifest(
        label=label,
        command=command,
        metadata=metadata,
        filesystem_paths=filesystem_paths,
        minimum_system_memory_available_bytes=(
            minimum_system_memory_available_bytes
        ),
    )
    _write_manifest(manifest_path, manifest)
    started = time.monotonic()
    process = subprocess.Popen(command, start_new_session=True)
    manifest["pid"] = process.pid
    manifest["status"] = "running"

    interrupted_signal: int | None = None
    memory_guard_triggered = False
    try:
        while process.poll() is None:
            sample = _sample_manifest(
                manifest,
                root_pid=process.pid,
                filesystem_paths=filesystem_paths,
            )
            manifest["wall_time_seconds"] = time.monotonic() - started
            available = sample["memory"]["memory_available_bytes"]
            if (
                minimum_system_memory_available_bytes
                and available < minimum_system_memory_available_bytes
            ):
                memory_guard_triggered = True
                manifest["memory_guard"].update(
                    {
                        "triggered": True,
                        "triggered_at": _utc_now(),
                        "triggered_available_bytes": available,
                    }
                )
                manifest["status"] = "terminating_resource_guard"
                _write_manifest(manifest_path, manifest)
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait()
                break
            _write_manifest(manifest_path, manifest)
            try:
                process.wait(timeout=sample_seconds)
            except subprocess.TimeoutExpired:
                pass
    except KeyboardInterrupt:
        interrupted_signal = signal.SIGINT
        os.killpg(process.pid, signal.SIGINT)
        try:
            process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()

    return_code = process.wait()
    final_sample = _sample_manifest(
        manifest,
        root_pid=process.pid,
        filesystem_paths=filesystem_paths,
    )
    manifest["memory_pressure_end"] = final_sample["pressure"]
    manifest["cgroup_memory_end"] = final_sample["cgroup"]
    ended_filesystems = _filesystem_snapshot(filesystem_paths)
    for name, snapshot in ended_filesystems.items():
        manifest["filesystems"][name]["end_available_bytes"] = snapshot[
            "available_bytes"
        ]
    manifest["ended_at"] = _utc_now()
    manifest["exit_code"] = return_code
    manifest["termination_signal"] = (
        interrupted_signal
        if interrupted_signal is not None
        else -return_code
        if return_code < 0
        else None
    )
    if memory_guard_triggered:
        manifest["status"] = "resource_limit"
    elif return_code == 0:
        manifest["status"] = "succeeded"
    elif return_code < 0 or interrupted_signal is not None:
        manifest["status"] = "signaled"
    else:
        manifest["status"] = "failed"
    manifest["wall_time_seconds"] = time.monotonic() - started
    _write_manifest(manifest_path, manifest)
    if memory_guard_triggered:
        return 75
    return 130 if interrupted_signal is not None else return_code


def _metadata(values: list[str]) -> dict[str, str]:
    """반복 ``KEY=VALUE`` CLI 값을 중복 없는 metadata mapping으로 파싱한다."""
    result: dict[str, str] = {}
    for value in values:
        key, separator, item = value.partition("=")
        if not separator or not key or key != key.strip() or key in result:
            raise ValueError(f"metadata는 중복 없는 KEY=VALUE여야 합니다: {value!r}")
        result[key] = item
    return result


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Resource probe 옵션과 ``--`` 뒤 대상 명령을 파싱한다."""
    parser = argparse.ArgumentParser(description="명령의 자원 peak와 종료 상태를 JSON으로 기록합니다.")
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--label", required=True)
    parser.add_argument("--sample-seconds", type=float, default=5.0)
    parser.add_argument(
        "--min-system-memory-available-gib",
        type=float,
        default=0.0,
        help="가용 system memory가 이 GiB보다 작아지면 대상 process group만 종료한다",
    )
    parser.add_argument("--filesystem-path", action="append", default=[])
    parser.add_argument("--metadata", action="append", default=[])
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args(argv)
    if args.command and args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("-- 뒤에 실행할 command가 필요합니다.")
    return args


def main(argv: list[str] | None = None) -> int:
    """CLI 명령을 프로파일링하고 대상 프로세스와 같은 성공·실패 코드를 반환한다."""
    args = _parse_args(argv)
    try:
        metadata = _metadata(args.metadata)
        return run_profiled_command(
            command=args.command,
            manifest_path=args.manifest,
            label=args.label,
            sample_seconds=args.sample_seconds,
            filesystem_paths=tuple(args.filesystem_path),
            metadata=metadata,
            minimum_system_memory_available_bytes=int(
                args.min_system_memory_available_gib * 1024**3
            ),
        )
    except (OSError, RuntimeError, ValueError) as exc:
        print(f"[resource-probe] ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
