"""장시간 실행 resource manifest가 성공·실패 증거를 보존하는지 검증한다."""

import json
import sys
from pathlib import Path

from ops import resource_probe


def test_profiled_command_records_process_tree_and_filesystem(tmp_path: Path):
    """성공 명령의 RSS·system·filesystem baseline과 종료 상태를 기록한다."""
    manifest_path = tmp_path / "success.json"

    return_code = resource_probe.run_profiled_command(
        command=[
            sys.executable,
            "-c",
            "import time; payload=bytearray(8*1024*1024); time.sleep(0.15)",
        ],
        manifest_path=manifest_path,
        label="test-success",
        sample_seconds=0.02,
        filesystem_paths=(str(tmp_path),),
        metadata={"commit_sha": "abc123"},
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert return_code == 0
    assert manifest["schema_version"] == "resource-profile-v2"
    assert manifest["status"] == "succeeded"
    assert manifest["exit_code"] == 0
    assert manifest["sample_count"] >= 2
    assert manifest["peak_process_tree_rss_bytes"] >= 8 * 1024 * 1024
    assert manifest["peak_process_tree_rss_plus_swap_bytes"] >= manifest["peak_process_tree_rss_bytes"]
    assert manifest["peak_process_tree_pss_plus_swap_pss_bytes"] > 0
    assert manifest["peak_process_tree_logical_memory_sample"]["observed_at"]
    assert manifest["peak_system_memory_used_bytes"] > 0
    assert manifest["peak_system_memory_plus_swap_used_bytes"] > 0
    assert manifest["peak_system_logical_memory_sample"]["observed_at"]
    assert set(manifest["memory_pressure_total_stall_delta_us"]) == {"some", "full"}
    if manifest["cgroup_memory_at_start"]:
        assert manifest["peak_cgroup_memory_plus_swap_bytes"] > 0
        assert manifest["peak_cgroup_logical_memory_sample"]["observed_at"]
    assert manifest["metadata"] == {"commit_sha": "abc123"}
    filesystem = manifest["filesystems"][str(tmp_path)]
    assert filesystem["start_available_bytes"] > 0
    assert filesystem["end_available_bytes"] > 0
    assert filesystem["peak_consumed_bytes"] >= 0


def test_profiled_command_preserves_nonzero_exit(tmp_path: Path):
    """대상 명령 실패를 final manifest와 wrapper 반환 코드에 그대로 남긴다."""
    manifest_path = tmp_path / "failed.json"

    return_code = resource_probe.run_profiled_command(
        command=[sys.executable, "-c", "raise SystemExit(7)"],
        manifest_path=manifest_path,
        label="test-failure",
        sample_seconds=0.02,
        filesystem_paths=(),
        metadata={},
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert return_code == 7
    assert manifest["status"] == "failed"
    assert manifest["exit_code"] == 7
    assert manifest["ended_at"] is not None
    assert manifest["wall_time_seconds"] >= 0


def test_metadata_rejects_duplicates_and_missing_separator():
    """감사 metadata의 모호한 key와 잘못된 값을 거부한다."""
    assert resource_probe._metadata(["a=1", "b=two=parts"]) == {
        "a": "1",
        "b": "two=parts",
    }
    for values in (["missing"], ["a=1", "a=2"], [" a=1"]):
        try:
            resource_probe._metadata(values)
        except ValueError:
            pass
        else:
            raise AssertionError(f"잘못된 metadata가 허용됐습니다: {values}")


def test_memory_guard_terminates_only_profiled_process_group(
    tmp_path: Path, monkeypatch
):
    """가용 RAM 하한을 넘으면 resource_limit manifest와 전용 종료 코드를 남긴다."""
    manifest_path = tmp_path / "guard.json"
    gib = 1024**3
    monkeypatch.setattr(
        resource_probe,
        "_system_memory",
        lambda: {
            "memory_available_bytes": gib,
            "memory_total_bytes": 10 * gib,
            "memory_used_bytes": 9 * gib,
            "swap_free_bytes": 8 * gib,
            "swap_total_bytes": 8 * gib,
            "swap_used_bytes": 0,
        },
    )

    return_code = resource_probe.run_profiled_command(
        command=[sys.executable, "-c", "import time; time.sleep(30)"],
        manifest_path=manifest_path,
        label="test-memory-guard",
        sample_seconds=0.02,
        filesystem_paths=(),
        metadata={},
        minimum_system_memory_available_bytes=2 * gib,
    )

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert return_code == 75
    assert manifest["status"] == "resource_limit"
    assert manifest["memory_guard"]["triggered"] is True
    assert manifest["memory_guard"]["triggered_available_bytes"] == gib
    assert manifest["termination_signal"] == 15
