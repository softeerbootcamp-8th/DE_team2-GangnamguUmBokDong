"""`_run_training_subprocess()`가 `LGB_NUM_MACHINES`에 따라 로컬 subprocess와
YARN distributed-shell 제출 중 어느 쪽으로 분기하는지 검증한다(ADR-0007).

실제 `yarn` CLI나 학습 subprocess를 띄우지 않는다 — `subprocess.run()` 자체를
monkeypatch해서 호출 인자만 캡처하고, `train_target()`이 archive에 남기는
`metrics.json`은 미리 S3에 심어둔다(`_run_training_subprocess()`가 학습 직후 이걸
다시 읽어오므로).
"""

import pytest
from core import s3 as s3_io
from ml_core.paths import archive_models_prefix, model_json_key

from training.scripts import monthly_retrain_check as mrc
from training.scripts.monthly_retrain_check import _run_training_subprocess

_ARCHIVE_DATE = "2026-08-24-test"
_PROFILE = "builtin-default"


def _seed_metrics(model_name: str) -> None:
    archive_prefix = archive_models_prefix(_ARCHIVE_DATE, _PROFILE)
    s3_io.write_json(model_json_key(model_name, "metrics", archive_prefix), {"model_name": model_name})


def test_run_training_subprocess_uses_local_subprocess_when_num_machines_is_one(monkeypatch):
    _seed_metrics("rental")
    captured = {}
    monkeypatch.setattr(
        "training.scripts.monthly_retrain_check.subprocess.run",
        lambda cmd, **kwargs: captured.update(cmd=cmd, kwargs=kwargs),
    )

    metrics = _run_training_subprocess("rental", _PROFILE, _ARCHIVE_DATE, {})

    assert metrics == {"model_name": "rental"}
    assert captured["cmd"][1:] == ["-m", "training.train_rental_model"]


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "") -> None:
        self.stdout = stdout
        self.returncode = 0


def test_run_training_subprocess_launches_yarn_distributed_shell_when_num_machines_over_one(monkeypatch):
    """`_resolve_distributed_shell_jar()`가 내부적으로 `find`도 subprocess.run으로
    돌리므로(YARN_DISTRIBUTED_SHELL_JAR 미설정 시), 이 fake는 그 find 호출과 실제
    yarn 제출 호출을 cmd[0]으로 구분해서 각각 다르게 응답해야 한다."""
    _seed_metrics("return")
    captured = {}

    def _fake_run(cmd, **kwargs):
        if cmd[0] == "find":
            return _FakeCompletedProcess(stdout="/usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell.jar\n")
        captured.update(cmd=cmd, kwargs=kwargs)
        return _FakeCompletedProcess()

    monkeypatch.setattr("training.scripts.monthly_retrain_check.subprocess.run", _fake_run)

    metrics = _run_training_subprocess("return", _PROFILE, _ARCHIVE_DATE, {"LGB_NUM_MACHINES": "8"})

    assert metrics == {"model_name": "return"}
    cmd = captured["cmd"]
    assert cmd[0] == "yarn"
    assert "-num_containers" in cmd
    assert cmd[cmd.index("-num_containers") + 1] == "8"
    assert any(
        cmd[i] == "-shell_env" and cmd[i + 1] == "LGB_NUM_MACHINES=8" for i in range(len(cmd) - 1)
    )
    run_id_args = [
        cmd[i + 1] for i in range(len(cmd) - 1) if cmd[i] == "-shell_env" and cmd[i + 1].startswith("TRAINING_RUN_ID=")
    ]
    assert run_id_args == [f"TRAINING_RUN_ID={_ARCHIVE_DATE}-{_PROFILE}-return"]
    shell_command = cmd[cmd.index("-shell_command") + 1]
    assert "training.scripts.yarn_worker_bootstrap --model return" in shell_command
    # distributed-shell 자체 AM에 메모리를 명시 안 하면 기본 100MB로 떠서
    # OutOfMemoryError로 즉시 죽는다(겉보기엔 "JNI error"라 Java 17 비호환처럼
    # 보였지만 실제로는 순수 메모리 부족이었다 — 실제 EMR 실행에서 확인,
    # 2026-08-26).
    assert "-master_memory" in cmd
    assert cmd[cmd.index("-master_memory") + 1] == str(mrc.YARN_AM_MEMORY_MB)
    assert "-master_vcores" in cmd


def test_resolve_distributed_shell_jar_prefers_explicit_env_override(monkeypatch):
    """환경변수가 있으면 find를 아예 안 돌고 그 값을 그대로 쓴다."""
    monkeypatch.setattr(mrc, "YARN_DISTRIBUTED_SHELL_JAR", "/custom/path/distributedshell.jar")
    monkeypatch.setattr(
        mrc.subprocess, "run", lambda *a, **k: (_ for _ in ()).throw(AssertionError("find를 돌면 안 됨"))
    )

    assert mrc._resolve_distributed_shell_jar() == "/custom/path/distributedshell.jar"


def test_resolve_distributed_shell_jar_finds_via_search_roots(monkeypatch):
    """환경변수가 없으면(기본값) 알려진 설치 위치를 find로 뒤져 실제 존재하는
    첫 후보를 쓴다 — EMR 릴리스마다 정확한 파일명이 다를 수 있어 하드코딩한
    경로 하나만 믿지 않기 위함(이 프로젝트가 실제 EMR을 아직 한 번도 안
    켜봐서 정확한 경로를 미리 확인할 수 없었다)."""
    monkeypatch.setattr(mrc, "YARN_DISTRIBUTED_SHELL_JAR", "")
    found_path = "/usr/lib/hadoop-yarn/hadoop-yarn-applications-distributedshell-3.3.6-amzn-1.jar"

    def _fake_run(cmd, **kwargs):
        assert cmd[0] == "find"
        if cmd[1] == "/usr/lib/hadoop-yarn":
            return _FakeCompletedProcess(stdout=f"{found_path}\n")
        return _FakeCompletedProcess(stdout="")

    monkeypatch.setattr(mrc.subprocess, "run", _fake_run)

    assert mrc._resolve_distributed_shell_jar() == found_path


def test_resolve_distributed_shell_jar_raises_when_not_found_anywhere(monkeypatch):
    monkeypatch.setattr(mrc, "YARN_DISTRIBUTED_SHELL_JAR", "")
    monkeypatch.setattr(mrc.subprocess, "run", lambda *a, **k: _FakeCompletedProcess(stdout=""))

    with pytest.raises(RuntimeError, match="distributed-shell jar를 찾을 수 없습니다"):
        mrc._resolve_distributed_shell_jar()
