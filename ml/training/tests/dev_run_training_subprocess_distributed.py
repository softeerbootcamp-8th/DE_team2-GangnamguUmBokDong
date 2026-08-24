"""`_run_training_subprocess()`가 `LGB_NUM_MACHINES`에 따라 로컬 subprocess와
YARN distributed-shell 제출 중 어느 쪽으로 분기하는지 검증한다(ADR-0007).

실제 `yarn` CLI나 학습 subprocess를 띄우지 않는다 — `subprocess.run()` 자체를
monkeypatch해서 호출 인자만 캡처하고, `train_target()`이 archive에 남기는
`metrics.json`은 미리 S3에 심어둔다(`_run_training_subprocess()`가 학습 직후 이걸
다시 읽어오므로).
"""

from core import s3 as s3_io
from ml_core.paths import archive_models_prefix, model_json_key

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


def test_run_training_subprocess_launches_yarn_distributed_shell_when_num_machines_over_one(monkeypatch):
    _seed_metrics("return")
    captured = {}
    monkeypatch.setattr(
        "training.scripts.monthly_retrain_check.subprocess.run",
        lambda cmd, **kwargs: captured.update(cmd=cmd, kwargs=kwargs),
    )

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
