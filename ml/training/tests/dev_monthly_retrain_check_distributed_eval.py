"""`_run_distributed_evaluation_via_yarn()`/`_check_all_models_distributed()`
(분산 평가 오케스트레이션)을 검증한다. 스타일은
dev_run_training_subprocess_distributed.py를 따른다 — 실제 `yarn` CLI는
monkeypatch로 대체하고, 워커가 S3에 남길 조각 결과는 미리 심어둔다.
"""

import pytest
from core import s3 as s3_io

from training.scripts import monthly_retrain_check as mrc


class _FakeCompletedProcess:
    def __init__(self, stdout: str = "") -> None:
        self.stdout = stdout
        self.returncode = 0


def _fake_run_factory(jar_path: str):
    captured = {}

    def _fake_run(cmd, **kwargs):
        if cmd[0] == "find":
            return _FakeCompletedProcess(stdout=f"{jar_path}\n")
        captured.update(cmd=cmd, kwargs=kwargs)
        return _FakeCompletedProcess()

    return captured, _fake_run


def test_run_distributed_evaluation_submits_yarn_and_combines_shards(monkeypatch):
    captured, fake_run = _fake_run_factory("/usr/lib/hadoop-yarn/distributedshell.jar")
    monkeypatch.setattr(mrc.subprocess, "run", fake_run)
    monkeypatch.setattr(mrc, "unique_archive_date", lambda: "2026-08-26-test")
    monkeypatch.setattr(mrc, "_recent_month_range", lambda months, as_of: ("2026-08-01", "2026-08-05"))
    monkeypatch.setattr(mrc, "_load_baseline_metrics", lambda model_name: {
        "poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.8, "rmse_test": 2.0,
    })

    run_id = "eval-2026-08-26-test-rental"
    for worker_id, n_rows, dev, sq, cov in [("w0", 3, 1.2, 3.0, 2.0), ("w1", 2, 0.8, 1.0, 2.0)]:
        s3_io.write_json(
            f"models/training-runs/{run_id}/eval-shards/rental/{worker_id}.json",
            {"n_rows": n_rows, "sum_deviance_term": dev, "sum_sq_err": sq, "sum_coverage_hits": cov},
        )

    result = mrc._run_distributed_evaluation_via_yarn(
        "rental", "rental_count", "rental_exposure", horizon=1, num_workers=2
    )

    cmd = captured["cmd"]
    assert cmd[0] == "yarn"
    assert "-num_containers" in cmd
    assert cmd[cmd.index("-num_containers") + 1] == "2"
    assert "training.scripts.yarn_eval_worker" in cmd[cmd.index("-shell_command") + 1]
    for expected in [
        "EVAL_RUN_ID=eval-2026-08-26-test-rental",
        "EVAL_MODEL=rental",
        "EVAL_TARGET_COL=rental_count",
        "EVAL_EXPOSURE_COL=rental_exposure",
        "EVAL_HORIZON=1",
        "EVAL_WINDOW_START=2026-08-01",
        "EVAL_WINDOW_END=2026-08-05",
        "EVAL_NUM_WORKERS=2",
    ]:
        assert any(cmd[i] == "-shell_env" and cmd[i + 1] == expected for i in range(len(cmd) - 1)), expected
    assert "-master_memory" in cmd  # OOM 재발 방지 — 학습 쪽과 같은 이유(monthly_retrain_check.py 참고)

    assert result["n_rows"] == 5
    assert result["current_deviance"] == pytest.approx(2 * (1.2 + 0.8) / 5)
    assert result["period"] == {"start": "2026-08-01", "end": "2026-08-05"}


def test_run_distributed_evaluation_raises_when_shard_count_mismatches(monkeypatch):
    _captured, fake_run = _fake_run_factory("/usr/lib/hadoop-yarn/distributedshell.jar")
    monkeypatch.setattr(mrc.subprocess, "run", fake_run)
    monkeypatch.setattr(mrc, "unique_archive_date", lambda: "2026-08-26-mismatch")
    monkeypatch.setattr(mrc, "_recent_month_range", lambda months, as_of: ("2026-08-01", "2026-08-05"))

    # num_workers=3인데 조각은 하나도 안 심어둠 -> 0/3
    with pytest.raises(RuntimeError, match=r"0/3"):
        mrc._run_distributed_evaluation_via_yarn(
            "rental", "rental_count", "rental_exposure", horizon=1, num_workers=3
        )


def test_check_all_models_distributed_delegates_to_check_all_models_when_single_worker(monkeypatch):
    called = {}
    monkeypatch.setattr(mrc, "check_all_models", lambda **kwargs: called.update(kwargs) or ["sentinel"])

    result = mrc._check_all_models_distributed(as_of="2026-08-17", horizon=1, model_names=["rental"], num_workers=1)

    assert result == ["sentinel"]
    # num_workers<=1일 땐 check_all_models()를 예전과 정확히 같은 인자로 부른다
    # (horizon은 그쪽 기본값(1)에 맡긴다) — 기존 CLI 테스트의 mock이 여전히
    # `horizon` 키워드를 안 받는 시그니처를 쓰므로, 여기서 넘기면 깨진다.
    assert called == {"as_of": "2026-08-17", "model_names": ["rental"]}


def test_check_all_models_distributed_runs_distributed_evaluation_per_model(monkeypatch):
    calls = []

    def _fake_distributed_eval(model_name, target_col, exposure_col, horizon, num_workers, as_of=None):
        calls.append(model_name)
        return {
            "model_name": model_name,
            "period": {"start": "2026-08-01", "end": "2026-08-05"},
            "n_rows": 10,
            "baseline_deviance": 1.0,
            "current_deviance": 1.0,
            "deviance_relative_change": 0.0,
            "baseline_rmse": 2.0,
            "current_rmse": 2.0,
            "baseline_coverage": 0.8,
            "current_coverage": 0.8,
            "coverage_drift": 0.0,
        }

    monkeypatch.setattr(mrc, "_run_distributed_evaluation_via_yarn", _fake_distributed_eval)
    logged = []
    monkeypatch.setattr(mrc, "_log_to_mlflow", lambda result, horizon: logged.append(result["model_name"]))

    results = mrc._check_all_models_distributed(as_of=None, horizon=1, model_names=None, num_workers=4)

    assert calls == ["rental", "return"]
    assert logged == ["rental", "return"]
    assert [r["model_name"] for r in results] == ["rental", "return"]
    assert all(r["needs_retrain"] is False for r in results)


def test_check_all_models_distributed_filters_by_model_names(monkeypatch):
    calls = []

    def _fake_distributed_eval(model_name, target_col, exposure_col, horizon, num_workers, as_of=None):
        calls.append(model_name)
        return {
            "model_name": model_name, "period": {"start": "s", "end": "e"}, "n_rows": 1,
            "baseline_deviance": 1.0, "current_deviance": 1.0, "deviance_relative_change": 0.0,
            "baseline_rmse": 1.0, "current_rmse": 1.0, "baseline_coverage": 0.8, "current_coverage": 0.8,
            "coverage_drift": 0.0,
        }

    monkeypatch.setattr(mrc, "_run_distributed_evaluation_via_yarn", _fake_distributed_eval)
    monkeypatch.setattr(mrc, "_log_to_mlflow", lambda result, horizon: None)

    mrc._check_all_models_distributed(as_of=None, horizon=1, model_names=["return"], num_workers=4)

    assert calls == ["return"]
