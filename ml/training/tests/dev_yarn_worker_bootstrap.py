"""yarn_worker_bootstrap.py의 barrier(S3 자기등록 + 폴링) 로직을 검증한다.

실제 YARN/EC2 IMDS 없이도 검증 가능한 부분만 다룬다 — `_discover_private_ip()`(IMDS
HTTP 호출)와 `_launch_training()`(subprocess 실행)은 monkeypatch로 대체하고,
S3 등록/폴링/타임아웃/rank 계산만 moto S3로 실제 검증한다.
"""

import pytest
from core import s3 as s3_io

from training.scripts.yarn_worker_bootstrap import (
    _poll_until_all_registered,
    _register_self,
    _resolve_rank_and_machines,
    main,
)


def test_register_self_writes_barrier_file():
    _register_self("run-1", "worker-a", "10.0.0.1", 12400)

    assert s3_io.read_json("models/training-runs/run-1/workers/worker-a.json") == {
        "host": "10.0.0.1",
        "port": 12400,
    }


def test_poll_until_all_registered_returns_once_count_reached():
    _register_self("run-2", "worker-a", "10.0.0.1", 12400)
    _register_self("run-2", "worker-b", "10.0.0.2", 12400)

    keys = _poll_until_all_registered("run-2", num_machines=2, timeout_seconds=5)

    assert len(keys) == 2


def test_poll_until_all_registered_times_out_when_not_enough_workers():
    _register_self("run-3", "worker-a", "10.0.0.1", 12400)

    with pytest.raises(TimeoutError, match="1/2"):
        _poll_until_all_registered("run-3", num_machines=2, timeout_seconds=0.01)


def test_resolve_rank_and_machines_is_consistent_and_sorted_by_host():
    _register_self("run-4", "worker-a", "10.0.0.2", 12400)
    _register_self("run-4", "worker-b", "10.0.0.1", 12400)
    keys = s3_io.list_keys("models/training-runs/run-4/workers/")

    rank_of_lower_host, machines = _resolve_rank_and_machines(keys, "10.0.0.1", 12400)
    rank_of_higher_host, machines_again = _resolve_rank_and_machines(keys, "10.0.0.2", 12400)

    assert machines == "10.0.0.1:12400,10.0.0.2:12400"
    assert machines_again == machines
    assert rank_of_lower_host == 0
    assert rank_of_higher_host == 1


def test_resolve_rank_and_machines_rejects_duplicate_host():
    _register_self("run-5", "worker-a", "10.0.0.1", 12400)
    _register_self("run-5", "worker-b", "10.0.0.1", 12400)
    keys = s3_io.list_keys("models/training-runs/run-5/workers/")

    with pytest.raises(RuntimeError, match="중복 등록"):
        _resolve_rank_and_machines(keys, "10.0.0.1", 12400)


def test_resolve_rank_and_machines_raises_when_self_not_in_registrations():
    _register_self("run-6", "worker-a", "10.0.0.1", 12400)
    keys = s3_io.list_keys("models/training-runs/run-6/workers/")

    with pytest.raises(RuntimeError, match="자기 자신을 찾을 수 없음"):
        _resolve_rank_and_machines(keys, "10.9.9.9", 12400)


def test_main_requires_num_machines_greater_than_one(monkeypatch):
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap.config.LGB_NUM_MACHINES", 1)

    with pytest.raises(RuntimeError, match="LGB_NUM_MACHINES"):
        main(["--model", "rental"])


def test_main_requires_training_run_id_env(monkeypatch):
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap.config.LGB_NUM_MACHINES", 2)
    monkeypatch.delenv("TRAINING_RUN_ID", raising=False)

    with pytest.raises(RuntimeError, match="TRAINING_RUN_ID"):
        main(["--model", "rental"])


def test_main_end_to_end_resolves_rank_and_launches_training(monkeypatch):
    """barrier가 이미 다 채워진 상태에서 main()이 rank/machines를 계산해 학습
    subprocess에 정확히 넘기는지 확인한다(IMDS/subprocess는 monkeypatch로 대체)."""
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap.config.LGB_NUM_MACHINES", 2)
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap.config.LGB_LOCAL_LISTEN_PORT", 12400)
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap.config.LGB_TIME_OUT", 1)
    monkeypatch.setenv("TRAINING_RUN_ID", "run-e2e")
    monkeypatch.setenv("CONTAINER_ID", "container-self")
    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap._discover_private_ip", lambda: "10.0.0.9")
    # 다른 워커는 이미 등록해둔 상태로 시작 — 이 프로세스가 등록하는 순간 2/2가 된다.
    _register_self("run-e2e", "container-other", "10.0.0.1", 12400)

    captured = {}

    def _fake_launch(model, env):
        captured["model"] = model
        captured["rank"] = env["LGB_MACHINE_RANK"]
        captured["machines"] = env["LGB_MACHINES"]
        return 0

    monkeypatch.setattr("training.scripts.yarn_worker_bootstrap._launch_training", _fake_launch)

    returncode = main(["--model", "rental"])

    assert returncode == 0
    assert captured["model"] == "rental"
    assert captured["machines"] == "10.0.0.1:12400,10.0.0.9:12400"
    assert captured["rank"] == "1"  # 10.0.0.9가 10.0.0.1보다 뒤 -> index 1
