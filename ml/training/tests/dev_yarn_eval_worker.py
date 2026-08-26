"""yarn_eval_worker.py의 barrier(S3 자기등록 + 폴링) 로직과 main() 엔드투엔드를
검증한다 — 스타일은 dev_yarn_worker_bootstrap.py를 그대로 따른다.

실제 YARN/EC2 IMDS/S3 feature mart 없이도 검증 가능한 부분만 다룬다 —
`_discover_private_ip()`(IMDS HTTP 호출)와 `evaluate_recent_performance_shard()`
(실제 predict/S3 읽기)는 monkeypatch로 대체하고, S3 등록/폴링/타임아웃/rank
계산과 main()이 계산한 shard_range를 올바른 워커에게 전달하는지만 검증한다.
"""

import pytest
from core import s3 as s3_io

from training.scripts.yarn_eval_worker import (
    _poll_until_all_registered,
    _register_self,
    _resolve_rank,
    main,
)


def test_register_self_writes_barrier_file():
    _register_self("run-1", "rental", "worker-a")

    assert s3_io.read_json("models/training-runs/run-1/eval-barrier/rental/worker-a.json") == {
        "worker_id": "worker-a"
    }


def test_poll_until_all_registered_returns_once_count_reached():
    _register_self("run-2", "rental", "worker-a")
    _register_self("run-2", "rental", "worker-b")

    keys = _poll_until_all_registered("run-2", "rental", num_workers=2, timeout_seconds=5)

    assert len(keys) == 2


def test_poll_until_all_registered_times_out_when_not_enough_workers():
    _register_self("run-3", "rental", "worker-a")

    with pytest.raises(TimeoutError, match="1/2"):
        _poll_until_all_registered("run-3", "rental", num_workers=2, timeout_seconds=0.01)


def test_poll_until_all_registered_raises_when_more_than_expected():
    _register_self("run-9", "rental", "worker-a")
    _register_self("run-9", "rental", "worker-b")
    _register_self("run-9", "rental", "worker-a-retry")

    with pytest.raises(RuntimeError, match="예상보다 많은 등록"):
        _poll_until_all_registered("run-9", "rental", num_workers=2, timeout_seconds=5)


def test_poll_is_scoped_by_model_name():
    """같은 run_id라도 model이 다르면 barrier 네임스페이스가 섞이지 않는다 —
    대여/반납을 같은 run_id로 동시에 평가해도 서로의 등록을 보지 않는다."""
    _register_self("run-shared", "rental", "worker-a")
    _register_self("run-shared", "return", "worker-a")
    _register_self("run-shared", "return", "worker-b")

    rental_keys = _poll_until_all_registered("run-shared", "rental", num_workers=1, timeout_seconds=5)
    return_keys = _poll_until_all_registered("run-shared", "return", num_workers=2, timeout_seconds=5)

    assert len(rental_keys) == 1
    assert len(return_keys) == 2


def test_resolve_rank_is_consistent_and_sorted_by_worker_id():
    _register_self("run-4", "rental", "worker-b")
    _register_self("run-4", "rental", "worker-a")
    keys = s3_io.list_keys("models/training-runs/run-4/eval-barrier/rental/")

    assert _resolve_rank(keys, "worker-a") == 0
    assert _resolve_rank(keys, "worker-b") == 1


def test_resolve_rank_raises_when_self_not_in_registrations():
    _register_self("run-6", "rental", "worker-a")
    keys = s3_io.list_keys("models/training-runs/run-6/eval-barrier/rental/")

    with pytest.raises(RuntimeError, match="자기 자신을 찾을 수 없음"):
        _resolve_rank(keys, "worker-nope")


def test_main_end_to_end_resolves_rank_and_writes_shard(monkeypatch):
    """barrier가 이미 다 채워진 상태에서 main()이 자기 rank에 해당하는
    날짜 조각만 evaluate_recent_performance_shard()에 넘기고, 그 결과를 자기
    worker_id 키로 S3에 쓰는지 확인한다(IMDS/실제 평가는 monkeypatch로 대체)."""
    monkeypatch.setenv("EVAL_RUN_ID", "run-e2e")
    monkeypatch.setenv("EVAL_MODEL", "rental")
    monkeypatch.setenv("EVAL_TARGET_COL", "rental_count")
    monkeypatch.delenv("EVAL_EXPOSURE_COL", raising=False)
    monkeypatch.setenv("EVAL_HORIZON", "1")
    monkeypatch.setenv("EVAL_WINDOW_START", "2026-08-01")
    monkeypatch.setenv("EVAL_WINDOW_END", "2026-08-02")
    monkeypatch.setenv("EVAL_NUM_WORKERS", "2")
    monkeypatch.setenv("CONTAINER_ID", "container-self")
    monkeypatch.setattr("training.scripts.yarn_eval_worker._discover_private_ip", lambda: "10.0.0.9")
    # 다른 워커는 이미 등록해둔 상태로 시작 — 이 프로세스가 등록하는 순간 2/2가 된다.
    _register_self("run-e2e", "rental", "container-other")

    captured = {}

    def _fake_shard(model_name, target_col, exposure_col, date_range, horizon):
        captured.update(
            model_name=model_name, target_col=target_col, exposure_col=exposure_col,
            date_range=date_range, horizon=horizon,
        )
        return {"n_rows": 3, "sum_deviance_term": 1.5, "sum_sq_err": 2.5, "sum_coverage_hits": 2.0}

    monkeypatch.setattr(
        "training.scripts.yarn_eval_worker.monitor_performance.evaluate_recent_performance_shard", _fake_shard
    )

    returncode = main()

    assert returncode == 0
    assert captured["model_name"] == "rental"
    assert captured["target_col"] == "rental_count"
    assert captured["exposure_col"] is None
    assert captured["horizon"] == 1
    # container-other(rank 0)이 앞 조각을, container-self(rank 1)가 뒷 조각을 맡는다
    # (worker_id 알파벳 정렬: "container-other" < "container-self").
    assert captured["date_range"] == ("2026-08-02", "2026-08-02")

    written = s3_io.read_json("models/training-runs/run-e2e/eval-shards/rental/container-self.json")
    assert written == {"n_rows": 3, "sum_deviance_term": 1.5, "sum_sq_err": 2.5, "sum_coverage_hits": 2.0}
