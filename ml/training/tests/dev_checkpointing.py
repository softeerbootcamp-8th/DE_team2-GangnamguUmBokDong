"""장시간 LightGBM phase checkpoint의 저장·검증·재개 계약을 검증한다."""

from __future__ import annotations

import copy
from types import SimpleNamespace

import lightgbm as lgb
import numpy as np
import pytest
from lightgbm.basic import EvalResult

from training import checkpointing, train_common


@pytest.fixture
def checkpoint_store(monkeypatch):
    """S3 JSON과 Booster object를 메모리 dict로 대체한다."""
    objects: dict[str, object] = {}

    def read_json(key: str):
        """저장된 JSON 객체의 복사본을 반환한다."""
        value = objects.get(key)
        return copy.deepcopy(value) if isinstance(value, dict) else None

    def write_json(key: str, value: dict) -> None:
        """JSON 객체의 복사본을 저장한다."""
        objects[key] = copy.deepcopy(value)

    def stage_and_upload(booster: lgb.Booster, key: str, log_to_mlflow: bool = False) -> None:
        """Booster 문자열을 S3 object 대신 메모리에 저장한다."""
        del log_to_mlflow
        objects[key] = booster.model_to_string()

    def download(key: str) -> lgb.Booster:
        """저장된 Booster 문자열을 다시 로드한다."""
        value = objects[key]
        assert isinstance(value, str)
        return lgb.Booster(model_str=value)

    monkeypatch.setattr(checkpointing.s3_io, "read_json", read_json)
    monkeypatch.setattr(checkpointing.s3_io, "write_json", write_json)
    monkeypatch.setattr(checkpointing.s3_io, "object_exists", lambda key: key in objects)
    monkeypatch.setattr(checkpointing.model_io, "stage_and_upload_booster", stage_and_upload)
    monkeypatch.setattr(checkpointing.model_io, "download_and_load_booster", download)
    return objects


def _manager(
    contract: dict,
    *,
    interval: int = 3,
    compatible_code_fingerprints: frozenset[str] = frozenset(),
) -> checkpointing.TrainingCheckpointManager:
    """테스트용 checkpoint manager를 생성한다."""
    return checkpointing.TrainingCheckpointManager(
        "models/archive/dt=test/profile",
        "rental",
        "poisson",
        contract,
        interval,
        True,
        compatible_code_fingerprints,
    )


def test_round_checkpoint_resumes_to_requested_total_rounds(checkpoint_store):
    """중단 후 마지막 정상 round부터 이어서 총 round 수를 정확히 맞춘다."""
    rng = np.random.default_rng(42)
    features = rng.normal(size=(500, 4))
    labels = 2 * features[:, 0] - features[:, 1] + rng.normal(scale=0.1, size=500)
    params = {
        "objective": "regression",
        "metric": "l2",
        "learning_rate": 0.1,
        "num_leaves": 7,
        "min_data_in_leaf": 5,
        "verbose": -1,
        "num_threads": 1,
        "seed": 17,
    }
    contract = {"dataset": "fixed", "params": params, "rounds": 10}
    first = _manager(contract)

    def interrupt_after_five(env) -> None:
        """첫 실행을 checkpoint 이후 의도적으로 중단한다."""
        if env.model.current_iteration() == 5:
            raise RuntimeError("simulated interruption")

    interrupt_after_five.order = 40
    interrupt_after_five.before_iteration = False
    with pytest.raises(RuntimeError, match="simulated interruption"):
        lgb.train(
            params,
            lgb.Dataset(features, label=labels, free_raw_data=False),
            num_boost_round=10,
            callbacks=[first.callback(), interrupt_after_five],
        )

    state = first.load("models/archive/dt=test/profile/rental_poisson.txt")
    assert state.completed_iterations == 3
    assert state.phase_completed is False
    assert state.booster is not None

    resumed = _manager(contract)
    resume_state = resumed.load("models/archive/dt=test/profile/rental_poisson.txt")
    resumed_booster = lgb.train(
        params,
        lgb.Dataset(features, label=labels, free_raw_data=False),
        num_boost_round=10 - resume_state.completed_iterations,
        init_model=resume_state.booster,
        callbacks=[resumed.callback()],
    )
    assert resumed_booster.current_iteration() == 10

    uninterrupted = lgb.train(
        params,
        lgb.Dataset(features, label=labels),
        num_boost_round=10,
    )
    np.testing.assert_allclose(
        resumed_booster.predict(features),
        uninterrupted.predict(features),
        rtol=1e-12,
        atol=1e-12,
    )

    final_key = "models/archive/dt=test/profile/rental_poisson.txt"
    checkpointing.model_io.stage_and_upload_booster(resumed_booster, final_key)
    resumed.mark_completed(resumed_booster, final_key)
    completed = _manager(contract).load(final_key)
    assert completed.phase_completed is True
    assert completed.completed_iterations == 10
    assert completed.booster is not None
    assert completed.booster.best_iteration == 10


def test_contract_mismatch_rejects_resume(checkpoint_store):
    """데이터나 설정 계약이 바뀐 checkpoint를 자동으로 재사용하지 않는다."""
    manager = _manager({"dataset": "v1"})
    manager._write_state(
        status="in_progress",
        checkpoint_key="models/checkpoint.txt",
        completed_iterations=3,
    )
    with pytest.raises(checkpointing.CheckpointContractMismatchError):
        _manager({"dataset": "v2"}).load("models/final.txt")


def test_explicit_code_compatibility_requires_all_other_contract_fields_to_match(
    checkpoint_store,
):
    """명시한 이전 코드라도 데이터·파라미터 계약 변화는 재개하지 않는다."""
    old_contract = {
        "dataset": "fixed",
        "params": {"num_leaves": 7},
        "filters": [["horizon", "in", [1, 2]]],
        "code_fingerprint": "old-code",
    }
    old_manager = _manager(old_contract)
    old_manager._write_state(status="in_progress", completed_iterations=0)

    compatible_contract = {
        **old_contract,
        "filters": [("horizon", "in", [1, 2])],
        "code_fingerprint": "resume-fix",
    }
    state = _manager(
        compatible_contract,
        compatible_code_fingerprints=frozenset({"old-code"}),
    ).load("models/final.txt")
    assert state.completed_iterations == 0

    changed_data_contract = {
        **compatible_contract,
        "params": {"num_leaves": 15},
    }
    with pytest.raises(checkpointing.CheckpointContractMismatchError):
        _manager(
            changed_data_contract,
            compatible_code_fingerprints=frozenset({"old-code"}),
        ).load("models/final.txt")


def test_resume_aware_early_stopping_preserves_best_score_and_patience():
    """중단 전 최고 round와 patience가 재개 후에도 동일한 종료점을 만든다."""
    scores = [10.0, 9.0, 8.0, 8.5, 8.6]

    def env(iteration: int, score: float):
        """LightGBM callback에 필요한 최소 환경을 만든다."""
        return SimpleNamespace(
            iteration=iteration,
            end_iteration=20,
            evaluation_result_list=[("valid_0", "l2", score, False)],
        )

    uninterrupted = checkpointing.ResumeAwareEarlyStopping(2)
    with pytest.raises(lgb.callback.EarlyStopException) as uninterrupted_stop:
        for iteration, score in enumerate(scores):
            uninterrupted(env(iteration, score))

    before_interrupt = checkpointing.ResumeAwareEarlyStopping(2)
    for iteration, score in enumerate(scores[:3]):
        before_interrupt(env(iteration, score))
    state = before_interrupt.snapshot()
    assert state is not None
    assert state["best_iteration"] == 2
    assert state["best_score"] == 8.0

    resumed = checkpointing.ResumeAwareEarlyStopping(2, state)
    with pytest.raises(lgb.callback.EarlyStopException) as resumed_stop:
        for iteration, score in enumerate(scores[3:], start=3):
            resumed(env(iteration, score))

    assert uninterrupted_stop.value.best_iteration == resumed_stop.value.best_iteration == 2


def test_resume_aware_early_stopping_supports_lightgbm_47_eval_result():
    """LightGBM 4.7 EvalResult의 동적 길이와 무관하게 저장 점수를 복원한다."""
    item = EvalResult("valid_0", "l2", 8.5, False, None)
    callback = checkpointing.ResumeAwareEarlyStopping(
        5,
        {
            "dataset_name": "valid_0",
            "metric_name": "l2",
            "higher_is_better": False,
            "best_iteration": 3,
            "best_score": 7.25,
        },
    )
    env = SimpleNamespace(
        iteration=4,
        end_iteration=20,
        evaluation_result_list=[item],
    )

    callback(env)

    restored = callback.best_score_list[0]
    assert restored.dataset_name == "valid_0"
    assert restored.metric_name == "l2"
    assert restored.metric_value == 7.25
    assert restored.maximize is False
    assert restored.metric_std_dev is None


def test_state_pointer_updates_only_after_booster_upload(monkeypatch):
    """Booster 업로드 실패 시 이전 state 포인터를 변경하지 않는다."""
    class FakeBooster:
        """현재 round만 제공하는 최소 Booster 대역이다."""

        def current_iteration(self) -> int:
            """저장 가능한 양수 round를 반환한다."""
            return 3

    writes: list[dict] = []
    monkeypatch.setattr(checkpointing.s3_io, "write_json", lambda key, value: writes.append(value))
    monkeypatch.setattr(
        checkpointing.model_io,
        "stage_and_upload_booster",
        lambda *args, **kwargs: (_ for _ in ()).throw(OSError("upload failed")),
    )
    booster = FakeBooster()
    manager = _manager({"dataset": "v1"})
    with pytest.raises(OSError, match="upload failed"):
        manager.save(booster)
    assert writes == []


def test_disabled_checkpoint_preserves_native_training_path(monkeypatch):
    """기본 설정에서는 checkpoint state를 만들지 않고 기존 학습 경로를 사용한다."""
    expected_booster = object()
    train_calls: list[dict] = []
    uploads: list[tuple[object, str, bool]] = []

    def fake_train(params, train_set, **kwargs):
        """LightGBM 호출 인자를 기록하고 고정 Booster를 반환한다."""
        del params, train_set
        train_calls.append(kwargs)
        return expected_booster

    def fail_if_checkpoint_manager_is_created(*args, **kwargs):
        """비활성 경로의 checkpoint 접근을 즉시 실패시킨다."""
        del args, kwargs
        raise AssertionError("checkpoint manager must not be created")

    monkeypatch.setattr(train_common.config, "TRAIN_CHECKPOINT_ENABLED", False)
    monkeypatch.setattr(train_common.config, "LGB_NUM_BOOST_ROUND", 7)
    monkeypatch.setattr(train_common.lgb, "train", fake_train)
    monkeypatch.setattr(
        train_common.checkpointing,
        "TrainingCheckpointManager",
        fail_if_checkpoint_manager_is_created,
    )
    monkeypatch.setattr(
        train_common.model_io,
        "stage_and_upload_booster",
        lambda booster, key, log_to_mlflow: uploads.append(
            (booster, key, log_to_mlflow)
        ),
    )

    booster = train_common._train_phase_with_checkpoint(
        model_name="rental",
        phase_name="poisson",
        target_col="rental_count",
        exposure_col="rental_exposure",
        table_path="features/rental",
        feature_columns=["station_no"],
        train_dates=["2025-01-01"],
        valid_dates=["2025-01-02"],
        filters=[("horizon", "in", [1])],
        params={"objective": "poisson"},
        train_set=object(),
        valid_set=None,
        final_model_key="models/rental_poisson.txt",
        models_prefix="models/archive",
        is_primary=True,
    )

    assert booster is expected_booster
    assert train_calls[0]["num_boost_round"] == 7
    assert "init_model" not in train_calls[0]
    assert uploads == [(expected_booster, "models/rental_poisson.txt", True)]
