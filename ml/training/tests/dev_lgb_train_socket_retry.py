"""`_lgb_train_with_socket_retry()`가 분산 학습에서만 소켓류 오류를 재시도하고,
serial 모드나 무관한 오류는 그대로 통과/전파하는지 검증한다 — 실제 소켓/여러
머신 없이 `lgb.train()` 자체를 monkeypatch해서 확인한다(PR #248 리뷰 지적:
한 프로세스 안에서 phase마다 같은 포트로 소켓을 다시 맺는 게 이론적 위험이라는
데 대한 방어적 재시도, 실제 재현 사례는 없음)."""

import lightgbm as lgb
import pytest

from training.train_common import _lgb_train_with_socket_retry


def test_serial_mode_calls_lgb_train_once_without_retry_wrapper(monkeypatch):
    calls = []

    def _fake_train(params, train_set, **kwargs):
        calls.append(1)
        return "booster"

    monkeypatch.setattr("training.train_common.lgb.train", _fake_train)

    result = _lgb_train_with_socket_retry({"tree_learner": "serial"}, "dataset")

    assert result == "booster"
    assert len(calls) == 1


def test_distributed_mode_retries_on_transient_socket_error(monkeypatch):
    attempts = []

    def _fake_train(params, train_set, **kwargs):
        attempts.append(1)
        if len(attempts) < 3:
            raise lgb.basic.LightGBMError("Socket bind failed: Address already in use")
        return "booster"

    monkeypatch.setattr("training.train_common.lgb.train", _fake_train)
    monkeypatch.setattr("training.train_common.time.sleep", lambda seconds: None)

    result = _lgb_train_with_socket_retry({"tree_learner": "data"}, "dataset")

    assert result == "booster"
    assert len(attempts) == 3


def test_distributed_mode_does_not_retry_non_socket_errors(monkeypatch):
    """데이터/설정 오류처럼 재시도해도 절대 안 없어질 에러까지 재시도하면
    단순히 실패까지의 시간만 늘어난다 — 소켓류가 아니면 즉시 전파해야 한다."""
    attempts = []

    def _fake_train(params, train_set, **kwargs):
        attempts.append(1)
        raise lgb.basic.LightGBMError("Check failed: (num_data) > (0)")

    monkeypatch.setattr("training.train_common.lgb.train", _fake_train)
    monkeypatch.setattr("training.train_common.time.sleep", lambda seconds: (_ for _ in ()).throw(AssertionError("재시도하면 안 됨")))

    with pytest.raises(lgb.basic.LightGBMError, match="num_data"):
        _lgb_train_with_socket_retry({"tree_learner": "data"}, "dataset")

    assert len(attempts) == 1


def test_distributed_mode_gives_up_after_max_attempts(monkeypatch):
    attempts = []

    def _fake_train(params, train_set, **kwargs):
        attempts.append(1)
        raise lgb.basic.LightGBMError("Socket recv error")

    monkeypatch.setattr("training.train_common.lgb.train", _fake_train)
    monkeypatch.setattr("training.train_common.time.sleep", lambda seconds: None)

    with pytest.raises(lgb.basic.LightGBMError, match="Socket recv error"):
        _lgb_train_with_socket_retry({"tree_learner": "data"}, "dataset")

    assert len(attempts) == 3
