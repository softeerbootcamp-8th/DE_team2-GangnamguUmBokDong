"""로컬 학습 smoke artifact 계약을 검증한다."""

from training.local_smoke_contract import _required_keys


def test_required_keys_cover_both_model_bundles() -> None:
    """대여·반납별 booster 4개와 JSON 4개를 모두 요구한다."""
    keys = _required_keys("models/archive/example/builtin-default")

    assert len(keys) == 16
    assert len(set(keys)) == 16
    assert "models/archive/example/builtin-default/rental_poisson.txt" in keys
    assert "models/archive/example/builtin-default/return_profile.json" in keys
