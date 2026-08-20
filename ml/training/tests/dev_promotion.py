"""training.promotion의 승격 판정(should_promote)과 챔피언 포인터 전환
(promote_challenger)을 검증한다."""

import pytest
from core import s3 as s3_io
from ml_core import common_config, scoring
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer
from ml_core.serving_contract import ServingProfileContractError

from training.promotion import (
    ChampionAlreadyExistsError,
    bootstrap_challenger,
    promote_challenger,
    should_promote,
)

_CHAMPION = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.83}


@pytest.fixture(autouse=True)
def _clear_champion_prefix_cache():
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()
    yield
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()


def _write_profile(model_name: str, archive_prefix: str, profile: dict | None = None) -> dict:
    """승격 대상 아카이브에 effective profile을 저장하고 그 값을 반환한다."""
    payload = profile or common_config.effective_profile()
    s3_io.write_json(model_json_key(model_name, "profile", archive_prefix), payload)
    return payload


def test_should_promote_when_no_champion_exists_yet():
    promote, reasons = should_promote({"poisson_deviance_test": 999.0, "p10_p90_coverage_calibrated_test": 0.0}, None)
    assert promote is True
    assert any("챔피언이 아직 없음" in r for r in reasons)


def test_should_promote_when_challenger_improves_deviance_and_coverage_in_range():
    challenger = {"poisson_deviance_test": 0.9, "p10_p90_coverage_calibrated_test": 0.83}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is True
    assert len(reasons) == 2


def test_should_reject_when_challenger_deviance_is_worse():
    challenger = {"poisson_deviance_test": 1.1, "p10_p90_coverage_calibrated_test": 0.83}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is False
    assert any("미달" in r and "deviance" in r for r in reasons)


def test_should_reject_when_coverage_out_of_range():
    lo = common_config.CONFORMAL_TARGET_COVERAGE - common_config.COVERAGE_DRIFT_THRESHOLD
    challenger = {"poisson_deviance_test": 0.9, "p10_p90_coverage_calibrated_test": lo - 0.01}
    promote, reasons = should_promote(challenger, _CHAMPION)
    assert promote is False
    assert any("미달" in r and "coverage" in r for r in reasons)


def test_should_promote_when_deviance_exactly_ties_champion():
    challenger = {"poisson_deviance_test": _CHAMPION["poisson_deviance_test"], "p10_p90_coverage_calibrated_test": 0.83}
    promote, _ = should_promote(challenger, _CHAMPION)
    assert promote is True


def test_promote_challenger_points_champion_at_archive_prefix():
    """더 이상 파일을 복사하지 않는다 — 포인터가 archive_prefix를 가리키는지만 확인한다."""
    archive_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", archive_prefix)

    promote_challenger("rental", archive_prefix)

    assert read_champion_prefix("rental") == archive_prefix


def test_promote_challenger_does_not_touch_other_model_names_pointer():
    """rental을 승격해도 return의 챔피언 포인터는 그대로여야 한다."""
    return_prefix = "models/archive/dt=2026-08-01/default"
    _write_profile("return", return_prefix)
    promote_challenger("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", rental_prefix)
    promote_challenger("rental", rental_prefix)

    assert read_champion_prefix("return") == return_prefix


def test_promote_challenger_invalidates_scoring_caches_so_repromotion_is_consistent():
    """**사용자 질문 재현**: "학습해봤더니 구려서 같은 프로세스 안에서 계속
    재학습→재승격"을 반복하면, 재승격 직후 다음 채점부터는 read_champion_prefix()/
    load_conformal_correction()이 전부 새 archive 하나로 일관되게 나와야 한다.
    write_champion_pointer() 혼자 read_champion_prefix만 비우면 scoring.py 쪽
    캐시가 옛 archive에 머물러 오히려 더 나쁜 불일치가 생긴다(dev_champion_pointer.py/
    dev_scoring.py의 회귀 테스트가 그 실패 모드를 고정해둠) — promote_challenger()가
    셋을 한꺼번에 비워서 막는다."""
    old_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", old_prefix)
    promote_challenger("rental", old_prefix)
    s3_io.write_json(f"{old_prefix}/rental_conformal_correction.json", {"correction": 1.5, "target_coverage": 0.8})
    assert scoring.load_conformal_correction("rental") == 1.5  # 캐시를 채워둔다
    assert scoring.validate_champion_serving_contract("rental")
    assert scoring.validate_champion_serving_contract.cache_info().currsize == 1

    new_prefix = "models/archive/dt=2026-08-18/default"
    _write_profile("rental", new_prefix)
    s3_io.write_json(f"{new_prefix}/rental_conformal_correction.json", {"correction": 9.9, "target_coverage": 0.8})
    promote_challenger("rental", new_prefix)

    assert read_champion_prefix("rental") == new_prefix
    assert scoring.validate_champion_serving_contract.cache_info().currsize == 0
    assert scoring.load_conformal_correction("rental") == 9.9


def test_promote_challenger_does_not_copy_any_archive_files():
    """archive는 immutable하게 그대로 둔다 — 챔피언 자리(MODELS_PREFIX 루트)에
    파일이 새로 생기지 않아야 한다."""
    archive_prefix = "models/archive/dt=2026-08-17/default"
    _write_profile("rental", archive_prefix)
    s3_io.put_object_bytes(f"{archive_prefix}/rental_poisson.txt", b"rental-booster-bytes")

    promote_challenger("rental", archive_prefix)

    assert s3_io.get_object_bytes("models/rental_poisson.txt") is None
    # archive 원본은 그대로 남아있어야 한다.
    assert s3_io.get_object_bytes(f"{archive_prefix}/rental_poisson.txt") == b"rental-booster-bytes"


def test_promote_challenger_bootstraps_when_other_champion_does_not_exist():
    """최초 배포에서는 반대 모델 포인터가 아직 없어도 첫 모델을 승격할 수 있다."""
    archive_prefix = "models/archive/dt=2026-08-17/bootstrap"
    _write_profile("rental", archive_prefix)

    promote_challenger("rental", archive_prefix)

    assert read_champion_prefix("rental") == archive_prefix
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("return")


def test_bootstrap_challenger_refuses_to_overwrite_existing_same_model_pointer():
    """최초 승격 전용 경로는 이미 생긴 같은 모델 챔피언을 절대 교체하지 않는다."""
    old_prefix = "models/archive/dt=2026-08-17/initial"
    _write_profile("rental", old_prefix)
    promote_challenger("rental", old_prefix)

    new_prefix = "models/archive/dt=2026-08-18/accidental-rerun"
    _write_profile("rental", new_prefix)
    with pytest.raises(ChampionAlreadyExistsError, match="이미 존재"):
        bootstrap_challenger("rental", new_prefix)

    read_champion_prefix.cache_clear()
    assert read_champion_prefix("rental") == old_prefix


def test_bootstrap_challenger_still_runs_serving_profile_guard():
    """챔피언이 없어도 현재 서빙과 다른 profile을 최초 포인터로 만들 수 없다."""
    archive_prefix = "models/archive/dt=2026-08-17/incompatible-bootstrap"
    profile = common_config.effective_profile()
    profile["GRID_TICK_MINUTES"] += 5
    _write_profile("return", archive_prefix, profile)

    with pytest.raises(ServingProfileContractError, match="GRID_TICK_MINUTES"):
        bootstrap_challenger("return", archive_prefix)

    with pytest.raises(FileNotFoundError):
        read_champion_prefix("return")


def test_promote_challenger_rejects_profile_incompatible_with_active_serving():
    """현재 실시간 feature 계약과 다른 챌린저는 포인터를 쓰기 전에 거부한다."""
    archive_prefix = "models/archive/dt=2026-08-17/incompatible"
    profile = common_config.effective_profile()
    profile["ROLLING_EMBARGO_MINUTES"] += 5
    _write_profile("rental", archive_prefix, profile)

    with pytest.raises(ServingProfileContractError, match="ROLLING_EMBARGO_MINUTES"):
        promote_challenger("rental", archive_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_rejects_contract_different_from_other_champion():
    """대여와 반납 챔피언이 서로 다른 feature 의미를 갖는 상태를 만들지 않는다."""
    return_prefix = "models/archive/dt=2026-08-01/legacy-mismatch"
    return_profile = common_config.effective_profile()
    return_profile["TARGET_HORIZON_MINUTES"] += 30
    _write_profile("return", return_prefix, return_profile)
    # 이미 잘못 승격된 레거시 상태를 재현하려고 저수준 포인터 함수를 직접 쓴다.
    write_champion_pointer("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/current"
    _write_profile("rental", rental_prefix)

    with pytest.raises(ServingProfileContractError, match="TARGET_HORIZON_MINUTES"):
        promote_challenger("rental", rental_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_allows_training_and_lgb_only_differences():
    """같은 serving contract라면 모델별 튜닝과 학습 기간은 달라도 승격한다."""
    return_prefix = "models/archive/dt=2026-08-01/return-tuned"
    return_profile = common_config.effective_profile()
    return_profile["TRAIN_LOOKBACK_MONTHS"] = 6
    return_profile["LGB_PARAMS_COMMON"] = {**return_profile["LGB_PARAMS_COMMON"], "num_leaves": 31}
    _write_profile("return", return_prefix, return_profile)
    promote_challenger("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/rental-tuned"
    rental_profile = common_config.effective_profile()
    rental_profile["TRAIN_LOOKBACK_MONTHS"] = 18
    rental_profile["LGB_PARAMS_COMMON"] = {**rental_profile["LGB_PARAMS_COMMON"], "num_leaves": 127}
    _write_profile("rental", rental_prefix, rental_profile)

    promote_challenger("rental", rental_prefix)

    assert read_champion_prefix("rental") == rental_prefix
    assert read_champion_prefix("return") == return_prefix


def test_promote_challenger_rejects_missing_effective_profile():
    """profile 아티팩트가 없는 챌린저는 계약을 확인할 수 없어 승격하지 않는다."""
    archive_prefix = "models/archive/dt=2026-08-17/no-profile"

    with pytest.raises(ServingProfileContractError, match="effective profile이 없습니다"):
        promote_challenger("rental", archive_prefix)

    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")


def test_promote_challenger_rejects_other_champion_without_profile():
    """반대 모델의 계약을 증명할 수 없으면 새 모델도 섞어 승격하지 않는다."""
    return_prefix = "models/archive/dt=2026-08-01/legacy-no-profile"
    write_champion_pointer("return", return_prefix)

    rental_prefix = "models/archive/dt=2026-08-17/current"
    _write_profile("rental", rental_prefix)

    with pytest.raises(ServingProfileContractError, match="effective profile이 없습니다"):
        promote_challenger("rental", rental_prefix)

    read_champion_prefix.cache_clear()
    with pytest.raises(FileNotFoundError):
        read_champion_prefix("rental")
