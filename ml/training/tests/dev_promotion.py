"""training.promotion의 승격 판정(should_promote)과 챔피언 포인터 전환
(promote_challenger)을 검증한다."""

import pytest
from ml_core import common_config
from ml_core.paths import read_champion_prefix

from training.promotion import promote_challenger, should_promote

_CHAMPION = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.83}


@pytest.fixture(autouse=True)
def _clear_champion_prefix_cache():
    read_champion_prefix.cache_clear()
    yield
    read_champion_prefix.cache_clear()


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

    promote_challenger("rental", archive_prefix)

    assert read_champion_prefix("rental") == archive_prefix


def test_promote_challenger_does_not_touch_other_model_names_pointer():
    """rental을 승격해도 return의 챔피언 포인터는 그대로여야 한다."""
    return_prefix = "models/archive/dt=2026-08-01/default"
    promote_challenger("return", return_prefix)

    promote_challenger("rental", "models/archive/dt=2026-08-17/default")

    assert read_champion_prefix("return") == return_prefix


def test_promote_challenger_does_not_copy_any_archive_files():
    """archive는 immutable하게 그대로 둔다 — 챔피언 자리(MODELS_PREFIX 루트)에
    파일이 새로 생기지 않아야 한다."""
    from core import s3 as s3_io

    archive_prefix = "models/archive/dt=2026-08-17/default"
    s3_io.put_object_bytes(f"{archive_prefix}/rental_poisson.txt", b"rental-booster-bytes")

    promote_challenger("rental", archive_prefix)

    assert s3_io.get_object_bytes("models/rental_poisson.txt") is None
    # archive 원본은 그대로 남아있어야 한다.
    assert s3_io.get_object_bytes(f"{archive_prefix}/rental_poisson.txt") == b"rental-booster-bytes"
