"""training.promotion의 승격 판정(should_promote)과 아카이브->챔피언 복사
(promote_challenger)를 검증한다."""

from core import s3 as s3_io
from ml_core import common_config

from training.promotion import promote_challenger, should_promote

_CHAMPION = {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.83}


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


def test_promote_challenger_copies_only_matching_model_name_with_identical_content():
    archive_prefix = "models/archive/dt=2026-08-17/default"
    s3_io.put_object_bytes(f"{archive_prefix}/rental_poisson.txt", b"rental-booster-bytes")
    s3_io.put_object_bytes(f"{archive_prefix}/rental_metrics.json", b'{"poisson_deviance_test": 0.9}')
    s3_io.put_object_bytes(f"{archive_prefix}/return_poisson.txt", b"return-booster-bytes")

    copied = promote_challenger("rental", archive_prefix)

    assert sorted(copied) == ["rental_metrics.json", "rental_poisson.txt"]
    assert s3_io.get_object_bytes("models/rental_poisson.txt") == b"rental-booster-bytes"
    assert s3_io.get_object_bytes("models/rental_metrics.json") == b'{"poisson_deviance_test": 0.9}'
    # return_* 아티팩트는 건드리지 않아야 한다.
    assert s3_io.get_object_bytes("models/return_poisson.txt") is None


def test_promote_challenger_returns_empty_list_when_archive_empty():
    assert promote_challenger("rental", "models/archive/dt=2026-01-01/default") == []
