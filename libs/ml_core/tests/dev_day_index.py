"""day_index()가 2000-01-01 기준 경과일수를 단조증가하며 연도 경계를 올바르게
넘기는지 검증 — month/cyclical 인코딩과 달리 연도 정보를 그대로 담고 있어야 한다.
"""

from datetime import date

from ml_core.day_index import DAY_INDEX_EPOCH, day_index


def test_epoch_is_day_zero():
    assert day_index(DAY_INDEX_EPOCH) == 0


def test_monotonically_increasing():
    assert day_index(date(2025, 1, 1)) < day_index(date(2025, 12, 31))


def test_year_boundary_is_close_not_far():
    """2025-12-31과 2026-01-01은 진짜로 하루 차이 — day index도 1 차이여야 한다."""
    d1 = day_index(date(2025, 12, 31))
    d2 = day_index(date(2026, 1, 1))
    assert d2 - d1 == 1


def test_same_calendar_year_far_apart_dates_are_far_in_day_index():
    """같은 해의 1/10과 12/20은 실제로 멀다 — month 순환 인코딩과 달리 day
    index는 이 둘을 가깝다고 착각하지 않는다."""
    d1 = day_index(date(2025, 1, 10))
    d2 = day_index(date(2025, 12, 20))
    assert d2 - d1 > 300


def test_fits_in_int16_for_realistic_years():
    """Spark엔 unsigned 타입이 없어 day는 ShortType(부호 있는 16비트, 최대 32,767)로
    저장된다 — 2000-01-01 기준 약 서기 2089년까지가 그 한계다. TRAIN_YEAR(2025)
    기준으로는 한참 여유 있는 범위인지 확인한다."""
    assert day_index(date(2080, 1, 1)) < 32768
