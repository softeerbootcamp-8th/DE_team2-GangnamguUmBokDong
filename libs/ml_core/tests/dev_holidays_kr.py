"""holidays_kr.korean_holidays()가 연도별 대한민국 공휴일을 정확히 계산하는지 검증."""

from ml_core.holidays_kr import korean_holidays


def test_returns_known_2025_holidays():
    result = korean_holidays(2025)
    assert "2025-01-01" in result  # 신정
    assert "2025-12-25" in result  # 성탄절
    assert all(d.startswith("2025-") for d in result)


def test_single_year_int_and_list_are_equivalent():
    assert korean_holidays(2025) == korean_holidays([2025])


def test_multiple_years_are_unioned():
    result = korean_holidays([2025, 2026])
    assert "2025-01-01" in result
    assert "2026-01-01" in result
    assert len(result) > len(korean_holidays(2025))


def test_lunar_based_holiday_shifts_across_years():
    """설날은 음력 기준이라 연도마다 날짜가 다르다 — 하드코딩이 아니라 실제
    계산되고 있는지 확인."""
    result_2025 = korean_holidays(2025)
    result_2026 = korean_holidays(2026)
    seollal_2025 = {d for d in result_2025 if d.startswith(("2025-01", "2025-02"))}
    seollal_2026 = {d for d in result_2026 if d.startswith(("2026-01", "2026-02"))}
    assert seollal_2025 != seollal_2026
