"""생활인구 원천 날짜 계약의 평일·특수일 선택을 검증한다."""

from datetime import date

from evaluation.population_contract import population_source_date_contract


def test_weekday_population_source_dates_use_one_to_eight_week_contract() -> None:
    """평일은 1~4주 전을 필수로, 5~8주 전을 fallback으로 고정한다."""
    contract = population_source_date_contract(date(2025, 3, 17))

    assert contract.base_dates == (
        date(2025, 3, 10),
        date(2025, 3, 3),
        date(2025, 2, 24),
        date(2025, 2, 17),
    )
    assert contract.fallback_dates == (
        date(2025, 2, 10),
        date(2025, 2, 3),
        date(2025, 1, 27),
        date(2025, 1, 20),
    )


def test_holiday_population_source_dates_use_previous_special_days() -> None:
    """공휴일은 직전 일요일·공휴일 네 날짜만 필수 후보로 고른다."""
    contract = population_source_date_contract(date(2025, 1, 1))

    assert contract.base_dates == (
        date(2024, 12, 29),
        date(2024, 12, 25),
        date(2024, 12, 22),
        date(2024, 12, 15),
    )
    assert contract.fallback_dates == (
        date(2024, 11, 27),
        date(2024, 11, 20),
        date(2024, 11, 13),
        date(2024, 11, 6),
    )
