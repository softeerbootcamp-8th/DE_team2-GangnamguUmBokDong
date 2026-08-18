"""training.config.safety_cutoff_date()와 day-of-month split 상수를 검증한다."""

from datetime import date, timedelta

from training.config import (
    TEST_DAYS_OF_MONTH,
    TRAIN_YEAR,
    TRAINING_SAFETY_MARGIN_DAYS,
    VALID_DAYS_OF_MONTH,
    safety_cutoff_date,
)


def test_safety_cutoff_is_exactly_margin_before_as_of():
    as_of = date(2026, 8, 17)
    assert safety_cutoff_date(as_of) == as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


def test_safety_cutoff_defaults_to_today():
    # as_of 미지정 시 today_kst() 기준으로 계산돼야 한다 — 정확한 값 대신 "오늘보다
    # 마진만큼 과거"라는 관계만 확인한다(테스트 실행 시각에 의존하지 않기 위함).
    from training.config import today_kst

    assert safety_cutoff_date() == today_kst() - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


def test_valid_and_test_days_of_month_do_not_overlap():
    assert not (VALID_DAYS_OF_MONTH & TEST_DAYS_OF_MONTH)


def test_train_year_is_a_plausible_calendar_year():
    assert 2000 <= TRAIN_YEAR <= 2100
