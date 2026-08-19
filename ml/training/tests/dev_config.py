"""training.config.safety_cutoff_date()와 day-of-month split 상수, 학습기간 롤링 윈도우를 검증한다."""

from datetime import date, timedelta

from training.config import (
    TEST_DAYS_OF_MONTH,
    TRAIN_WINDOW_END,
    TRAIN_WINDOW_START,
    TRAINING_SAFETY_MARGIN_DAYS,
    VALID_DAYS_OF_MONTH,
    safety_cutoff_date,
    today_kst,
    unique_archive_date,
)


def test_safety_cutoff_is_exactly_margin_before_as_of():
    as_of = date(2026, 8, 17)
    assert safety_cutoff_date(as_of) == as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


def test_safety_cutoff_defaults_to_today():
    # as_of 미지정 시 today_kst() 기준으로 계산돼야 한다 — 정확한 값 대신 "오늘보다
    # 마진만큼 과거"라는 관계만 확인한다(테스트 실행 시각에 의존하지 않기 위함).
    assert safety_cutoff_date() == today_kst() - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)


def test_valid_and_test_days_of_month_do_not_overlap():
    assert not (VALID_DAYS_OF_MONTH & TEST_DAYS_OF_MONTH)


def test_train_window_is_rolling_and_ends_at_safety_cutoff():
    # 고정 TRAIN_YEAR 대신 "오늘 기준 롤링 윈도우"로 바뀌었다(2026-08) — 끝은
    # safety_cutoff_date()와 정확히 같아야 하고(같은 마진을 공유), 시작은 그보다
    # 과거여야 한다.
    assert TRAIN_WINDOW_END == safety_cutoff_date()
    assert TRAIN_WINDOW_START < TRAIN_WINDOW_END
    assert date(2000, 1, 1) <= TRAIN_WINDOW_START


def test_unique_archive_date_embeds_given_date_but_differs_across_calls():
    """회귀 재현 — archive_models_prefix()는 date+profile_name만으로 경로를 만들어서,
    같은 날 같은 프로필로 학습을 두 번 돌리면(수동 재실행 등) archive_prefix가
    겹쳐 이미 챔피언이 가리키는 아티팩트를 비원자적으로 덮어쓸 수 있었다(리뷰
    지적). unique_archive_date()가 매 호출마다 다른 값을 내야 이 문제가 해결된다."""
    as_of = date(2026, 8, 19)

    a = unique_archive_date(as_of)
    b = unique_archive_date(as_of)

    assert a != b
    assert a.startswith("2026-08-19-")
    assert b.startswith("2026-08-19-")
