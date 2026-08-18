"""training.config._default_window()의 "오늘 - 안전마진" 슬라이딩 구간 계산을 검증한다."""

from datetime import date, timedelta

from training.config import (
    TEST_DAYS,
    TRAIN_DAYS,
    TRAINING_SAFETY_MARGIN_DAYS,
    VALID_DAYS,
    _default_window,
)

_ONE_DAY = timedelta(days=1)


def test_test_end_is_exactly_safety_margin_before_as_of():
    as_of = date(2026, 8, 17)
    *_, test_end = _default_window(as_of)
    assert test_end == (as_of - timedelta(days=TRAINING_SAFETY_MARGIN_DAYS)).isoformat()


def test_windows_are_contiguous_and_non_overlapping():
    train_start, train_end, valid_start, valid_end, test_start, test_end = _default_window(date(2026, 8, 17))
    # 시간 순으로 하루씩 이어붙어야 한다(빈틈/겹침 없음).
    assert date.fromisoformat(train_end) + _ONE_DAY == date.fromisoformat(valid_start)
    assert date.fromisoformat(valid_end) + _ONE_DAY == date.fromisoformat(test_start)
    assert date.fromisoformat(train_start) <= date.fromisoformat(train_end)
    assert date.fromisoformat(valid_start) <= date.fromisoformat(valid_end)
    assert date.fromisoformat(test_start) <= date.fromisoformat(test_end)


def test_window_sizes_match_configured_day_counts():
    train_start, train_end, valid_start, valid_end, test_start, test_end = _default_window(date(2026, 8, 17))
    assert (date.fromisoformat(train_end) - date.fromisoformat(train_start)).days + 1 == TRAIN_DAYS
    assert (date.fromisoformat(valid_end) - date.fromisoformat(valid_start)).days + 1 == VALID_DAYS
    assert (date.fromisoformat(test_end) - date.fromisoformat(test_start)).days + 1 == TEST_DAYS
