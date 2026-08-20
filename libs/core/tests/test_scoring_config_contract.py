"""Urgency scoring 정책의 versioned contract를 회귀 검증한다."""

from core.scoring_config import (
    FIRST_FORECAST_MIN,
    HALF_LIFE_MIN,
    RESPONSE_LAG_MIN,
    SEVERITY_SCALE,
    SUPPLY_LOW_STOCK_RATIO,
    URGENCY_SCORING_CONFIG_VERSION,
    URGENCY_STOCK_HISTORY_OFFSETS_MINUTES,
    URGENCY_STOCK_WINDOW_COUNT,
)


def test_urgency_scoring_v1_exact_values() -> None:
    """v1 점수 의미를 바꾸는 모든 상수 값을 고정한다."""
    assert URGENCY_SCORING_CONFIG_VERSION == "urgency-scoring-v1"
    assert RESPONSE_LAG_MIN == 30
    assert HALF_LIFE_MIN == 60
    assert FIRST_FORECAST_MIN == 60
    assert SUPPLY_LOW_STOCK_RATIO == 0.20
    assert SEVERITY_SCALE == 1.5


def test_urgency_stock_windows_are_oldest_to_newest() -> None:
    """과거 다섯 window와 현재 하나의 시간 방향·총수를 고정한다."""
    assert URGENCY_STOCK_HISTORY_OFFSETS_MINUTES == (-25, -20, -15, -10, -5)
    assert URGENCY_STOCK_WINDOW_COUNT == 6
    assert str(URGENCY_STOCK_WINDOW_COUNT) == "6"
