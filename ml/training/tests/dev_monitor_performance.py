"""monitor_performance.py의 판정 로직(decide_retrain) 검증 — 실제 모델/데이터 없이
순수 로직만 합성 입력으로 확인한다. ml_core/common_config.py의 임계값(기본 10%/15%p)을
그대로 적용하는지, 경계값에서 정확히 판정이 갈리는지가 핵심이다.
"""

import pytest

from training import config
from training.monitor_performance import _recent_month_range, decide_retrain


def _evaluation(deviance_relative_change: float, coverage_drift: float) -> dict:
    return {
        "model_name": "rental",
        "baseline_deviance": 1.0,
        "current_deviance": 1.0 + deviance_relative_change,
        "deviance_relative_change": deviance_relative_change,
        "baseline_coverage": 0.83,
        "current_coverage": 0.83 + coverage_drift,
        "coverage_drift": coverage_drift,
    }


def test_no_retrain_when_within_thresholds():
    """기본 임계값(10%, 15%p) 안이면 재학습 불필요."""
    evaluation = _evaluation(deviance_relative_change=0.05, coverage_drift=0.05)
    result = decide_retrain(evaluation)
    assert result["needs_retrain"] is False
    assert result["reasons"] == []


def test_retrain_triggered_by_deviance_degradation():
    """deviance가 임계값보다 많이 나빠지면 재학습 필요 + 이유에 명시."""
    evaluation = _evaluation(deviance_relative_change=config.PERFORMANCE_DEGRADATION_THRESHOLD + 0.01, coverage_drift=0.0)
    result = decide_retrain(evaluation)
    assert result["needs_retrain"] is True
    assert len(result["reasons"]) == 1
    assert "poisson_deviance" in result["reasons"][0]


def test_deviance_improvement_does_not_trigger():
    """deviance가 오히려 좋아졌으면(음수 변화) 재학습 트리거가 안 돼야 한다 — '변화'가 아니라 '악화'만 본다."""
    evaluation = _evaluation(deviance_relative_change=-0.20, coverage_drift=0.0)
    result = decide_retrain(evaluation)
    assert result["needs_retrain"] is False


def test_retrain_triggered_by_coverage_drift():
    """커버리지 드리프트가 임계값을 넘으면(개선/악화 방향 무관하게) 재학습 필요."""
    evaluation = _evaluation(deviance_relative_change=0.0, coverage_drift=config.COVERAGE_DRIFT_THRESHOLD + 0.01)
    result = decide_retrain(evaluation)
    assert result["needs_retrain"] is True
    assert any("커버리지" in r for r in result["reasons"])


def test_both_thresholds_breached_gives_two_reasons():
    evaluation = _evaluation(
        deviance_relative_change=config.PERFORMANCE_DEGRADATION_THRESHOLD + 0.05,
        coverage_drift=config.COVERAGE_DRIFT_THRESHOLD + 0.05,
    )
    result = decide_retrain(evaluation)
    assert result["needs_retrain"] is True
    assert len(result["reasons"]) == 2


@pytest.mark.parametrize(
    "as_of,lookback_months,expected",
    [
        ("2026-01-15", 1, ("2025-12-01", "2025-12-31")),
        ("2026-01-01", 1, ("2025-12-01", "2025-12-31")),
        ("2026-03-01", 3, ("2025-12-01", "2026-02-28")),
        ("2025-06-10", 1, ("2025-05-01", "2025-05-31")),
    ],
)
def test_recent_month_range(as_of, lookback_months, expected):
    """'완결된' 최근 N개월 범위를 정확히 계산해야 한다 — 이번 달(진행 중)은 항상 제외."""
    assert _recent_month_range(lookback_months, as_of=as_of) == expected
