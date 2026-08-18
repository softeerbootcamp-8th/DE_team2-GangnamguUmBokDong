"""monitor_performance.py의 판정 로직(decide_retrain) 검증 — 실제 모델/데이터 없이
순수 로직만 합성 입력으로 확인한다. ml_core/common_config.py의 임계값(기본 10%/15%p)을
그대로 적용하는지, 경계값에서 정확히 판정이 갈리는지가 핵심이다.
"""

import pandas as pd
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
        # TRAINING_SAFETY_MARGIN_DAYS(기본 7일)는 한 달보다 짧아서, as_of의 날짜에
        # 따라 "지난달 말일"이 이미 안전한 경우(마진 안 밀림)와 안 밀려야 하는 경우가
        # 둘 다 생긴다 — 이 파라미터라이즈가 그 두 경우를 실제 _recent_month_range()
        # 알고리즘으로 재계산해 고정해둔 것.
        ("2026-01-15", 1, ("2025-12-01", "2025-12-31")),  # 15일 지남(>=7일) — 안 밀림
        ("2026-01-01", 1, ("2025-11-01", "2025-11-30")),  # 1일밖에 안 지남 — 한 달 더 밀림
        ("2026-03-01", 3, ("2025-11-01", "2026-01-31")),
        ("2025-06-10", 1, ("2025-05-01", "2025-05-31")),  # 10일 지남(>=7일) — 안 밀림
    ],
)
def test_recent_month_range(as_of, lookback_months, expected):
    """'완결된' 최근 N개월 범위를 정확히 계산해야 한다 — 이번 달(진행 중)뿐 아니라,
    TRAINING_SAFETY_MARGIN_DAYS 안에 들어와 아직 rental_count가 사후 보정될 수 있는
    달도 전부 제외해야 한다(그냥 "지난달까지"가 아님 — 모듈 docstring 참고)."""
    assert _recent_month_range(lookback_months, as_of=as_of) == expected


def test_recent_month_range_end_never_within_safety_margin_of_as_of():
    """실제 TRAINING_SAFETY_MARGIN_DAYS 기준으로, 반환된 end가 그 마진보다 더 최근이면
    안 된다 — 이게 이번 수정의 핵심 불변조건이다(리뷰에서 지적된 문제)."""
    for as_of in ["2026-08-01", "2026-08-17", "2026-01-01", "2025-12-31", "2026-03-15"]:
        _, end = _recent_month_range(1, as_of=as_of)
        gap_days = (pd.Timestamp(as_of) - pd.Timestamp(end)).days
        assert gap_days >= config.TRAINING_SAFETY_MARGIN_DAYS, f"as_of={as_of}, end={end}, gap={gap_days}일"


def test_recent_month_range_boundary_exactly_at_cutoff_is_safe(monkeypatch):
    """end가 정확히 as_of - TRAINING_SAFETY_MARGIN_DAYS와 같으면(더 밀어낼 필요 없음)
    그 달을 그대로 쓴다 — '>' 비교라 경계값 자체는 안전하다고 판정돼야 한다."""
    monkeypatch.setattr(config, "TRAINING_SAFETY_MARGIN_DAYS", 10)  # 계산이 깔끔한 값으로 고정

    # 2026-01-31 말일 기준 정확히 10일 뒤인 2026-02-10에 실행하면 경계에 정확히 걸친다.
    assert _recent_month_range(1, as_of="2026-02-10") == ("2026-01-01", "2026-01-31")
    # 하루만 더 지나도(경계 안쪽) 아직 안전 — 그대로 1월을 씀.
    assert _recent_month_range(1, as_of="2026-02-11") == ("2026-01-01", "2026-01-31")
    # 반대로 하루 이르면(경계 밖) 1월도 아직 불안전 — 한 달 더 밀려 12월로.
    assert _recent_month_range(1, as_of="2026-02-09") == ("2025-12-01", "2025-12-31")
