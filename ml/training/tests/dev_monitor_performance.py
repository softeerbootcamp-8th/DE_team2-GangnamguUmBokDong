"""monitor_performance.py의 판정 로직(decide_retrain) 검증 — 실제 모델/데이터 없이
순수 로직만 합성 입력으로 확인한다. ml_core/common_config.py의 임계값(기본 10%/15%p)을
그대로 적용하는지, 경계값에서 정확히 판정이 갈리는지가 핵심이다.
"""

import mlflow
import numpy as np
import pandas as pd
import pytest
from core import s3 as s3_io
from ml_core import common_config, scoring
from ml_core.day_index import day_index
from ml_core.model_contract import RETURN_FEATURE_COLUMNS
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer

from training import config, monitor_performance
from training.monitor_performance import (
    _log_to_mlflow,
    _recent_month_range,
    decide_retrain,
    evaluate_recent_performance,
)


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


def _result(**overrides) -> dict:
    base = {
        "model_name": "rental",
        "period": {"start": "2026-01-01", "end": "2026-01-31"},
        "n_rows": 100,
        "baseline_deviance": 1.0,
        "current_deviance": 1.05,
        "deviance_relative_change": 0.05,
        "baseline_rmse": 2.0,
        "current_rmse": 2.1,
        "baseline_coverage": 0.8,
        "current_coverage": 0.82,
        "coverage_drift": 0.02,
        "needs_retrain": False,
        "reasons": [],
    }
    base.update(overrides)
    return base


def test_log_to_mlflow_writes_params_and_metrics(tmp_path, monkeypatch):
    """월별 점검 결과가 MLflow에 그대로 기록되는지 확인한다(로컬 파일 backend 사용)."""
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setattr(monitor_performance.mlflow_tracking, "MLFLOW_TRACKING_URI", str(tmp_path / "mlruns"))

    _log_to_mlflow(_result(), horizon=1)

    client = mlflow.tracking.MlflowClient()
    exp = client.get_experiment_by_name(config.MLFLOW_MONITORING_EXPERIMENT_NAME)
    run = client.search_runs([exp.experiment_id], max_results=1)[0]
    assert run.info.status == "FINISHED"
    assert run.data.metrics["deviance_relative_change"] == 0.05
    assert run.data.metrics["needs_retrain"] == 0.0
    assert run.data.params["model_name"] == "rental"
    assert run.data.params["horizon"] == "1"


def test_log_to_mlflow_swallows_errors_so_check_keeps_going(monkeypatch, capsys):
    """MLflow 서버가 없어도(로컬 개발 등) 월별 점검 자체(재학습 판단)는 죽으면 안 된다."""
    monkeypatch.setattr(
        monitor_performance.mlflow_tracking, "configure", lambda *_: (_ for _ in ()).throw(RuntimeError("no server"))
    )

    _log_to_mlflow(_result(), horizon=1)  # 예외를 던지지 않아야 한다

    assert "MLflow 로깅 실패" in capsys.readouterr().out


class _FakeBooster:
    """scoring.predict()가 부르는 최소 인터페이스(.predict(X))만 흉내낸 가짜 booster.

    실제 LightGBM 모델을 학습시키지 않고도 predict()의 "본문"(특히 리뷰에서
    지적된 `df[["station_no", "date", "hour"]]` 줄)이 실제로 실행되게 하는 게
    목적이라, 값 자체는 임의의 상수면 충분하다.
    """

    def __init__(self, value: float = 3.0):
        self._value = value

    def predict(self, X, num_iteration=None):
        return np.full(len(X), self._value)


def test_evaluate_recent_performance_does_not_crash_on_missing_date_hour_columns(monkeypatch):
    """회귀 재현 — needed에 "date"/"hour"가 없으면 scoring.predict()의
    `df[["station_no", "date", "hour"]]`에서 KeyError가 났다(리뷰 지적). 실제
    predict() 본문을 그대로 실행시켜서(booster/station_dtype/conformal_correction만
    가짜로 교체) 이 줄이 실제로 안 죽는지 확인한다.
    """
    monkeypatch.setattr(scoring, "load_boosters", lambda model_name: {
        "poisson": _FakeBooster(3.0), "q10": _FakeBooster(1.0), "q50": _FakeBooster(3.0), "q90": _FakeBooster(5.0),
    })
    monkeypatch.setattr(scoring, "load_conformal_correction", lambda model_name: 0.0)
    monkeypatch.setattr(scoring, "load_station_dtype", lambda model_name: pd.CategoricalDtype(categories=[1]))
    read_champion_prefix.cache_clear()

    as_of = "2026-08-17"
    start, _end = _recent_month_range(1, as_of=as_of)
    seeded_date = start  # 구간 안 아무 날짜 — 첫날로 고정

    table_path = config.RETURN_MULTI_HORIZON_FEATURES_TABLE_PARQUET
    rows = [
        {
            "station_no": 1, "capacity": 10, "lat": 37.5, "lon": 127.0, "temp": 20.0, "precip": 0.0,
            "pop_total": 1000.0, "minute": 0, "dow": 0, "is_holiday": 0,
            "day": day_index(pd.Timestamp(seeded_date).date()), "horizon": 1,
            "return_lag_1h": 3.0 + i, "return_count": 5 + i, "hour": 0,
        }
        for i in range(6)
    ]
    assert set(RETURN_FEATURE_COLUMNS) <= set(rows[0].keys())
    s3_io.write_parquet(pd.DataFrame(rows), f"{table_path}/date={seeded_date}/part-0000.parquet")

    write_champion_pointer("return", "models/archive/dt=test/default")
    s3_io.write_json(
        model_json_key("return", "profile", "models/archive/dt=test/default"),
        common_config.effective_profile(),
    )
    s3_io.write_json(
        model_json_key("return", "metrics", "models/archive/dt=test/default"),
        {"poisson_deviance_test": 1.0, "p10_p90_coverage_calibrated_test": 0.8, "rmse_test": 2.0},
    )

    evaluation = evaluate_recent_performance("return", "return_count", None, lookback_months=1, as_of=as_of)

    assert evaluation["model_name"] == "return"
    assert evaluation["n_rows"] == 6
