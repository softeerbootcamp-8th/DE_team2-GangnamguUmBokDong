"""monthly_retrain_check.py의 CLI 옵션 및 챌린저 승격/차선책 승격/유지 로직을 검증한다."""

import json

import pytest
from core import s3 as s3_io
from ml_core import common_config, scoring
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer

from training.scripts import monthly_retrain_check as mrc


@pytest.fixture(autouse=True)
def _clear_champion_cache():
    """테스트 간 챔피언 포인터 캐시를 초기화한다."""
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    yield
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()


def _setup_champion(model_name: str, archive_prefix: str, deviance: float = 1.0, coverage: float = 0.80):
    """테스트용 챔피언 포인터 및 metrics.json, profile.json을 등록한다."""
    write_champion_pointer(model_name, archive_prefix)
    metrics = {
        "poisson_deviance_test": deviance,
        "p10_p90_coverage_calibrated_test": coverage,
    }
    s3_io.write_json(model_json_key(model_name, "metrics", archive_prefix), metrics)
    s3_io.write_json(model_json_key(model_name, "profile", archive_prefix), common_config.effective_profile())


def test_attempt_promotion_promotes_fully_qualified_lowest_deviance(monkeypatch):
    """완전 승격 기준을 충족하는 후보 중 deviance가 가장 낮은 모델이 승격된다."""
    _setup_champion("rental", "models/archive/2026-01-01-champ/builtin-default", deviance=1.00)

    # 3개 후보 시뮬레이션:
    # 1: deviance 0.95, coverage 0.80 (완전 통과)
    # 2: deviance 0.90, coverage 0.80 (완전 통과 - 최저 deviance)
    # 3: deviance 1.05, coverage 0.80 (deviance 미달)
    candidates = [
        ("profile-1", {"poisson_deviance_test": 0.95, "p10_p90_coverage_calibrated_test": 0.80}),
        ("profile-2", {"poisson_deviance_test": 0.90, "p10_p90_coverage_calibrated_test": 0.80}),
        ("profile-3", {"poisson_deviance_test": 1.05, "p10_p90_coverage_calibrated_test": 0.80}),
    ]

    monkeypatch.setattr(mrc, "_candidate_profiles", lambda _: [(name, {}) for name, _ in candidates])
    monkeypatch.setattr(mrc, "_validate_candidate_serving_contract", lambda *_: None)

    def mock_run_training(model_name, profile_name, archive_date, env_overrides):
        for name, metrics in candidates:
            if name == profile_name:
                from ml_core.paths import archive_models_prefix
                prefix = archive_models_prefix(archive_date, profile_name)
                s3_io.write_json(model_json_key(model_name, "profile", prefix), common_config.effective_profile())
                return metrics
        raise ValueError("Unknown profile")

    monkeypatch.setattr(mrc, "_run_training_subprocess", mock_run_training)

    champion_metrics = {"poisson_deviance_test": 1.00, "p10_p90_coverage_calibrated_test": 0.80}
    promoted = mrc._attempt_promotion("rental", champion_metrics, skip_feature_pipeline=True)

    assert promoted is True
    # 최저 deviance인 profile-2로 승격되었는지 확인
    new_champion = read_champion_prefix("rental")
    assert "profile-2" in new_champion


def test_attempt_promotion_fallback_to_better_deviance_when_coverage_missed(monkeypatch):
    """완전 충족 후보가 없어도 챔피언보다 deviance가 우수한 후보가 있다면 차선책으로 승격된다."""
    _setup_champion("rental", "models/archive/2026-01-01-champ/builtin-default", deviance=1.00)

    # 후보 1: deviance 0.92, coverage 0.60 (coverage 미달이지만 deviance 0.92 < 1.00)
    # 후보 2: deviance 0.88, coverage 0.60 (coverage 미달이지만 deviance 0.88 < 1.00 - 최선)
    # 후보 3: deviance 1.10, coverage 0.80 (deviance 악화)
    candidates = [
        ("profile-1", {"poisson_deviance_test": 0.92, "p10_p90_coverage_calibrated_test": 0.60}),
        ("profile-2", {"poisson_deviance_test": 0.88, "p10_p90_coverage_calibrated_test": 0.60}),
        ("profile-3", {"poisson_deviance_test": 1.10, "p10_p90_coverage_calibrated_test": 0.80}),
    ]

    monkeypatch.setattr(mrc, "_candidate_profiles", lambda _: [(name, {}) for name, _ in candidates])
    monkeypatch.setattr(mrc, "_validate_candidate_serving_contract", lambda *_: None)

    def mock_run_training(model_name, profile_name, archive_date, env_overrides):
        for name, metrics in candidates:
            if name == profile_name:
                from ml_core.paths import archive_models_prefix
                prefix = archive_models_prefix(archive_date, profile_name)
                s3_io.write_json(model_json_key(model_name, "profile", prefix), common_config.effective_profile())
                return metrics
        raise ValueError("Unknown profile")

    monkeypatch.setattr(mrc, "_run_training_subprocess", mock_run_training)

    champion_metrics = {"poisson_deviance_test": 1.00, "p10_p90_coverage_calibrated_test": 0.80}
    promoted = mrc._attempt_promotion("rental", champion_metrics, skip_feature_pipeline=True)

    assert promoted is True
    # 차선책 중 최선인 profile-2로 승격되었는지 확인
    new_champion = read_champion_prefix("rental")
    assert "profile-2" in new_champion


def test_attempt_promotion_keeps_champion_when_all_worse(monkeypatch):
    """모든 후보가 챔피언보다 deviance가 높으면 챔피언을 그대로 유지한다."""
    initial_prefix = "models/archive/2026-01-01-champ/builtin-default"
    _setup_champion("rental", initial_prefix, deviance=1.00)

    # 모든 후보가 deviance 1.00보다 나쁨
    candidates = [
        ("profile-1", {"poisson_deviance_test": 1.05, "p10_p90_coverage_calibrated_test": 0.80}),
        ("profile-2", {"poisson_deviance_test": 1.15, "p10_p90_coverage_calibrated_test": 0.79}),
    ]

    monkeypatch.setattr(mrc, "_candidate_profiles", lambda _: [(name, {}) for name, _ in candidates])
    monkeypatch.setattr(mrc, "_validate_candidate_serving_contract", lambda *_: None)

    def mock_run_training(model_name, profile_name, archive_date, env_overrides):
        for name, metrics in candidates:
            if name == profile_name:
                return metrics
        raise ValueError("Unknown profile")

    monkeypatch.setattr(mrc, "_run_training_subprocess", mock_run_training)

    champion_metrics = {"poisson_deviance_test": 1.00, "p10_p90_coverage_calibrated_test": 0.80}
    promoted = mrc._attempt_promotion("rental", champion_metrics, skip_feature_pipeline=True)

    assert promoted is False
    # 기존 챔피언이 유지되었는지 확인
    assert read_champion_prefix("rental") == initial_prefix


def test_main_check_only_json_output(monkeypatch, capsys):
    """--check-only --json-output 실행 시 JSON 형태의 요약이 출력된다."""
    mock_results = [
        {
            "model_name": "rental",
            "needs_retrain": True,
            "period": {"start": "2026-07-01", "end": "2026-07-31"},
            "n_rows": 1000,
            "baseline_deviance": 0.90,
            "current_deviance": 1.05,
            "deviance_relative_change": 0.16,
            "baseline_coverage": 0.80,
            "current_coverage": 0.79,
            "coverage_drift": -0.01,
            "reasons": ["deviance 16.7% 악화"],
        },
        {
            "model_name": "return",
            "needs_retrain": False,
            "period": {"start": "2026-07-01", "end": "2026-07-31"},
            "n_rows": 1000,
            "baseline_deviance": 0.85,
            "current_deviance": 0.86,
            "deviance_relative_change": 0.01,
            "baseline_coverage": 0.82,
            "current_coverage": 0.81,
            "coverage_drift": -0.01,
            "reasons": [],
        },
    ]

    monkeypatch.setattr(mrc, "check_all_models", lambda as_of=None: mock_results)
    monkeypatch.setattr("sys.argv", ["monthly_retrain_check", "--check-only", "--json-output"])

    mrc.main()

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["needs_retrain"] is True
    assert data["retrain_models"] == ["rental"]
    assert len(data["results"]) == 2
