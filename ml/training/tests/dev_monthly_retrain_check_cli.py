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

    monkeypatch.setattr(mrc, "check_all_models", lambda as_of=None, model_names=None: mock_results)
    monkeypatch.setattr("sys.argv", ["monthly_retrain_check", "--check-only", "--json-output"])

    mrc.main()

    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["needs_retrain"] is True
    assert data["retrain_models"] == ["rental"]
    assert len(data["results"]) == 2


def test_main_check_only_model_filtering(monkeypatch, capsys):
    """--models로 특정 모델을 지정했을 때 해당 모델만 평가되고(다른 모델의 feature
    mart를 읽는 비용/메모리를 아예 안 씀 — m4.large 컨테이너에서 실제로
    exitCode 137 OOM으로 확인됨, 2026-08-26) 요약에도 그 모델만 포함된다."""
    all_mock_results = [
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

    monkeypatch.setattr(
        mrc,
        "check_all_models",
        lambda as_of=None, model_names=None: (
            [r for r in all_mock_results if r["model_name"] in model_names] if model_names else all_mock_results
        ),
    )

    # 1. --models return: return은 정상이므로 needs_retrain=False
    monkeypatch.setattr("sys.argv", ["monthly_retrain_check", "--check-only", "--json-output", "--models", "return"])
    mrc.main()
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["needs_retrain"] is False
    assert data["retrain_models"] == []
    assert len(data["results"]) == 1
    assert data["results"][0]["model_name"] == "return"

    # 2. --models rental: rental은 재학습 필요하므로 needs_retrain=True
    monkeypatch.setattr("sys.argv", ["monthly_retrain_check", "--check-only", "--json-output", "--models", "rental"])
    mrc.main()
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["needs_retrain"] is True
    assert data["retrain_models"] == ["rental"]
    assert len(data["results"]) == 1
    assert data["results"][0]["model_name"] == "rental"


def test_main_check_only_writes_result_to_s3_key(monkeypatch, capsys):
    """--result-s3-key를 주면 EMR 스텝처럼 stdout을 못 읽는 호출부를 위해 같은
    요약을 S3에도 써준다(월간 재학습 DAG가 스텝 완료 후 이 키를 읽는다)."""
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
    ]
    monkeypatch.setattr(mrc, "check_all_models", lambda as_of=None, model_names=None: mock_results)
    monkeypatch.setattr(
        "sys.argv",
        ["monthly_retrain_check", "--check-only", "--result-s3-key", "models/training-runs/test/check.json"],
    )

    mrc.main()

    written = s3_io.read_json("models/training-runs/test/check.json")
    assert written["needs_retrain"] is True
    assert written["retrain_models"] == ["rental"]


def test_main_execute_writes_promotion_result_to_s3_key(monkeypatch):
    """--execute --result-s3-key 실행 시 모델별 승격 여부를 S3에 기록한다."""
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
    ]
    monkeypatch.setattr(mrc, "check_all_models", lambda as_of=None, model_names=None: mock_results)
    monkeypatch.setattr(mrc, "_load_baseline_metrics", lambda model_name: None)
    monkeypatch.setattr(mrc, "_attempt_promotion", lambda *args, **kwargs: True)
    monkeypatch.setattr(
        "sys.argv",
        [
            "monthly_retrain_check",
            "--execute",
            "--skip-feature-pipeline",
            "--profile-name",
            "builtin-default",
            "--result-s3-key",
            "models/training-runs/test/execute.json",
        ],
    )

    mrc.main()

    written = s3_io.read_json("models/training-runs/test/execute.json")
    assert written["promoted"] == {"rental": True}
    assert written["target_models"] == ["rental"]


def test_main_execute_skips_redundant_performance_check(monkeypatch):
    """상위 오케스트레이터가 성능 점검을 끝냈으면 지정 모델을 바로 재학습한다."""
    monkeypatch.setattr(
        mrc,
        "_check_all_models_distributed",
        lambda *args, **kwargs: pytest.fail("성능 점검을 다시 실행하면 안 됩니다"),
    )
    monkeypatch.setattr(mrc, "_load_baseline_metrics", lambda model_name: None)
    attempted = []
    monkeypatch.setattr(
        mrc,
        "_attempt_promotion",
        lambda model_name, *args, **kwargs: attempted.append(model_name) or True,
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "monthly_retrain_check",
            "--execute",
            "--performance-already-checked",
            "--models",
            "rental",
            "--profile-name",
            "builtin-default",
        ],
    )

    mrc.main()

    assert attempted == ["rental"]


def test_candidate_profiles_model_specific_filtering(monkeypatch):
    """대여 모델은 rental_* 프로필을 우선하고 return_*을 제외하며, 반납 모델은 반대로 동작한다."""
    mock_profiles = [
        "rental_embargo45",
        "return_fast_lgb",
        "general_profile",
    ]
    monkeypatch.setattr(common_config, "list_profile_names", lambda: mock_profiles)
    monkeypatch.setattr(mrc, "_champion_profile_name", lambda _: "champ_prof")

    rental_candidates = [name for name, _ in mrc._candidate_profiles("rental")]
    # 대여: return_fast_lgb는 제외되고 rental_embargo45가 우선순위에 위치
    assert "return_fast_lgb" not in rental_candidates
    assert "rental_embargo45" in rental_candidates
    assert "general_profile" in rental_candidates
    assert rental_candidates[0] == "champ_prof"
    assert rental_candidates[1] == "rental_embargo45"

    return_candidates = [name for name, _ in mrc._candidate_profiles("return")]
    # 반납: rental_embargo45는 제외되고 return_fast_lgb가 우선순위에 위치
    assert "rental_embargo45" not in return_candidates
    assert "return_fast_lgb" in return_candidates
    assert "general_profile" in return_candidates
    assert return_candidates[0] == "champ_prof"
    assert return_candidates[1] == "return_fast_lgb"


def test_get_lgb_params_model_specific_overrides():
    """LGB_PARAMS_RENTAL 및 LGB_PARAMS_RETURN이 LGB_PARAMS_COMMON 위에 올바르게 병합된다."""
    custom_profile = {
        "LGB_PARAMS_COMMON": {"num_leaves": 63, "learning_rate": 0.05},
        "LGB_PARAMS_RENTAL": {"learning_rate": 0.02, "feature_fraction": 0.7},
        "LGB_PARAMS_RETURN": {"num_leaves": 31, "min_data_in_leaf": 50},
    }

    rental_params = common_config.get_lgb_params("rental", profile=custom_profile)
    assert rental_params["num_leaves"] == 63
    assert rental_params["learning_rate"] == 0.02
    assert rental_params["feature_fraction"] == 0.7

    return_params = common_config.get_lgb_params("return", profile=custom_profile)
    assert return_params["num_leaves"] == 31
    assert return_params["learning_rate"] == 0.05
    assert return_params["min_data_in_leaf"] == 50
