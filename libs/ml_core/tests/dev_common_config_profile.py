"""common_config.py의 S3 기반 프로필 조회(`_load_profile()`/`_fetch_profile_from_s3()`)와
학습기간 롤링 윈도우 계산(`training_window()`/`_subtract_months()`)을 검증한다.

`common_config.PROFILE`은 모듈 import 시점(테스트 수집 시점, 아직 moto가 활성화되기
전)에 이미 한 번 계산돼 굳어 있으므로, 이 테스트들은 그 캐시된 값이 아니라 함수
자체를 다시 호출해서 검증한다(conftest.py의 `_bucket` fixture가 각 테스트마다 새
가짜 S3를 만들어준 뒤에).
"""

import json
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import pytest
from core import s3 as s3_io

from ml_core import common_config, profile_contract, profile_registry

_REPO_ROOT = Path(__file__).resolve().parents[3]


def _fresh_process_env(**overrides: str) -> dict[str, str]:
    """현재 worktree의 ml_core/core를 fresh subprocess가 import할 환경을 만든다."""
    python_paths = [
        str(_REPO_ROOT / "ml"),
        str(_REPO_ROOT / "libs"),
        str(_REPO_ROOT / "libs" / "core" / "src"),
    ]
    if existing := os.environ.get("PYTHONPATH"):
        python_paths.append(existing)
    return {**os.environ, "PYTHONPATH": os.pathsep.join(python_paths), **overrides}


def test_fetch_profile_from_s3_returns_none_when_missing():
    assert common_config._fetch_profile_from_s3("does-not-exist") is None


def test_fetch_profile_from_s3_returns_written_value():
    s3_io.write_json("profiles/custom.json", {"ROLLING_EMBARGO_MINUTES": 999})
    assert common_config._fetch_profile_from_s3("custom") == {"ROLLING_EMBARGO_MINUTES": 999}


def test_load_profile_uses_builtin_default_without_querying_s3(monkeypatch):
    """ML_PROFILE 미지정 기본 실행은 오래된 S3 default 객체의 영향을 받지 않아야 한다."""
    monkeypatch.delenv("ML_PROFILE", raising=False)
    monkeypatch.setattr(
        common_config,
        "_fetch_profile_from_s3",
        lambda _name: pytest.fail("내장 기본 프로필에서 S3를 조회하면 안 됩니다"),
    )

    profile = common_config._load_profile()

    assert common_config._selected_profile_name() == common_config.BUILTIN_PROFILE_NAME
    assert profile == common_config._DEFAULT_PROFILE
    assert profile["GRID_TICK_MINUTES"] == 5


def test_load_profile_does_not_let_stale_s3_default_override_implicit_builtin(monkeypatch):
    s3_io.write_json(
        "profiles/default.json",
        {**common_config._DEFAULT_PROFILE, "GRID_TICK_MINUTES": 20, "ROLLING_TICK_MINUTES": 20},
    )
    monkeypatch.delenv("ML_PROFILE", raising=False)

    assert common_config._load_profile()["GRID_TICK_MINUTES"] == 5


def test_load_profile_fails_when_explicit_s3_profile_is_missing(monkeypatch):
    monkeypatch.setenv("ML_PROFILE", "nonexistent")

    with pytest.raises(FileNotFoundError, match="nonexistent"):
        common_config._load_profile()


def test_load_profile_prefers_s3_value_over_embedded_default(monkeypatch):
    custom = {**common_config._DEFAULT_PROFILE, "ROLLING_EMBARGO_MINUTES": 12345}
    s3_io.write_json("profiles/from-s3.json", custom)
    monkeypatch.setenv("ML_PROFILE", "from-s3")
    assert common_config._load_profile()["ROLLING_EMBARGO_MINUTES"] == 12345


def test_load_profile_merges_partial_s3_profile_with_embedded_defaults(monkeypatch):
    """회귀 재현 — S3에 이번 PR 이전에 올려둔(또는 사람이 손으로 만든) 프로필처럼
    신규 키(TRAIN_LOOKBACK_MONTHS 등)가 빠진 채로 있으면, 그 키가 내장 기본값으로
    채워져야 한다. 병합 없이 그대로 반환하면 이 파일 끝의
    `_PROFILE["TRAIN_LOOKBACK_MONTHS"]`에서 KeyError가 나 전 서비스가 import
    시점에 죽는다(리뷰 지적)."""
    partial = {"ROLLING_EMBARGO_MINUTES": 999}  # TRAIN_LOOKBACK_MONTHS 등 신규 키 없음
    s3_io.write_json("profiles/partial.json", partial)
    monkeypatch.setenv("ML_PROFILE", "partial")

    profile = common_config._load_profile()

    assert profile["ROLLING_EMBARGO_MINUTES"] == 999  # S3 값이 우선
    assert profile["TRAIN_LOOKBACK_MONTHS"] == common_config._DEFAULT_PROFILE["TRAIN_LOOKBACK_MONTHS"]  # 누락분은 기본값


def test_future_lgb_profile_keys_survive_effective_params():
    """max_bin 같은 새 키가 profile 병합과 실제 LightGBM params 생성에서 보존돼야 한다."""
    profile = profile_contract.merge_and_validate_profile(
        {"LGB_PARAMS_COMMON": {"max_bin": 31}},
        "future-lgb-key",
    )

    params = common_config._build_lgb_params(profile["LGB_PARAMS_COMMON"])

    assert params["max_bin"] == 31
    assert params["num_leaves"] == common_config._DEFAULT_PROFILE["LGB_PARAMS_COMMON"]["num_leaves"]


def test_load_profile_propagates_explicit_s3_failure(monkeypatch):
    monkeypatch.setenv("ML_PROFILE", "remote")

    def _raise(_name):
        raise RuntimeError("S3 unavailable")

    monkeypatch.setattr(common_config, "_fetch_profile_from_s3", _raise)

    with pytest.raises(RuntimeError, match="S3 unavailable"):
        common_config._load_profile()


def test_load_profile_rejects_non_five_minute_profile(monkeypatch):
    invalid = {**common_config._DEFAULT_PROFILE, "GRID_TICK_MINUTES": 20, "ROLLING_TICK_MINUTES": 20}
    s3_io.write_json("profiles/old-20min.json", invalid)
    monkeypatch.setenv("ML_PROFILE", "old-20min")

    with pytest.raises(ValueError, match=r"운영 계약\(5분\)"):
        common_config._load_profile()


def test_load_profile_rejects_reserved_profile_name_metadata(monkeypatch):
    s3_io.write_json(
        "profiles/forged-name.json",
        {**common_config._DEFAULT_PROFILE, "profile_name": "different-name"},
    )
    monkeypatch.setenv("ML_PROFILE", "forged-name")

    with pytest.raises(ValueError, match="예약 메타데이터 키 profile_name"):
        common_config._load_profile()


def test_selected_profile_name_rejects_empty_explicit_value(monkeypatch):
    monkeypatch.setenv("ML_PROFILE", "   ")

    with pytest.raises(ValueError, match="빈 문자열"):
        common_config._selected_profile_name()


def test_effective_profile_includes_environment_overrides_in_fresh_process():
    """모델에 저장할 snapshot은 프로필 원문이 아니라 실제 env override를 반영해야 한다."""
    env = _fresh_process_env(
        ROLLING_EMBARGO_MINUTES="55",
        LGB_NUM_LEAVES="7",
    )
    env.pop("ML_PROFILE", None)
    code = (
        "import json; from ml_core import common_config; "
        "print(json.dumps(common_config.effective_profile()))"
    )

    result = subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )
    snapshot = json.loads(result.stdout)

    assert snapshot["ROLLING_EMBARGO_MINUTES"] == 55
    assert snapshot["LGB_PARAMS_COMMON"]["num_leaves"] == 7
    assert snapshot["GRID_TICK_MINUTES"] == 5


def test_profile_registry_import_does_not_require_valid_runtime_profile():
    """깨진 ML_PROFILE 상태에서도 관리 모듈을 import해 원격 프로필을 복구할 수 있어야 한다."""
    env = _fresh_process_env(ML_PROFILE="missing-profile-that-must-not-load")
    code = (
        "import sys; import ml_core.profile_registry; "
        "assert 'ml_core.common_config' not in sys.modules"
    )

    subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.parametrize(
    ("d", "months", "expected"),
    [
        (date(2026, 8, 19), 18, date(2025, 2, 19)),
        (date(2026, 1, 1), 1, date(2025, 12, 1)),
        (date(2026, 3, 31), 1, date(2026, 2, 28)),  # 2월엔 31일이 없음 -> 말일로 보정
        (date(2024, 3, 31), 1, date(2024, 2, 29)),  # 윤년 2월 -> 29일까지
    ],
)
def test_subtract_months_clamps_to_actual_month_end(d, months, expected):
    assert common_config._subtract_months(d, months) == expected


def test_training_window_uses_lookback_and_safety_margin(monkeypatch):
    monkeypatch.delenv("TRAIN_WINDOW_START", raising=False)
    monkeypatch.delenv("TRAIN_WINDOW_END", raising=False)
    monkeypatch.setattr(common_config, "TRAIN_LOOKBACK_MONTHS", 6)
    monkeypatch.setattr(common_config, "TRAINING_SAFETY_MARGIN_DAYS", 7)

    start, end = common_config.training_window(as_of=date(2026, 8, 19))

    assert end == date(2026, 8, 12)  # 8/19 - 7일
    assert start == date(2026, 2, 12)  # end - 6개월


def test_training_window_uses_exact_explicit_pair(monkeypatch):
    """최초 챔피언은 현재 날짜와 무관하게 2025년 전체를 정확히 선택할 수 있어야 한다."""
    monkeypatch.setenv("TRAIN_WINDOW_START", "2025-01-01")
    monkeypatch.setenv("TRAIN_WINDOW_END", "2025-12-31")

    assert common_config.training_window(as_of=date(2099, 1, 1)) == (
        date(2025, 1, 1),
        date(2025, 12, 31),
    )


def test_training_and_feature_engine_resolve_same_explicit_window_in_fresh_process():
    """두 파이프라인 설정이 공용 API에서 동일한 2025 exact window를 받아야 한다."""
    env = _fresh_process_env(
        TRAIN_WINDOW_START="2025-01-01",
        TRAIN_WINDOW_END="2025-12-31",
    )
    env.pop("ML_PROFILE", None)
    code = (
        "from training import config as training_config; "
        "from feature_engine.spark import config as feature_config; "
        "assert training_config.TRAIN_WINDOW_START.isoformat() == '2025-01-01'; "
        "assert training_config.TRAIN_WINDOW_END.isoformat() == '2025-12-31'; "
        "assert feature_config.WINDOW_START == training_config.TRAIN_WINDOW_START; "
        "assert feature_config.WINDOW_END == training_config.TRAIN_WINDOW_END"
    )

    subprocess.run(
        [sys.executable, "-c", code],
        check=True,
        capture_output=True,
        text=True,
        env=env,
    )


@pytest.mark.parametrize(
    ("start", "end", "message"),
    [
        ("2025-01-01", None, "반드시 함께"),
        (None, "2025-12-31", "반드시 함께"),
        ("2025-02-30", "2025-12-31", "YYYY-MM-DD"),
        ("20250101", "2025-12-31", "YYYY-MM-DD"),
        ("2025-12-31", "2025-01-01", "늦을 수 없습니다"),
    ],
)
def test_training_window_rejects_incomplete_invalid_or_reversed_explicit_pair(monkeypatch, start, end, message):
    """부분·오형식·역전 구간을 rolling으로 조용히 대체하면 안 된다."""
    for name, value in (("TRAIN_WINDOW_START", start), ("TRAIN_WINDOW_END", end)):
        if value is None:
            monkeypatch.delenv(name, raising=False)
        else:
            monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=message):
        common_config.training_window()


def test_profile_registry_push_fetch_list_round_trip(tmp_path, monkeypatch):
    # push_profile()은 S3 쓰기 + MLflow 기록을 같이 한다 — conftest.py의
    # _no_real_mlflow_server가 기본적으로 막아두므로, 여기서만 로컬 파일 기반
    # tracking store로 다시 덮어써서 실제 MLflow 동작까지 함께 검증한다
    # (ml/training/tests/dev_train_target_mlflow.py와 같은 패턴).
    monkeypatch.setenv("MLFLOW_ALLOW_FILE_STORE", "true")
    monkeypatch.setattr(profile_registry.mlflow_tracking, "MLFLOW_TRACKING_URI", str(tmp_path / "mlruns"))

    profile = {**common_config._DEFAULT_PROFILE, "ROLLING_EMBARGO_MINUTES": 77}

    profile_registry.push_profile("roundtrip-test", profile)

    assert profile_registry.fetch_profile("roundtrip-test")["ROLLING_EMBARGO_MINUTES"] == 77
    assert "roundtrip-test" in profile_registry.list_profiles()
    assert "roundtrip-test" in common_config.list_profile_names()


def test_profile_registry_rejects_reserved_builtin_name():
    with pytest.raises(ValueError, match="예약된 내장 프로필"):
        profile_registry.push_profile(common_config.BUILTIN_PROFILE_NAME, common_config._DEFAULT_PROFILE)
