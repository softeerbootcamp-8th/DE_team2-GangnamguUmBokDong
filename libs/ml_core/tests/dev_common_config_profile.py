"""common_config.py의 S3 기반 프로필 조회(`_load_profile()`/`_fetch_profile_from_s3()`)와
학습기간 롤링 윈도우 계산(`training_window()`/`_subtract_months()`)을 검증한다.

`common_config.PROFILE`은 모듈 import 시점(테스트 수집 시점, 아직 moto가 활성화되기
전)에 이미 한 번 계산돼 굳어 있으므로, 이 테스트들은 그 캐시된 값이 아니라 함수
자체를 다시 호출해서 검증한다(conftest.py의 `_bucket` fixture가 각 테스트마다 새
가짜 S3를 만들어준 뒤에).
"""

from datetime import date

import pytest
from core import s3 as s3_io

from ml_core import common_config, profile_registry


def test_fetch_profile_from_s3_returns_none_when_missing():
    assert common_config._fetch_profile_from_s3("does-not-exist") is None


def test_fetch_profile_from_s3_returns_written_value():
    s3_io.write_json("profiles/custom.json", {"ROLLING_EMBARGO_MINUTES": 999})
    assert common_config._fetch_profile_from_s3("custom") == {"ROLLING_EMBARGO_MINUTES": 999}


def test_load_profile_falls_back_to_embedded_default_when_s3_empty(monkeypatch):
    monkeypatch.setenv("ML_PROFILE", "nonexistent")
    assert common_config._load_profile() == common_config._DEFAULT_PROFILE


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


def test_load_profile_falls_back_when_s3_unreachable(monkeypatch):
    # moto가 목킹하는 건 boto3 호출 자체라, 잘못된 엔드포인트로 돌리면 moto 밖으로
    # 나가 실제 네트워크 에러가 난다 — _load_profile()이 이 경우도 예외를 삼키고
    # 폴백하는지 확인한다(S3 연결 자체가 안 되는 극단적인 경우).
    monkeypatch.setenv("S3_ENDPOINT_URL", "http://localhost:0")
    assert common_config._load_profile() == common_config._DEFAULT_PROFILE


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
    monkeypatch.setattr(common_config, "TRAIN_LOOKBACK_MONTHS", 6)
    monkeypatch.setattr(common_config, "TRAINING_SAFETY_MARGIN_DAYS", 7)

    start, end = common_config.training_window(as_of=date(2026, 8, 19))

    assert end == date(2026, 8, 12)  # 8/19 - 7일
    assert start == date(2026, 2, 12)  # end - 6개월


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
