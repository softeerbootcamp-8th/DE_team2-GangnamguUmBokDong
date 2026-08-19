"""monthly_retrain_check.py의 프로필 재시도 순서(_candidate_profiles())를 검증한다.

**회귀 배경**: S3 `profiles/` 목록이 비어 있거나 조회에 실패하면
`common_config.list_profile_names()`가 `[]`를 반환하는데, 예전 `_candidate_profiles()`는
"PROFILE_NAME이 이미 목록에 있을 때만 맨 앞으로 재배치"할 뿐 없으면 추가하지
않아서, 이 경우 후보가 아예 0개가 되어 `--execute`로 실행해도 재학습 시도 자체가
0번으로 조용히 끝났다. 지금은 목록 여부와 무관하게 항상 최소 1개(챔피언이 학습됐던
프로필, 없으면 이 프로세스의 기본 프로필)가 후보에 들어간다.

**2026-08 재설계**: 1차 후보는 이제 임의의 "기본 프로필"이 아니라 **챔피언이 실제로
학습됐던 프로필**이다(하이퍼파라미터는 그대로, 학습기간만 최신 롤링 윈도우로 갱신) —
성능 저하 시 첫 대응은 "최신 데이터로 다시 학습"이지 하이퍼파라미터를 바꾸는 게
아니기 때문. 그 외 등록된 프로필(임베고/앵커 조합이 다른 것들)은 그 뒤에
이름순으로 시도된다.
"""

import pytest
from core import s3 as s3_io
from ml_core import common_config, scoring
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer

from training.scripts.monthly_retrain_check import (
    _candidate_profiles,
    _champion_profile_name,
)


@pytest.fixture(autouse=True)
def _clear_champion_prefix_cache():
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    yield
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()


def _promote_with_profile(model_name: str, archive_prefix: str, profile_name: str) -> None:
    """실제 승격 흐름(promotion.promote_challenger)을 거치지 않고, 이 테스트에
    필요한 최소 상태(챔피언 포인터 + profile.json)만 직접 만든다."""
    write_champion_pointer(model_name, archive_prefix)
    s3_io.write_json(model_json_key(model_name, "profile", archive_prefix), {"profile_name": profile_name})


def test_champion_profile_name_returns_none_when_no_champion_yet():
    assert _champion_profile_name("rental") is None


def test_champion_profile_name_returns_none_and_logs_when_profile_record_missing(capsys):
    write_champion_pointer("rental", "models/archive/dt=2026-01-01/some-profile")

    result = _champion_profile_name("rental")

    assert result is None
    assert "ERROR" in capsys.readouterr().err


def test_champion_profile_name_returns_saved_profile_name():
    _promote_with_profile("rental", "models/archive/dt=2026-01-01/custom-profile", "custom-profile")

    assert _champion_profile_name("rental") == "custom-profile"


def test_candidate_profiles_always_has_at_least_one_entry_when_s3_profile_list_is_empty(monkeypatch):
    # 챔피언도 없고(부트스트랩) S3 profiles/ 목록도 비어 있는 극단적인 경우 —
    # 그래도 후보가 0개면 안 된다(회귀 재현).
    monkeypatch.setattr(common_config, "PROFILE_NAME", "default")
    monkeypatch.setattr(common_config, "TRAIN_LOOKBACK_MONTHS", 12)

    candidates = _candidate_profiles("rental")

    assert candidates == [("default", {"TRAIN_LOOKBACK_MONTHS": "12"})]


def test_candidate_profiles_puts_champion_profile_first_with_period_override_then_others_sorted():
    _promote_with_profile("return", "models/archive/dt=2026-01-01/b-profile", "b-profile")
    for name in ("c-profile", "a-profile", "b-profile"):
        s3_io.write_json(f"profiles/{name}.json", {"dummy": True})

    candidates = _candidate_profiles("return")

    assert candidates[0] == ("b-profile", {"TRAIN_LOOKBACK_MONTHS": str(common_config.TRAIN_LOOKBACK_MONTHS)})
    # 챔피언 프로필(b-profile)은 1차 시도에서 이미 다뤘으니 "나머지" 목록에서는 빠지고,
    # 나머지는 학습기간 override 없이(빈 dict) 이름순으로 온다.
    assert candidates[1:] == [("a-profile", {}), ("c-profile", {})]


def test_attempt_promotion_uses_a_unique_archive_prefix_each_call(monkeypatch):
    """회귀 재현 — 예전엔 archive_date가 순수 날짜(`today_kst().isoformat()`)라
    같은 날 두 번 --execute하면(수동 재실행, 부분 실패 후 재시도 등) 두 시도가
    정확히 같은 archive_prefix를 다시 써서, 이미 승격된 라이브 챔피언 아티팩트를
    비원자적으로 덮어쓸 수 있었다(리뷰 지적). 두 번의 `_attempt_promotion()` 호출이
    서로 다른 archive_date(그래서 다른 archive_prefix)를 쓰는지 확인한다."""
    from training.scripts import monthly_retrain_check as mrc

    monkeypatch.setattr(mrc, "_candidate_profiles", lambda model_name: [("default", {})])
    monkeypatch.setattr(mrc, "_trigger_feature_pipeline", lambda profile_name, env_overrides: None)
    monkeypatch.setattr(mrc, "should_promote", lambda challenger, champion: (False, ["기준 미달"]))

    used_archive_dates = []

    def _fake_run_training_subprocess(model_name, profile_name, archive_date, env_overrides):
        used_archive_dates.append(archive_date)
        return {"poisson_deviance_test": 0.5, "p10_p90_coverage_calibrated_test": 0.8}

    monkeypatch.setattr(mrc, "_run_training_subprocess", _fake_run_training_subprocess)

    mrc._attempt_promotion("rental", None)
    mrc._attempt_promotion("rental", None)

    assert len(used_archive_dates) == 2
    assert used_archive_dates[0] != used_archive_dates[1]
