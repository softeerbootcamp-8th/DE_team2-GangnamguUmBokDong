"""train_rental_model.py/train_return_model.py의 MODEL_ARCHIVE_DATE 기본값을 검증한다.

**회귀 배경**: 두 스크립트 모두 `MODEL_ARCHIVE_DATE`가 안 정해져 있으면(수동 실행,
`monthly_retrain_check.py`를 거치지 않은 직접 트리거 등) 예전엔 순수 오늘 날짜
(`today_kst().isoformat()`)를 기본값으로 썼다 — `archive_models_prefix()`는
date+profile_name만으로 경로를 만들기 때문에, 같은 날 같은 프로필로 이 스크립트를
두 번 실행하면(사람이 재시도하는 경우 등) archive_prefix가 겹쳐서 이미 챔피언
포인터가 가리키는 아티팩트를 비원자적으로 덮어쓸 수 있었다(리뷰 지적).
`monthly_retrain_check.py`의 자동 재시도 경로만 `config.unique_archive_date()`로
막혀 있었고, 실제 운영 엔트리포인트인 이 두 스크립트 자체의 기본값은 안 막혀
있었다 — 이제 둘 다 같은 `unique_archive_date()`를 기본값으로 쓴다.
"""

import pytest


@pytest.fixture(autouse=True)
def _clear_env(monkeypatch):
    monkeypatch.delenv("MODEL_ARCHIVE_DATE", raising=False)


def _fake_train_target(*, calls):
    def _train_target(target_col, model_name, models_prefix, exposure_col=None):
        calls.append(models_prefix)
        return {"poisson_deviance_test": 1.0}

    return _train_target


def test_rental_model_uses_a_unique_archive_prefix_each_run_without_env_override(monkeypatch):
    from training import train_rental_model as trm

    calls: list[str] = []
    monkeypatch.setattr(trm, "train_target", _fake_train_target(calls=calls))

    trm.main()
    trm.main()

    assert len(calls) == 2
    assert calls[0] != calls[1]


def test_return_model_uses_a_unique_archive_prefix_each_run_without_env_override(monkeypatch):
    from training import train_return_model as trm

    calls: list[str] = []
    monkeypatch.setattr(trm, "train_target", _fake_train_target(calls=calls))

    trm.main()
    trm.main()

    assert len(calls) == 2
    assert calls[0] != calls[1]


def test_rental_model_still_honors_explicit_model_archive_date_env(monkeypatch):
    """MODEL_ARCHIVE_DATE를 명시하면(monthly_retrain_check.py의 subprocess 경로처럼)
    그 값을 그대로 써야 한다 — 이 회귀 수정이 명시적 override까지 덮어써서는 안 됨."""
    from training import train_rental_model as trm

    calls: list[str] = []
    monkeypatch.setattr(trm, "train_target", _fake_train_target(calls=calls))
    monkeypatch.setenv("MODEL_ARCHIVE_DATE", "2026-08-19-fixedrun")

    trm.main()

    assert len(calls) == 1
    assert "2026-08-19-fixedrun" in calls[0]
