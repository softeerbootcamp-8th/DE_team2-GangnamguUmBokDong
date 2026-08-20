"""ml_core.scoring/model_contract이 챔피언 포인터를 통해 archive_prefix를
실제로 공유하는지 검증한다 — `read_champion_prefix()`의 프로세스 캐시를 두
모듈이 같은 함수 객체로 나눠 쓰는 게 원자적 승격 설계의 핵심이라, 배선이
맞는지(따로 캐시되는 게 아닌지) 직접 확인한다.
"""

import pytest
from core import s3 as s3_io

from ml_core import common_config, model_contract, scoring
from ml_core.paths import model_json_key, read_champion_prefix, write_champion_pointer
from ml_core.serving_contract import ServingProfileContractError


@pytest.fixture(autouse=True)
def _clear_caches():
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()
    yield
    read_champion_prefix.cache_clear()
    scoring.load_boosters.cache_clear()
    scoring.load_conformal_correction.cache_clear()
    scoring.validate_champion_serving_contract.cache_clear()


def _write_profile(model_name: str, archive_prefix: str, profile: dict | None = None) -> dict:
    """테스트 아카이브에 effective profile을 저장하고 그 값을 반환한다."""
    payload = profile or common_config.effective_profile()
    s3_io.write_json(model_json_key(model_name, "profile", archive_prefix), payload)
    return payload


def test_load_conformal_correction_and_station_dtype_use_same_cached_archive_prefix():
    """scoring.load_conformal_correction()이 먼저 read_champion_prefix()를 캐시해두면,
    그 뒤 승격이 일어나 포인터가 바뀌어도 model_contract.load_station_dtype()은
    (같은 프로세스 안이므로) 여전히 처음 캐시된 옛 archive를 읽어야 한다 — 서로
    다른 archive에서 온 correction/station_categories가 섞이면 안 된다."""
    old_prefix = "models/archive/dt=2026-08-17/default"
    write_champion_pointer("rental", old_prefix)
    s3_io.write_json(f"{old_prefix}/rental_conformal_correction.json", {"correction": 1.5})
    s3_io.write_json(f"{old_prefix}/rental_station_categories.json", ["ST-1", "ST-2"])

    # correction을 먼저 읽어서 read_champion_prefix 캐시를 채운다.
    assert scoring.load_conformal_correction("rental") == 1.5

    # 그 사이 승격이 일어나 포인터가 새 archive로 바뀌었다고 가정.
    new_prefix = "models/archive/dt=2026-08-18/default"
    s3_io.write_json(f"{new_prefix}/rental_station_categories.json", ["ST-9"])
    write_champion_pointer("rental", new_prefix)

    # 이 프로세스는 station_categories를 읽을 때도 여전히 처음 캐시된
    # archive_prefix(dt=08-17)를 써야 한다 — 새 archive(dt=08-18)의 ["ST-9"]가
    # 아니라 옛 archive의 ["ST-1", "ST-2"]가 나와야 섞이지 않은 것이다.
    dtype = model_contract.load_station_dtype("rental")
    assert list(dtype.categories) == ["ST-1", "ST-2"]


def test_load_station_dtype_with_explicit_prefix_bypasses_champion_pointer():
    """models_prefix를 명시적으로 주면(실험/스윕 등) 챔피언 포인터를 아예 안 본다."""
    experiment_prefix = "models/experiments/run-1"
    s3_io.write_json(f"{experiment_prefix}/rental_station_categories.json", ["ST-A"])

    dtype = model_contract.load_station_dtype("rental", models_prefix=experiment_prefix)

    assert list(dtype.categories) == ["ST-A"]


def test_load_boosters_fails_closed_before_download_when_champion_profile_differs(monkeypatch):
    """현재 feature 계산과 챔피언 학습 계약이 다르면 booster를 읽기 전에 실패한다."""
    archive_prefix = "models/archive/dt=2026-08-18/incompatible"
    write_champion_pointer("rental", archive_prefix)
    profile = common_config.effective_profile()
    profile["ROLLING_WINDOW_MINUTES"] += 5
    _write_profile("rental", archive_prefix, profile)

    downloads = []
    monkeypatch.setattr(scoring.model_io, "download_and_load_booster", downloads.append)

    with pytest.raises(ServingProfileContractError, match="ROLLING_WINDOW_MINUTES"):
        scoring.load_boosters("rental")

    assert downloads == []


def test_load_boosters_allows_training_only_profile_differences(monkeypatch):
    """학습 기간과 LGB 파라미터 차이는 서빙 feature 의미를 바꾸지 않는다."""
    archive_prefix = "models/archive/dt=2026-08-18/training-tuned"
    write_champion_pointer("return", archive_prefix)
    profile = common_config.effective_profile()
    profile["TRAIN_LOOKBACK_MONTHS"] = 3
    profile["LGB_PARAMS_COMMON"] = {**profile["LGB_PARAMS_COMMON"], "num_leaves": 127}
    _write_profile("return", archive_prefix, profile)

    monkeypatch.setattr(scoring.model_io, "download_and_load_booster", lambda key: key)

    boosters = scoring.load_boosters("return")

    assert set(boosters) == set(scoring.BOOSTER_SUFFIXES)


def test_validate_champion_contract_fails_when_profile_artifact_is_missing():
    """기존 모델이라도 계약을 증명할 profile.json이 없으면 추론하지 않는다."""
    write_champion_pointer("rental", "models/archive/dt=2026-08-18/no-profile")

    with pytest.raises(ServingProfileContractError, match="effective profile이 없습니다"):
        scoring.validate_champion_serving_contract("rental")
