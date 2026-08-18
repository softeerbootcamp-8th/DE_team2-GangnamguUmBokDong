"""ml_core.paths의 챔피언 포인터(champion_pointer_key/read_champion_prefix/
write_champion_pointer)를 검증한다.

핵심 주장(원자적 승격 설계의 근거)은 `test_read_champion_prefix_is_cached_within_process_even_after_repromotion`
— 한 프로세스가 archive_prefix를 한 번 읽고 나면, 그 사이 승격이 일어나
포인터가 바뀌어도 그 프로세스는 계속 처음 읽은 값을 본다는 것이다. 이게 없으면
`ml_core.scoring`의 `load_boosters()`/`load_conformal_correction()`과
`ml_core.model_contract`의 `load_station_dtype()`이 한 프로세스 안에서도 서로
다른 시점에 서로 다른 archive_prefix를 읽어 booster/station_categories/
conformal_correction이 섞일 수 있다.
"""

import pytest

from ml_core.paths import read_champion_prefix, write_champion_pointer


@pytest.fixture(autouse=True)
def _clear_champion_prefix_cache():
    """moto가 매 테스트마다 S3는 새로 비워주지만, `@cache`는 파이썬 프로세스
    레벨이라 테스트 사이에도 안 비워진다 — 명시적으로 매번 지운다."""
    read_champion_prefix.cache_clear()
    yield
    read_champion_prefix.cache_clear()


def test_read_champion_prefix_raises_when_never_promoted():
    with pytest.raises(FileNotFoundError, match="챔피언 포인터 없음"):
        read_champion_prefix("rental")


def test_write_then_read_champion_prefix_round_trips():
    write_champion_pointer("rental", "models/archive/dt=2026-08-18/default")
    assert read_champion_prefix("rental") == "models/archive/dt=2026-08-18/default"


def test_champion_pointer_is_per_model_name():
    write_champion_pointer("rental", "models/archive/dt=2026-08-18/default")
    write_champion_pointer("return", "models/archive/dt=2026-08-17/default")

    assert read_champion_prefix("rental") == "models/archive/dt=2026-08-18/default"
    assert read_champion_prefix("return") == "models/archive/dt=2026-08-17/default"


def test_read_champion_prefix_is_cached_within_process_even_after_repromotion():
    write_champion_pointer("rental", "models/archive/dt=2026-08-17/default")
    first = read_champion_prefix("rental")

    # "승격"이 일어나 포인터가 새 archive_prefix로 바뀐다.
    write_champion_pointer("rental", "models/archive/dt=2026-08-18/default")

    # 이 프로세스는 여전히 처음 캐시된 값만 봐야 한다.
    assert read_champion_prefix("rental") == first == "models/archive/dt=2026-08-17/default"


def test_read_champion_prefix_sees_new_pointer_after_cache_clear():
    """cache_clear() 이후(=새 프로세스를 흉내냄)에는 최신 포인터를 봐야 한다."""
    write_champion_pointer("rental", "models/archive/dt=2026-08-17/default")
    read_champion_prefix("rental")

    write_champion_pointer("rental", "models/archive/dt=2026-08-18/default")
    read_champion_prefix.cache_clear()

    assert read_champion_prefix("rental") == "models/archive/dt=2026-08-18/default"
