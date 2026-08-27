"""두 예보 YAML 기반 weather_grid seed의 exact 계약을 검증한다."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest
from core.gold_publication import parse_canonical_json
from core.gold_publication.errors import ContractViolation
from gold.weather_grid import (
    EXPECTED_WEATHER_GRID_COUNT,
    WEATHER_SOURCE_PATHS,
    build_weather_grid_seed,
    load_weather_grid_seed,
    parse_weather_grid_seed,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
EFFECTIVE_DTTM = datetime(2026, 8, 19, tzinfo=UTC)


def _source_payloads() -> dict[str, bytes]:
    """repository의 두 예보 YAML exact bytes를 읽는다."""
    return {
        path: (REPOSITORY_ROOT / path).read_bytes() for path in WEATHER_SOURCE_PATHS
    }


def test_repository_weather_sources_build_exact_34_grid_seed() -> None:
    """두 예보 YAML이 같은 순서의 34개 중복 없는 X_Y ID를 만든다."""
    seed = load_weather_grid_seed(
        REPOSITORY_ROOT,
        seed_version="weather-grid-v1",
        effective_dttm=EFFECTIVE_DTTM,
    )

    assert len(seed.rows) == EXPECTED_WEATHER_GRID_COUNT
    assert (
        len({row.weather_grid_id for row in seed.rows}) == EXPECTED_WEATHER_GRID_COUNT
    )
    assert all(
        row.weather_grid_id == f"{row.weather_grid_x_no}_{row.weather_grid_y_no}"
        for row in seed.rows
    )
    assert parse_weather_grid_seed(seed.canonical_bytes) == seed
    document = parse_canonical_json(seed.canonical_bytes)
    assert type(document) is dict
    assert [source["byte_sha256"] for source in document["sources"]] == [
        "48a89a3d203d8afabdf37a251e152903e03aae309f5a17badee049fd27b9bc7d",
        # weather_ultra_short_forecast.yaml에 freshness_rule(thirty_minutely) 필드가
        # 추가되면서 바뀐 해시(2026-08-26, fix/weather-skip) — grid 목록/순서는
        # 그대로라 이 테스트가 검증하는 34격자 계약 자체엔 영향 없다.
        "470bfb367f191ba6e4ffdd81f919340d376e0de7f8df11282ddd0248a38c3087",
    ]


def test_grid_list_mismatch_between_sources_is_rejected() -> None:
    """source 하나의 grid가 달라지면 공통 seed를 게시하지 않는다."""
    payloads = _source_payloads()
    target = WEATHER_SOURCE_PATHS[1]
    payloads[target] = payloads[target].replace(b"  - [63, 127]", b"  - [64, 127]")

    with pytest.raises(ContractViolation, match="정확히 같아야"):
        build_weather_grid_seed(
            payloads,
            seed_version="weather-grid-v1",
            effective_dttm=EFFECTIVE_DTTM,
        )


def test_duplicate_grid_is_rejected_before_cross_source_comparison() -> None:
    """source 내부 중복 grid를 row count가 같아도 거부한다."""
    payloads = _source_payloads()
    target = WEATHER_SOURCE_PATHS[0]
    payloads[target] = payloads[target].replace(b"  - [63, 127]", b"  - [63, 126]")

    with pytest.raises(ContractViolation, match="중복"):
        build_weather_grid_seed(
            payloads,
            seed_version="weather-grid-v1",
            effective_dttm=EFFECTIVE_DTTM,
        )


def test_embedded_source_checksum_tamper_is_rejected() -> None:
    """canonical seed 안의 내장 YAML이나 checksum을 독립 변조할 수 없다."""
    seed = build_weather_grid_seed(
        _source_payloads(),
        seed_version="weather-grid-v1",
        effective_dttm=EFFECTIVE_DTTM,
    )
    tampered = seed.canonical_bytes.replace(b'"byte_sha256":"', b'"byte_sha256":"0', 1)

    with pytest.raises(ContractViolation):
        parse_weather_grid_seed(tampered)
