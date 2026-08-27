"""dispatch-center-v3 seed의 Point·관리권역·source hash 계약을 검증한다."""

from __future__ import annotations

from collections import Counter
from pathlib import Path

import pytest
from core.gold_publication import sha256_hex
from core.gold_publication.errors import ContractViolation
from gold.dispatch_center import (
    EXPECTED_DISPATCH_CENTER_COUNT,
    load_dispatch_center_seed,
    parse_dispatch_center_seed,
)

REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
SEED_PATH = REPOSITORY_ROOT / "docs/gold/dispatch-center-seed.yaml"


def test_repository_dispatch_seed_is_exact_11_with_valid_points() -> None:
    """SSOT YAML이 11개 고정 ID와 DDL box 안의 SRID 4326 Point를 만든다."""
    seed = load_dispatch_center_seed(REPOSITORY_ROOT)

    assert len(seed.rows) == EXPECTED_DISPATCH_CENTER_COUNT
    assert all(126.5 <= row.longitude <= 127.5 for row in seed.rows)
    assert all(37.0 <= row.latitude <= 38.0 for row in seed.rows)
    assert Counter(row.location_accuracy_cd for row in seed.rows) == Counter(
        {"landmark_approximation": 11}
    )
    assert all(row.location_verified_dt is not None for row in seed.rows)
    assert sha256_hex(seed.yaml_bytes) == (
        "853d48c964f520a33c67a4016d1ba54dc045320675e379c4f7a6b1bc896ba733"
    )
    assert seed.source_file_sha256 == (
        "d23a54c185e61f4b5248ddda1128bba2cc3184799859f3da7a9de732a9b9f796"
    )


def test_seed_source_file_hash_is_verified_against_repository(tmp_path: Path) -> None:
    """seed가 가리키는 regions.py bytes가 hash와 다르면 준비를 거부한다."""
    seed_payload = SEED_PATH.read_bytes()
    seed_path = tmp_path / "docs/gold/dispatch-center-seed.yaml"
    source_path = tmp_path / "libs/core/src/core/regions.py"
    seed_path.parent.mkdir(parents=True)
    source_path.parent.mkdir(parents=True)
    seed_path.write_bytes(seed_payload)
    source_path.write_bytes(b"tampered")

    with pytest.raises(ContractViolation, match="source file hash"):
        load_dispatch_center_seed(tmp_path)


def test_duplicate_center_id_is_rejected() -> None:
    """11개 count를 유지해도 center ID 중복과 SSOT ID 누락을 거부한다."""
    payload = SEED_PATH.read_bytes().replace(
        b"dispatch_center_id: hangnyeoul",
        b"dispatch_center_id: cheonho",
    )

    with pytest.raises(ContractViolation, match="ID 집합"):
        parse_dispatch_center_seed(payload)


def test_out_of_box_center_point_is_rejected() -> None:
    """seed Point가 DDL safety box 밖이면 output artifact를 만들지 않는다."""
    payload = SEED_PATH.read_bytes().replace(
        b"longitude: 126.897273034482", b"longitude: 125.0"
    )

    with pytest.raises(ContractViolation, match="safety box"):
        parse_dispatch_center_seed(payload)
