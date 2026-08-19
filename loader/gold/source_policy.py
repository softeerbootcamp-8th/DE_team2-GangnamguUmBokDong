"""Gold publisher가 허용하는 checked-in source 수집 계획을 검증한다."""

from __future__ import annotations

import re
from itertools import pairwise
from pathlib import Path
from typing import Any

from core.gold_publication import sha256_hex
from core.gold_publication.errors import ContractViolation
from core.source_snapshot import SourceSnapshotManifest, SourceSnapshotStatus

from .common import parse_yaml_mapping

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]
_SOURCE_CONFIG_PATHS = {
    "bike_station_master": "collector/sources/bike_station_master.yaml",
    "bike_station_realtime": "collector/sources/bike_station_realtime.yaml",
    "cultural_event": "collector/sources/cultural_event.yaml",
    "performance_event": "collector/sources/performance_event.yaml",
    "weather_short_term_forecast": (
        "collector/sources/weather_short_term_forecast.yaml"
    ),
    "weather_ultra_short_forecast": (
        "collector/sources/weather_ultra_short_forecast.yaml"
    ),
}
_PAGE_PART = re.compile(r"page-([0-9]{5})-([0-9]{5})\Z")


def validate_source_snapshot_policy(
    manifest: SourceSnapshotManifest,
    *,
    repository_root: Path = _REPOSITORY_ROOT,
) -> None:
    """manifest가 배포 코드와 같은 source config·전체 part 계획인지 확인한다."""
    if type(manifest) is not SourceSnapshotManifest:
        raise ContractViolation("source policy manifest 타입이 잘못됐습니다.")
    if not isinstance(repository_root, Path):
        raise ContractViolation("repository_root는 Path여야 합니다.")
    try:
        relative_path = _SOURCE_CONFIG_PATHS[manifest.source_id]
    except KeyError as exc:
        raise ContractViolation(
            f"Gold source policy에 등록되지 않은 source입니다: {manifest.source_id}"
        ) from exc
    config_bytes = (repository_root / relative_path).read_bytes()
    expected_version = f"sha256:{sha256_hex(config_bytes)}"
    if manifest.config_version != expected_version:
        raise ContractViolation(
            "source snapshot config_version이 배포된 source config와 다릅니다."
        )
    config = parse_yaml_mapping(config_bytes)
    if config.get("source_id") != manifest.source_id:
        raise ContractViolation("source config 파일과 manifest source_id가 다릅니다.")
    quality = config.get("quality")
    if type(quality) is not dict or type(quality.get("allow_empty")) is not bool:
        raise ContractViolation("source config quality.allow_empty가 bool이 아닙니다.")
    if (
        manifest.status is SourceSnapshotStatus.EMPTY
        and quality["allow_empty"] is False
    ):
        raise ContractViolation("source config가 허용하지 않는 EMPTY snapshot입니다.")
    adapter = config.get("adapter")
    params = config.get("adapter_params")
    if type(params) is not dict:
        raise ContractViolation("source config adapter_params가 mapping이 아닙니다.")
    if adapter == "kma_apihub":
        _validate_kma_plan(manifest, params)
        return
    if adapter == "seoul_openapi":
        _validate_seoul_plan(manifest, params)
        return
    raise ContractViolation("Gold source가 허용하지 않는 collector adapter입니다.")


def _validate_kma_plan(
    manifest: SourceSnapshotManifest,
    params: dict[str, Any],
) -> None:
    """KMA manifest가 config의 exact 34-grid 계획을 완료했는지 검증한다."""
    grids = params.get("grids")
    if type(grids) is not list or len(grids) != 34:
        raise ContractViolation("KMA source config는 정확히 34개 grid여야 합니다.")
    keys: list[str] = []
    for grid in grids:
        if (
            type(grid) is not list
            or len(grid) != 2
            or type(grid[0]) is not int
            or type(grid[1]) is not int
        ):
            raise ContractViolation("KMA source grid는 integer 두 개여야 합니다.")
        keys.append(f"grid-{grid[0]:03d}x{grid[1]:03d}")
    expected = tuple(sorted(keys))
    if len(set(expected)) != 34 or manifest.planned_parts != expected:
        raise ContractViolation("KMA source manifest가 exact 34-grid 계획과 다릅니다.")


def _validate_seoul_plan(
    manifest: SourceSnapshotManifest,
    params: dict[str, Any],
) -> None:
    """서울 API manifest의 page 계획이 config page size와 연속인지 검증한다."""
    page_size = params.get("page_size")
    if type(page_size) is not int or page_size <= 0:
        raise ContractViolation("서울 API source page_size가 양의 integer가 아닙니다.")
    ranges: list[tuple[int, int]] = []
    for part in manifest.planned_parts:
        match = _PAGE_PART.fullmatch(part)
        if match is None:
            raise ContractViolation("서울 API source part key 형식이 잘못됐습니다.")
        start, end = int(match.group(1)), int(match.group(2))
        if start <= 0 or end < start or end - start + 1 > page_size:
            raise ContractViolation("서울 API source page 범위가 config와 다릅니다.")
        ranges.append((start, end))
    if not ranges or ranges[0][0] != 1:
        raise ContractViolation("서울 API source page 계획은 1부터 시작해야 합니다.")
    if any(current[0] != previous[1] + 1 for previous, current in pairwise(ranges)):
        raise ContractViolation("서울 API source page 계획이 연속적이지 않습니다.")
    if params.get("pagination") == "probe_until_empty":
        if any(end - start + 1 != page_size for start, end in ranges):
            raise ContractViolation(
                "probe source page 범위는 모두 config page_size여야 합니다."
            )
        minimum_parts = 1 if manifest.status is SourceSnapshotStatus.EMPTY else 2
        if len(ranges) < minimum_parts:
            raise ContractViolation(
                "probe source manifest에 종료 sentinel 증거가 없습니다."
            )
