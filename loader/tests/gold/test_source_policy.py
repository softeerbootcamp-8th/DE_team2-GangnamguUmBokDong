"""Gold source config version과 completeness 계획 검증을 회귀한다."""

from __future__ import annotations

from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest
from core.gold_publication import ContractViolation, sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from gold.source_policy import validate_source_snapshot_policy

_ROOT = Path(__file__).resolve().parents[3]


def _config_version(path: str) -> str:
    """checked-in source config의 collector config_version을 반환한다."""
    return f"sha256:{sha256_hex((_ROOT / path).read_bytes())}"


def test_realtime_policy_requires_config_version_and_terminal_probe() -> None:
    """bike probe가 배포 config와 연속 nonempty+sentinel page를 증명해야 한다."""
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_realtime",
        logical_dttm=datetime(2026, 8, 20, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=_config_version("collector/sources/bike_station_realtime.yaml"),
        silver_uri=(
            "s3://fixture/source_snapshot_silver/bike_station_realtime/"
            f"sha256={'1' * 64}.parquet"
        ),
        silver_byte_sha256="1" * 64,
        counts=SourceSnapshotCounts(1, 1, 1, 0, 0),
        planned_parts=("page-00001-01000", "page-01001-02000"),
        completed_parts=("page-00001-01000", "page-01001-02000"),
    )

    validate_source_snapshot_policy(manifest)

    with pytest.raises(ContractViolation, match="sentinel"):
        validate_source_snapshot_policy(
            replace(
                manifest,
                planned_parts=("page-00001-01000",),
                completed_parts=("page-00001-01000",),
            )
        )
    with pytest.raises(ContractViolation, match="config_version"):
        validate_source_snapshot_policy(replace(manifest, config_version="old"))


def test_weather_policy_requires_exact_checked_in_grid_plan() -> None:
    """weather manifest가 배포 YAML의 서로 다른 34-grid 전체를 증명한다."""
    path = "collector/sources/weather_short_term_forecast.yaml"
    import yaml

    document = yaml.safe_load((_ROOT / path).read_bytes())
    parts = tuple(
        sorted(
            f"grid-{grid[0]:03d}x{grid[1]:03d}"
            for grid in document["adapter_params"]["grids"]
        )
    )
    manifest = build_source_snapshot_manifest(
        source_id="weather_short_term_forecast",
        logical_dttm=datetime(2026, 8, 20, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version=_config_version(path),
        silver_uri=(
            "s3://fixture/source_snapshot_silver/weather_short_term_forecast/"
            f"sha256={'2' * 64}.parquet"
        ),
        silver_byte_sha256="2" * 64,
        counts=SourceSnapshotCounts(34, 34, 34, 0, 0),
        planned_parts=parts,
        completed_parts=parts,
    )

    validate_source_snapshot_policy(manifest)
    with pytest.raises(ContractViolation, match="34-grid"):
        validate_source_snapshot_policy(
            replace(
                manifest,
                planned_parts=parts[:-1],
                completed_parts=parts[:-1],
            )
        )


def test_nonempty_source_policy_rejects_canonical_empty_manifest() -> None:
    """공통 wire상 EMPTY여도 source YAML allow_empty=false면 downstream을 열지 않는다."""
    manifest = build_source_snapshot_manifest(
        source_id="bike_station_master",
        logical_dttm=datetime(2026, 8, 20, tzinfo=UTC),
        revision_no=0,
        status=SourceSnapshotStatus.EMPTY,
        config_version=_config_version("collector/sources/bike_station_master.yaml"),
        silver_uri=None,
        silver_byte_sha256=None,
        counts=SourceSnapshotCounts(0, 0, 0, 0, 0),
        planned_parts=("page-00001-01000",),
        completed_parts=("page-00001-01000",),
    )

    with pytest.raises(ContractViolation, match="허용하지 않는 EMPTY"):
        validate_source_snapshot_policy(manifest)
