"""학습 archive를 serving release로 연결하는 production CLI 계약을 검증한다."""

import pyarrow as pa
import pytest
from core import s3 as s3_io
from core.model_snapshot import ModelSnapshotContractError, parse_station_crosswalk
from ml_core.serving_release import (
    SERVING_RELEASE_POINTER_SCHEMA_VERSION,
    ServingReleasePointer,
)

from training import publish_serving_release as cli


def _write_station_master_part(
    key: str,
    station_ids: list[str],
    station_nos: list[int],
) -> None:
    """테스트용 station master Parquet part를 S3에 쓴다."""
    s3_io.write_parquet(
        pa.table(
            {
                "station_id": pa.array(station_ids, type=pa.string()),
                "station_no": pa.array(station_nos, type=pa.int16()),
            }
        ),
        key,
    )


def test_build_station_crosswalk_from_multipart_source_uses_exact_part_bytes():
    """Spark part 전체를 읽어 정렬된 canonical crosswalk와 source SHA를 만든다."""
    prefix = "processed_v2/station_master.parquet"
    _write_station_master_part(
        f"{prefix}/part-00001.snappy.parquet",
        ["ST-2"],
        [2],
    )
    _write_station_master_part(
        f"{prefix}/part-00000.snappy.parquet",
        ["ST-3", "ST-1"],
        [3, 1],
    )

    result = cli.build_station_crosswalk_from_spark_source(prefix)

    parsed = parse_station_crosswalk(result.crosswalk.canonical_bytes)
    assert [(entry.station_no, entry.sta_id) for entry in parsed.entries] == [
        (1, "ST-1"),
        (2, "ST-2"),
        (3, "ST-3"),
    ]
    assert result.source_object_count == 2
    assert result.source_row_count == 3
    assert len(result.source_fingerprint_sha256) == 64


def test_build_station_crosswalk_rejects_non_bijective_mapping():
    """서로 다른 station_no가 같은 sta_id를 쓰면 release 준비 전에 실패한다."""
    prefix = "processed_v2/station_master.parquet"
    _write_station_master_part(
        f"{prefix}/part-00000.snappy.parquet",
        ["ST-1", "ST-1"],
        [1, 2],
    )

    with pytest.raises(ModelSnapshotContractError, match="여러 station_no"):
        cli.build_station_crosswalk_from_spark_source(prefix)


def test_publish_pair_release_pins_crosswalk_then_passes_exact_key(monkeypatch):
    """Canonical crosswalk를 고정한 exact key만 pair publication 경계에 전달한다."""
    prefix = "processed_v2/station_master.parquet"
    _write_station_master_part(
        f"{prefix}/part-00000.snappy.parquet",
        ["ST-1", "ST-2"],
        [1, 2],
    )
    digest = "a" * 64
    pointer = ServingReleasePointer(
        schema_version=SERVING_RELEASE_POINTER_SCHEMA_VERSION,
        generation=7,
        release_manifest_byte_sha256=digest,
        release_manifest_uri=(
            "s3://test-bucket/models/serving-release/manifests/"
            f"sha256={digest}.json"
        ),
    )
    captured: dict = {}

    def _prepare(**kwargs):
        """Pair publication 호출 인자를 기록하고 검증된 pointer를 반환한다."""
        captured.update(kwargs)
        return pointer

    monkeypatch.setattr(cli, "prepare_and_promote_serving_release_pair", _prepare)

    result = cli.publish_pair_release(
        rental_archive_prefix="models/archive/dt=run/profile",
        return_archive_prefix="models/archive/dt=run/profile",
        station_profile_source_key="processed/features/station_profile.parquet",
        station_master_source_key=prefix,
    )

    crosswalk_key = captured["station_master_source_key"]
    assert crosswalk_key.startswith(
        "models/serving-release/artifacts/station_master_source/sha256="
    )
    assert crosswalk_key.endswith(".json")
    assert s3_io.get_object_bytes(crosswalk_key) is not None
    assert captured["rental_archive_prefix"] == "models/archive/dt=run/profile"
    assert captured["return_archive_prefix"] == "models/archive/dt=run/profile"
    assert captured["station_profile_source_key"] == (
        "processed/features/station_profile.parquet"
    )
    assert captured["allow_contract_change"] is False
    assert result["generation"] == 7
    assert result["station_crosswalk_source_object_count"] == 1
    assert result["station_crosswalk_source_row_count"] == 2


def test_missing_station_master_fails_before_pair_publication(monkeypatch):
    """Station source 누락 시 기존 pointer를 건드릴 publication 호출을 하지 않는다."""
    called = False

    def _prepare(**_kwargs):
        """호출 여부만 기록하는 실패 경계용 대역이다."""
        nonlocal called
        called = True
        raise AssertionError("pair publication을 호출하면 안 됩니다")

    monkeypatch.setattr(cli, "prepare_and_promote_serving_release_pair", _prepare)

    with pytest.raises(FileNotFoundError, match="station master Parquet"):
        cli.publish_pair_release(
            rental_archive_prefix="models/archive/dt=run/profile",
            return_archive_prefix="models/archive/dt=run/profile",
            station_profile_source_key="processed/features/station_profile.parquet",
            station_master_source_key="processed_v2/station_master.parquet",
        )

    assert called is False


def test_parse_args_requires_all_exact_release_inputs():
    """운영 CLI가 archive pair와 두 station dependency를 모두 요구한다."""
    args = cli._parse_args(
        [
            "--rental-archive-prefix",
            "models/archive/dt=run/profile",
            "--return-archive-prefix",
            "models/archive/dt=run/profile",
            "--station-profile-key",
            "processed/features/station_profile.parquet",
            "--station-master-key",
            "processed_v2/station_master.parquet",
            "--allow-contract-change",
        ]
    )

    assert args.allow_contract_change is True
