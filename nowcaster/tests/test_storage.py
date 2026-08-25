"""storage.py의 S3 I/O를 moto로 검증한다."""

import io
from datetime import date, datetime
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import storage
from core.gold_publication.canonical import sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from tests.conftest import TEST_BUCKET

KST = ZoneInfo("Asia/Seoul")


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


def _put_authoritative_grid(day: date, table: pa.Table) -> str:
    logical = datetime.combine(day, datetime.min.time(), tzinfo=KST).replace(hour=3)
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        f"silver/living_population_grid/dt={day:%Y-%m-%d}/hh=03/"
        f"0300/sha256={checksum}.parquet"
    )
    manifest = build_source_snapshot_manifest(
        source_id="living_population_grid",
        logical_dttm=logical,
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="fixture-v1",
        silver_uri=f"s3://{TEST_BUCKET}/{silver_key}",
        silver_byte_sha256=checksum,
        counts=SourceSnapshotCounts(
            expected=table.num_rows,
            fetched=table.num_rows,
            kept=table.num_rows,
            repaired=0,
            dropped=0,
        ),
        planned_parts=("page=1",),
        completed_parts=("page=1",),
    )
    utc = logical.astimezone(ZoneInfo("UTC"))
    manifest_key = (
        "source_snapshot_manifest/living_population_grid/"
        f"dt={utc:%Y-%m-%d}/hh={utc:%H}/"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
        "revision=0000000000.json"
    )
    _s3().put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    _s3().put_object(
        Bucket=TEST_BUCKET,
        Key=manifest_key,
        Body=manifest.canonical_bytes,
    )
    return silver_key


class TestReadRealGridSilver:
    def test_reads_latest_authoritative_file_for_collection_date(self):
        table = pa.table(
            {"CELL_ID": ["가가00000000", "가가00000001"], "SPOP": [10.0, 20.0]}
        )
        _put_authoritative_grid(date(2026, 8, 11), table)

        result = storage.read_real_grid_silver(date(2026, 8, 11))

        assert sorted(result.column("CELL_ID").to_pylist()) == ["가가00000000", "가가00000001"]

    def test_ignores_nowcast_file_because_only_authority_is_selected(self):
        real = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        estimated = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [99.0]})
        _put_authoritative_grid(date(2026, 8, 11), real)
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-11/hh=00/nowcast.parquet",
            estimated,
        )

        result = storage.read_real_grid_silver(date(2026, 8, 11))

        assert result.column("CELL_ID").to_pylist() == ["가가00000000"]

    def test_rejects_unpublished_partial_silver(self):
        partial = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [99.0]})
        _put_parquet(
            "silver/living_population_grid/dt=2026-08-11/hh=03/"
            f"0300/sha256={'a' * 64}.parquet",
            partial,
        )

        assert storage.read_real_grid_silver(date(2026, 8, 11)) is None

    def test_returns_none_when_prefix_missing(self):
        assert storage.read_real_grid_silver(date(2026, 8, 11)) is None


class TestListArchiveDates:
    def test_returns_sorted_dates_of_existing_archive_files(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_archive(date(2026, 8, 12), table)
        storage.write_archive(date(2026, 8, 5), table)

        assert storage.list_archive_dates() == [date(2026, 8, 5), date(2026, 8, 12)]

    def test_returns_empty_list_when_nothing_archived(self):
        assert storage.list_archive_dates() == []


class TestArchive:
    def test_write_then_read_archive_roundtrip(self):
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})

        key = storage.write_archive(date(2026, 8, 11), table)

        assert key == "archive/living_population_grid/dt=2026-08-11.parquet"
        result = storage.read_archive(date(2026, 8, 11))
        assert result.column("CELL_ID").to_pylist() == ["가가00000000"]

    def test_read_archive_returns_none_when_missing(self):
        assert storage.read_archive(date(2026, 8, 11)) is None


class TestNowcastFile:
    def test_write_nowcast_key_and_roundtrip(self):
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42.0], "is_estimated": [True]})

        key = storage.write_nowcast(date(2026, 8, 13), table)

        assert key == "silver/living_population_grid/dt=2026-08-13/hh=00/nowcast.parquet"
        stored = pq.read_table(io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()))
        assert stored.column("SPOP").to_pylist() == [42.0]

    def test_nowcast_exists_true_and_false(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_nowcast(date(2026, 8, 13), table)

        assert storage.nowcast_exists(date(2026, 8, 13)) is True
        assert storage.nowcast_exists(date(2026, 8, 14)) is False

    def test_delete_nowcast_removes_file(self):
        table = pa.table({"CELL_ID": ["가가00000000"]})
        storage.write_nowcast(date(2026, 8, 13), table)

        storage.delete_nowcast(date(2026, 8, 13))

        assert storage.nowcast_exists(date(2026, 8, 13)) is False

    def test_delete_nowcast_is_idempotent_when_missing(self):
        storage.delete_nowcast(date(2026, 8, 13))  # 에러 없이 통과해야 함
