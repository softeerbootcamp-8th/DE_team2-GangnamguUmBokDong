"""storage.py의 S3 I/O를 moto로 검증한다."""

import io
from datetime import date

import boto3
import pyarrow as pa
import pyarrow.parquet as pq

import storage
from tests.conftest import TEST_BUCKET


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


class TestReadRealGridSilver:
    def test_reads_and_concats_real_files_under_date_prefix(self):
        table_a = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        table_b = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [20.0]})
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=00/0000.parquet", table_a)
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=01/0100.parquet", table_b)

        result = storage.read_real_grid_silver(date(2026, 8, 11))

        assert sorted(result.column("CELL_ID").to_pylist()) == ["가가00000000", "가가00000001"]

    def test_excludes_nowcast_file_from_same_prefix(self):
        real = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        estimated = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [99.0]})
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=00/0000.parquet", real)
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=00/nowcast.parquet", estimated)

        result = storage.read_real_grid_silver(date(2026, 8, 11))

        assert result.column("CELL_ID").to_pylist() == ["가가00000000"]

    def test_returns_none_when_only_nowcast_file_present(self):
        estimated = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [99.0]})
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=00/nowcast.parquet", estimated)

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
