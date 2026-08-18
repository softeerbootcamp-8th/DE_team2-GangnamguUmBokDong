"""storage.py의 S3 I/O를 moto로 검증한다."""

import io
from datetime import date, datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
import storage

from tests.conftest import KST, TEST_BUCKET


def _s3():
    return boto3.client("s3", region_name="us-east-1")


def _put_parquet(key: str, table: pa.Table) -> None:
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    _s3().put_object(Bucket=TEST_BUCKET, Key=key, Body=buffer.getvalue())


class TestPartitionDiscovery:
    def test_list_partition_dates_empty_when_nothing_written(self):
        assert storage.list_partition_dates("living_population_grid") == []

    def test_list_partition_dates_returns_sorted_dates(self):
        table = pa.table({"a": [1]})
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/0000.parquet", table)
        _put_parquet("silver/living_population_grid/dt=2026-08-11/hh=00/0000.parquet", table)

        result = storage.list_partition_dates("living_population_grid")

        assert result == [date(2026, 8, 11), date(2026, 8, 12)]

    def test_partition_exists_true_and_false(self):
        table = pa.table({"a": [1]})
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/0000.parquet", table)

        assert storage.partition_exists("living_population_grid", date(2026, 8, 12)) is True
        assert storage.partition_exists("living_population_grid", date(2026, 8, 13)) is False

    def test_find_latest_partition_date_returns_max(self):
        table = pa.table({"a": [1]})
        _put_parquet("silver/living_population_grid/dt=2026-08-10/hh=00/0000.parquet", table)
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/0000.parquet", table)

        assert storage.find_latest_partition_date("living_population_grid") == date(2026, 8, 12)

    def test_find_latest_partition_date_raises_when_none_exist(self):
        with pytest.raises(storage.PartitionNotFoundError):
            storage.find_latest_partition_date("living_population_grid")


class TestReadGridSilver:
    def test_reads_and_concats_all_parquet_under_date_prefix(self):
        table_a = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        table_b = pa.table({"CELL_ID": ["가가00000001"], "SPOP": [20.0]})
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/0000.parquet", table_a)
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=01/0100.parquet", table_b)

        result = storage.read_grid_silver(date(2026, 8, 12))

        assert sorted(result.column("CELL_ID").to_pylist()) == ["가가00000000", "가가00000001"]

    def test_excludes_nowcast_with_different_schema(self):
        measured = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [10.0]})
        nowcast = pa.table({"grid_id": ["가가99999999"], "estimated_population": [999]})
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=14/1400.parquet", measured)
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/nowcast.parquet", nowcast)

        result = storage.read_grid_silver(date(2026, 8, 12))

        assert result.schema == measured.schema
        assert result.to_pylist() == measured.to_pylist()

    def test_raises_when_date_prefix_contains_only_nowcast(self):
        nowcast = pa.table({"grid_id": ["가가99999999"], "estimated_population": [999]})
        _put_parquet("silver/living_population_grid/dt=2026-08-12/hh=00/nowcast.parquet", nowcast)

        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_grid_silver(date(2026, 8, 12))

    def test_raises_when_date_prefix_has_no_parquet(self):
        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_grid_silver(date(2026, 8, 12))


class TestReadRealtimeSilver:
    def test_reads_exact_window_key(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        table = pa.table({"AREA_CD": ["POI001"]})
        _put_parquet("silver/population_realtime/dt=2026-08-12/hh=14/1405.parquet", table)

        result = storage.read_realtime_silver(window_start)

        assert result.column("AREA_CD").to_pylist() == ["POI001"]

    def test_raises_when_window_file_missing(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        with pytest.raises(storage.PartitionNotFoundError):
            storage.read_realtime_silver(window_start)


class TestWriteNormalizedSilverAndManifest:
    def test_write_normalized_silver_key_and_roundtrip(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)
        table = pa.table({"CELL_ID": ["가가00000000"], "SPOP": [42]})

        key = storage.write_normalized_silver(window_start, table)

        assert key == "silver/living_population_normalized/dt=2026-08-12/hh=14/1405.parquet"
        stored = pq.read_table(io.BytesIO(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()))
        assert stored.column("SPOP").to_pylist() == [42]

    def test_write_manifest_key_and_content(self):
        window_start = datetime(2026, 8, 12, 14, 5, tzinfo=KST)

        key = storage.write_manifest(window_start, {"baseline_date": "2026-08-12", "baseline_date_mode": "strict"})

        assert key == "_manifest/living_population_normalized/dt=2026-08-12/hh=14/1405.json"
        import json
        body = json.loads(_s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read())
        assert body == {"baseline_date": "2026-08-12", "baseline_date_mode": "strict"}
