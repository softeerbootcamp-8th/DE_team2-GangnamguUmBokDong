"""storage.py의 S3 I/O를 moto로 검증한다."""

import gzip
import json
from datetime import datetime

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
from storage import (
    clear_bronze,
    delete_retry_marker,
    list_retry_markers,
    read_bronze,
    read_manifest,
    write_bronze_part,
    write_manifest,
    write_quarantine,
    write_retry_marker,
    write_silver,
)
from tests.conftest import KST, TEST_BUCKET

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=KST)


def _s3():
    return boto3.client("s3", region_name="us-east-1")


class TestBronzeRoundTrip:
    def test_write_then_read_in_parts_order(self):
        write_bronze_part("test_source", WINDOW_START, "page-002", b'{"b": 2}')
        write_bronze_part("test_source", WINDOW_START, "page-001", b'{"a": 1}')

        result = read_bronze("test_source", WINDOW_START, ["page-001", "page-002"])

        assert result == [b'{"a": 1}', b'{"b": 2}']

    def test_bytes_round_trip_exactly(self):
        raw = b'{"weird": "\\u00e9", "n": 1}'
        write_bronze_part("test_source", WINDOW_START, "page-001", raw)

        result = read_bronze("test_source", WINDOW_START, ["page-001"])

        assert result == [raw]

    def test_stored_object_is_gzip_compressed(self):
        raw = b"plain bytes, not json necessarily"
        write_bronze_part("test_source", WINDOW_START, "page-001", raw)

        key = "bronze/test_source/dt=2026-08-12/hh=14/1410/part=page-001.json.gz"
        stored = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()

        assert gzip.decompress(stored) == raw

    def test_read_ignores_objects_not_listed_in_parts(self):
        write_bronze_part("test_source", WINDOW_START, "page-001", b"keep")
        write_bronze_part("test_source", WINDOW_START, "page-002", b"ignore-me")

        result = read_bronze("test_source", WINDOW_START, ["page-001"])

        assert result == [b"keep"]


class TestClearBronze:
    def test_removes_all_parts_for_that_window(self):
        write_bronze_part("test_source", WINDOW_START, "page-001", b"a")
        write_bronze_part("test_source", WINDOW_START, "page-002", b"b")
        write_bronze_part("test_source", WINDOW_START, "page-003", b"c")

        clear_bronze("test_source", WINDOW_START)

        listed = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="bronze/test_source/dt=2026-08-12/hh=14/1410/"
        )
        assert listed.get("KeyCount", 0) == 0

    def test_rerun_with_fewer_parts_leaves_no_ghosts(self):
        write_bronze_part("test_source", WINDOW_START, "page-001", b"a")
        write_bronze_part("test_source", WINDOW_START, "page-002", b"b")
        write_bronze_part("test_source", WINDOW_START, "page-003", b"c")

        clear_bronze("test_source", WINDOW_START)
        write_bronze_part("test_source", WINDOW_START, "page-001", b"a2")

        listed = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="bronze/test_source/dt=2026-08-12/hh=14/1410/"
        )
        assert listed["KeyCount"] == 1

    def test_clear_on_empty_prefix_does_not_raise(self):
        clear_bronze("nonexistent_source", WINDOW_START)


class TestWriteSilver:
    def test_writes_parquet_and_returns_key(self):
        table = pa.table({"stationId": ["ST-1", "ST-2"], "rackTotCnt": [10, 20]})

        key = write_silver("test_source", WINDOW_START, table)

        assert key == "silver/test_source/dt=2026-08-12/hh=14/1410.parquet"
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read()
        roundtrip = pq.read_table(pa.BufferReader(body))
        assert roundtrip.to_pydict() == table.to_pydict()

    def test_overwrites_same_key_on_second_call(self):
        table1 = pa.table({"x": [1]})
        table2 = pa.table({"x": [1, 2]})

        key1 = write_silver("test_source", WINDOW_START, table1)
        key2 = write_silver("test_source", WINDOW_START, table2)

        assert key1 == key2
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key1)["Body"].read()
        assert pq.read_table(pa.BufferReader(body)).num_rows == 2


class TestWriteQuarantine:
    def test_empty_rows_writes_nothing_and_returns_none(self):
        result = write_quarantine("test_source", WINDOW_START, [])

        assert result is None
        listed = _s3().list_objects_v2(
            Bucket=TEST_BUCKET, Prefix="quarantine/test_source/"
        )
        assert listed.get("KeyCount", 0) == 0

    def test_nonempty_rows_writes_jsonl(self):
        rows = [{"stationId": "ST-1", "_issues": ["missing:rackTotCnt"]}, {"stationId": "ST-2"}]

        key = write_quarantine("test_source", WINDOW_START, rows)

        assert key == "quarantine/test_source/dt=2026-08-12/hh=14/1410.jsonl"
        body = _s3().get_object(Bucket=TEST_BUCKET, Key=key)["Body"].read().decode()
        lines = [json.loads(line) for line in body.strip().split("\n")]
        assert lines == rows


class TestManifestRawIO:
    def test_write_then_read_round_trip(self):
        data = {"source_id": "test_source", "status": "succeeded"}

        write_manifest("test_source", WINDOW_START, data)
        result = read_manifest("test_source", WINDOW_START)

        assert result == data

    def test_read_missing_returns_none(self):
        assert read_manifest("never_written", WINDOW_START) is None


class TestRetryMarkerRawIO:
    def test_write_then_list(self):
        write_retry_marker(
            "test_source", WINDOW_START, {"source_id": "test_source", "attempts": 1}
        )

        result = list_retry_markers("test_source")

        assert result == [{"source_id": "test_source", "attempts": 1}]

    def test_list_only_returns_matching_source(self):
        write_retry_marker("source_a", WINDOW_START, {"source_id": "source_a"})
        write_retry_marker("source_b", WINDOW_START, {"source_id": "source_b"})

        result = list_retry_markers("source_a")

        assert result == [{"source_id": "source_a"}]

    def test_list_with_no_markers_returns_empty(self):
        assert list_retry_markers("no_markers_here") == []

    def test_delete_removes_marker(self):
        write_retry_marker("test_source", WINDOW_START, {"source_id": "test_source"})

        delete_retry_marker("test_source", WINDOW_START)

        assert list_retry_markers("test_source") == []

    def test_delete_nonexistent_does_not_raise(self):
        delete_retry_marker("never_written", WINDOW_START)
