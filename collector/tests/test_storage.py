"""storage.py의 S3 I/O를 moto로 검증한다."""

import gzip
from datetime import UTC, datetime

import boto3

from storage import clear_bronze, read_bronze, write_bronze_part
from tests.conftest import TEST_BUCKET

WINDOW_START = datetime(2026, 8, 12, 14, 10, tzinfo=UTC)


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
