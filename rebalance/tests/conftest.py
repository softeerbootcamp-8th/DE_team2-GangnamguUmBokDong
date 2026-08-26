"""테스트 공용 픽스처. moto S3 목킹은 loader/tests/conftest.py 패턴을 그대로 따른다."""

import io
from datetime import datetime
from zoneinfo import ZoneInfo

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from core import s3 as s3_io
from core.gold_publication.canonical import sha256_hex
from core.source_snapshot import (
    SourceSnapshotCounts,
    SourceSnapshotStatus,
    build_source_snapshot_manifest,
)
from moto import mock_aws

TEST_BUCKET = "test-bucket"


def put_source_snapshot(source_id: str, logical: datetime, table: pa.Table) -> None:
    """Collector와 같은 immutable Silver 및 authority manifest를 moto에 쓴다."""
    if type(logical) is not datetime:
        logical = logical.to_pydatetime()
    if logical.tzinfo is None:
        logical = logical.replace(tzinfo=ZoneInfo("Asia/Seoul"))
    buffer = io.BytesIO()
    pq.write_table(table, buffer)
    body = buffer.getvalue()
    checksum = sha256_hex(body)
    silver_key = (
        f"silver/{source_id}/dt={logical:%Y-%m-%d}/hh={logical:%H}/"
        f"{logical:%H%M}/sha256={checksum}.parquet"
    )
    manifest = build_source_snapshot_manifest(
        source_id=source_id,
        logical_dttm=logical,
        revision_no=0,
        status=SourceSnapshotStatus.SUCCEEDED,
        config_version="fixture-v1",
        silver_uri=f"s3://{TEST_BUCKET}/{silver_key}",
        silver_byte_sha256=checksum,
        counts=SourceSnapshotCounts(
            table.num_rows, table.num_rows, table.num_rows, 0, 0
        ),
        planned_parts=("page=1",),
        completed_parts=("page=1",),
    )
    utc = logical.astimezone(ZoneInfo("UTC"))
    manifest_key = (
        f"source_snapshot_manifest/{source_id}/dt={utc:%Y-%m-%d}/hh={utc:%H}/"
        f"logical={utc:%Y%m%dT%H%M%S}{utc.microsecond:06d}Z/"
        "revision=0000000000.json"
    )
    client = boto3.client("s3", region_name="us-east-1")
    client.put_object(Bucket=TEST_BUCKET, Key=silver_key, Body=body)
    client.put_object(
        Bucket=TEST_BUCKET, Key=manifest_key, Body=manifest.canonical_bytes
    )


@pytest.fixture(autouse=True)
def _s3_env(monkeypatch):
    """환경과 process-local S3 client cache를 테스트마다 격리한다."""
    s3_io._clear_client_cache()
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.delenv("S3_ENDPOINT_URL", raising=False)
    monkeypatch.setenv("S3_BUCKET", TEST_BUCKET)
    yield
    s3_io._clear_client_cache()


@pytest.fixture(autouse=True)
def _bucket():
    with mock_aws():
        boto3.client("s3", region_name="us-east-1").create_bucket(Bucket=TEST_BUCKET)
        yield
